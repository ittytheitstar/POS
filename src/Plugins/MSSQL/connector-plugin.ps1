#Requires -Version 5.1
<#
.SYNOPSIS
    MSSQL Connector Plugin for the Object Sync Engine.
.DESCRIPTION
    Implements the full connector contract against a Microsoft SQL Server source
    or target. Supports:
    - Full import (paged SELECT)
    - Delta import via watermark timestamp column (or checksum fallback)
    - Create / Update / Delete / Set-Attribute operations
    - Test-Exists using configurable unique key columns
    - Parameterized queries only (no string interpolation of user data)

    Connector JSON connection settings expected:
        Connection.Server             - SQL Server host\instance
        Connection.Database           - database name
        Connection.IntegratedSecurity - true for Windows auth
        Connection.Username / Password - SQL auth (use SecureString env var pattern)

    Entity ConnectorMetadata expected per attribute:
        ColumnName     - SQL column name (defaults to AttributeName)
        IsAnchor       - true if this column is the primary key / anchor
        IsTimestamp    - true if this column is the delta watermark

    Entity-level ConnectorMetadata:
        TableName      - fully qualified table name, e.g. "dbo.Users"
        PageSize       - rows per page (default 1000)
#>

Set-StrictMode -Version Latest

# ─────────────────────────────────────────────────────────────────────────────
# Helper: build a SQL connection from connector config
# ─────────────────────────────────────────────────────────────────────────────

function script:New-SqlConn {
    param($ConnectorConfig)

    $c = $ConnectorConfig.Connection
    $b = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
    $b['Data Source']     = $c.Server
    $b['Initial Catalog'] = $c.Database

    if ($c.IntegratedSecurity -eq $true) {
        $b['Integrated Security'] = $true
    } else {
        # Support reading password from environment variable for security.
        $pwd = if ($c.PasswordEnvVar) { [System.Environment]::GetEnvironmentVariable($c.PasswordEnvVar) } else { $c.Password }
        $b['User ID']  = $c.Username
        $b['Password'] = $pwd
    }

    $conn = New-Object System.Data.SqlClient.SqlConnection($b.ConnectionString)
    $conn.Open()
    return $conn
}

# ─────────────────────────────────────────────────────────────────────────────
# Helper: map a SQL data row to a RenderedObject using entity config
# ─────────────────────────────────────────────────────────────────────────────

function script:ConvertTo-RenderedObjectFromRow {
    param(
        [hashtable]$Row,
        $EntityConfig,
        $ConnectorConfig
    )

    # Find anchor column.
    $anchorAttr = $EntityConfig.Attributes | Where-Object { $_.ConnectorMetadata?.IsAnchor -eq $true } | Select-Object -First 1
    if (-not $anchorAttr) {
        $anchorAttr = $EntityConfig.Attributes | Where-Object { $_.IsKey -eq $true } | Select-Object -First 1
    }
    $anchorCol = $anchorAttr?.ConnectorMetadata?.ColumnName ?? $anchorAttr?.AttributeName ?? 'Id'
    $anchor    = $Row[$anchorCol]?.ToString() ?? ''

    $obj = New-RenderedObject `
        -ObjectType         $EntityConfig.ObjectType `
        -Anchor             $anchor `
        -ObjectFriendlyName ($Row[$EntityConfig.FriendlyNameColumn ?? ''] ?? '') `
        -SourceConnector    $ConnectorConfig.ConnectorName

    # Map each attribute definition.
    foreach ($attrDef in $EntityConfig.Attributes) {
        $colName = $attrDef.ConnectorMetadata?.ColumnName ?? $attrDef.AttributeName
        if ($Row.ContainsKey($colName)) {
            $val = $Row[$colName]
            if ($val -is [DBNull]) { $val = $null }

            # Handle multi-valued: JSON-encoded arrays stored as string.
            if ($attrDef.IsMultiValued -eq $true -and $val -is [string] -and $val.StartsWith('[')) {
                try { $val = $val | ConvertFrom-Json } catch {}
            }

            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName $attrDef.AttributeName -Value $val
        }
    }

    # Set ExternalId if defined.
    $extIdAttr = $EntityConfig.Attributes | Where-Object { $_.AttributeName -eq ($EntityConfig.ExternalIdAttribute ?? 'employeeId') } | Select-Object -First 1
    if ($extIdAttr) {
        $obj.ExternalId = (Get-RenderedObjectSingleValue -RenderedObject $obj -AttributeName $extIdAttr.AttributeName) ?? ''
    }

    # Meta timestamps.
    $createdCol  = $EntityConfig.CreatedColumnName
    $modifiedCol = $EntityConfig.ModifiedColumnName
    if ($createdCol  -and $Row[$createdCol])  { $obj.Meta.Created  = $Row[$createdCol]  }
    if ($modifiedCol -and $Row[$modifiedCol]) { $obj.Meta.Modified = $Row[$modifiedCol] }

    $obj.Meta.Raw = $Row

    return $obj
}

# ─────────────────────────────────────────────────────────────────────────────
# Helper: validate a SQL identifier (table/column name) against a safe pattern.
# Prevents SQL injection when identifiers come from config files.
# ─────────────────────────────────────────────────────────────────────────────

function script:Assert-SafeSqlIdentifier {
    param([string]$Identifier, [string]$Context = '')
    # Allow word chars, dots (schema.table), and square-bracket quoting.
    if ($Identifier -notmatch '^[\w\[\]\.\s]+$') {
        throw "Unsafe SQL identifier '$Identifier' rejected. $Context Only alphanumeric, underscore, dot, and square-bracket characters are allowed."
    }
}

function script:Get-SafeIdentifier {
    <#
    .SYNOPSIS
        Returns the identifier wrapped in square brackets for safe embedding in SQL text.
        E.g. "dbo.Users" → "[dbo].[Users]" ; "EmployeeId" → "[EmployeeId]"
    #>
    param([string]$Identifier)
    script:Assert-SafeSqlIdentifier $Identifier
    # Split on dot for schema-qualified names, bracket each part.
    return ($Identifier -split '\.' | ForEach-Object { "[$_]" }) -join '.'
}



function script:Get-AttributeColumns {
    param($EntityConfig)

    $cols = [System.Collections.Generic.List[hashtable]]::new()
    foreach ($attr in $EntityConfig.Attributes) {
        if ($attr.Calculated) { continue }
        $cols.Add(@{
            AttrName   = $attr.AttributeName
            ColName    = $attr.ConnectorMetadata?.ColumnName ?? $attr.AttributeName
            IsAnchor   = $attr.ConnectorMetadata?.IsAnchor -eq $true
            IsTimestamp = $attr.ConnectorMetadata?.IsTimestamp -eq $true
            MultiValued = $attr.IsMultiValued -eq $true
        })
    }
    return $cols
}

# ─────────────────────────────────────────────────────────────────────────────
# Connector contract implementations
# ─────────────────────────────────────────────────────────────────────────────

function Initialize-Connector {
    <#
    .SYNOPSIS Validates prerequisites and returns a runtime connector object.
    #>
    param($ConnectorConfig, $EngineContext)

    # Test connectivity.
    try {
        $conn = script:New-SqlConn $ConnectorConfig
        $conn.Close(); $conn.Dispose()
    }
    catch {
        throw "MSSQL connector '$($ConnectorConfig.ConnectorName)' failed connectivity test: $_"
    }

    return @{
        ConnectorConfig = $ConnectorConfig
        Type            = 'MSSQL'
        IsInitialized   = $true
    }
}

function Get-EntitySchema {
    <#
    .SYNOPSIS Returns schema metadata for the entity (columns, types).
    #>
    param($Connector, $EntityConfig, $EngineContext)

    $conn = script:New-SqlConn $Connector.ConnectorConfig
    try {
        $table = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = "SELECT TOP 0 * FROM $table"
        $schema = $cmd.ExecuteReader([System.Data.CommandBehavior]::SchemaOnly)
        $schemaTable = $schema.GetSchemaTable()
        $schema.Close()

        $columns = foreach ($row in $schemaTable.Rows) {
            @{
                ColumnName   = $row['ColumnName']
                DataType     = $row['DataType'].Name
                IsNullable   = $row['AllowDBNull']
                IsKey        = $row['IsKey'] -eq $true
                MaxLength    = $row['ColumnSize']
            }
        }
        return @{ Columns = $columns }
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }
}

function Import-Full {
    <#
    .SYNOPSIS Full import: pages through all rows in the source table.
    #>
    param($ConnectorConfig, $EntityConfig, $EngineContext)

    $table    = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
    $pageSize = $EntityConfig.ConnectorMetadata?.PageSize  ?? 1000
    $cols     = script:Get-AttributeColumns $EntityConfig

    # Build SELECT column list (only mapped columns) using safe-bracketed identifiers.
    $colList   = ($cols | ForEach-Object { script:Get-SafeIdentifier $_.ColName }) -join ', '
    $anchorRaw = ($cols | Where-Object { $_.IsAnchor } | Select-Object -First 1)?.ColName ?? ($cols | Select-Object -First 1).ColName
    $anchorCol = script:Get-SafeIdentifier $anchorRaw

    $offset = 0
    $allObjects = [System.Collections.Generic.List[hashtable]]::new()
    $conn = script:New-SqlConn $ConnectorConfig
    try {
        do {
            $sql = "SELECT $colList FROM $table ORDER BY $anchorCol OFFSET $offset ROWS FETCH NEXT $pageSize ROWS ONLY"
            $cmd = $conn.CreateCommand()
            $cmd.CommandText = $sql
            $reader = $cmd.ExecuteReader()
            $rowCount = 0
            while ($reader.Read()) {
                $row = @{}
                for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                    $row[$reader.GetName($i)] = $reader.GetValue($i)
                }
                $allObjects.Add((script:ConvertTo-RenderedObjectFromRow -Row $row -EntityConfig $EntityConfig -ConnectorConfig $ConnectorConfig))
                $rowCount++
            }
            $reader.Close()
            $offset += $pageSize
        } while ($rowCount -eq $pageSize)
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }

    return $allObjects.ToArray()
}

function Import-Delta {
    <#
    .SYNOPSIS
        Delta import: returns objects modified since $SinceTimestamp using a
        watermark column, or falls back to full import if no timestamp column.
    #>
    param($ConnectorConfig, $EntityConfig, $SinceTimestamp, $LastToken, $EngineContext)

    $cols          = script:Get-AttributeColumns $EntityConfig
    $timestampRaw  = ($cols | Where-Object { $_.IsTimestamp } | Select-Object -First 1)?.ColName

    if (-not $timestampRaw) {
        # Fallback: full import (engine checksum strategy handles deltas).
        return Import-Full -ConnectorConfig $ConnectorConfig -EntityConfig $EntityConfig -EngineContext $EngineContext
    }

    $table        = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
    $colList      = ($cols | ForEach-Object { script:Get-SafeIdentifier $_.ColName }) -join ', '
    $timestampCol = script:Get-SafeIdentifier $timestampRaw
    $since        = if ($SinceTimestamp) { $SinceTimestamp } else { [datetime]'1970-01-01' }
    $pageSize     = $EntityConfig.ConnectorMetadata?.PageSize ?? 1000
    $anchorRaw    = ($cols | Where-Object { $_.IsAnchor } | Select-Object -First 1)?.ColName ?? 'Id'
    $anchorCol    = script:Get-SafeIdentifier $anchorRaw

    $allObjects = [System.Collections.Generic.List[hashtable]]::new()
    $offset = 0
    $conn = script:New-SqlConn $ConnectorConfig
    try {
        do {
            $sql = "SELECT $colList FROM $table WHERE $timestampCol > @Since ORDER BY $anchorCol OFFSET $offset ROWS FETCH NEXT $pageSize ROWS ONLY"
            $cmd = $conn.CreateCommand()
            $cmd.CommandText = $sql
            $cmd.Parameters.AddWithValue('@Since', $since) | Out-Null
            $reader = $cmd.ExecuteReader()
            $rowCount = 0
            while ($reader.Read()) {
                $row = @{}
                for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                    $row[$reader.GetName($i)] = $reader.GetValue($i)
                }
                $allObjects.Add((script:ConvertTo-RenderedObjectFromRow -Row $row -EntityConfig $EntityConfig -ConnectorConfig $ConnectorConfig))
                $rowCount++
            }
            $reader.Close()
            $offset += $pageSize
        } while ($rowCount -eq $pageSize)
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }

    return $allObjects.ToArray()
}

function Get-Object {
    <#
    .SYNOPSIS Fetches a single object by anchor.
    #>
    param($ConnectorConfig, $EntityConfig, $Anchor, $EngineContext)

    $cols      = script:Get-AttributeColumns $EntityConfig
    $table     = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
    $colList   = ($cols | ForEach-Object { script:Get-SafeIdentifier $_.ColName }) -join ', '
    $anchorCol = script:Get-SafeIdentifier (($cols | Where-Object { $_.IsAnchor } | Select-Object -First 1)?.ColName ?? 'Id')

    $conn = script:New-SqlConn $ConnectorConfig
    try {
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = "SELECT $colList FROM $table WHERE $anchorCol = @Anchor"
        $cmd.Parameters.AddWithValue('@Anchor', $Anchor) | Out-Null
        $reader = $cmd.ExecuteReader()
        if ($reader.Read()) {
            $row = @{}
            for ($i = 0; $i -lt $reader.FieldCount; $i++) { $row[$reader.GetName($i)] = $reader.GetValue($i) }
            $reader.Close()
            return script:ConvertTo-RenderedObjectFromRow -Row $row -EntityConfig $EntityConfig -ConnectorConfig $ConnectorConfig
        }
        $reader.Close()
        return $null
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }
}

function Create-Object {
    <#
    .SYNOPSIS Inserts a new row for the RenderedObject.
    #>
    param($ConnectorConfig, $EntityConfig, $RenderedObject, $EngineContext)

    $cols      = script:Get-AttributeColumns $EntityConfig
    $table     = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
    $writeCols = $cols | Where-Object { -not $_.IsAnchor -or $EntityConfig.ConnectorMetadata?.AnchorIsClientGenerated -eq $true }

    $colNames  = ($writeCols | ForEach-Object { script:Get-SafeIdentifier $_.ColName }) -join ', '
    $paramList = ($writeCols | ForEach-Object { "@$($_.AttrName)" }) -join ', '

    $conn = script:New-SqlConn $ConnectorConfig
    try {
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = "INSERT INTO $table ($colNames) VALUES ($paramList)"
        foreach ($col in $writeCols) {
            $val = Get-RenderedObjectSingleValue -RenderedObject $RenderedObject -AttributeName $col.AttrName
            $cmd.Parameters.AddWithValue("@$($col.AttrName)", ($null -eq $val ? [DBNull]::Value : $val)) | Out-Null
        }
        $cmd.ExecuteNonQuery() | Out-Null
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }
}

function Update-Object {
    <#
    .SYNOPSIS Updates only the patched attributes for an existing row.
    #>
    param($ConnectorConfig, $EntityConfig, $Anchor, $Patch, $EngineContext)

    if (-not $Patch -or $Patch.Count -eq 0) { return }

    $cols      = script:Get-AttributeColumns $EntityConfig
    $table     = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
    $anchorCol = script:Get-SafeIdentifier (($cols | Where-Object { $_.IsAnchor } | Select-Object -First 1)?.ColName ?? 'Id')

    # Build SET clause only for patched attributes.
    $setClauses = [System.Collections.Generic.List[string]]::new()
    $conn = script:New-SqlConn $ConnectorConfig
    try {
        $cmd = $conn.CreateCommand()
        foreach ($attrName in $Patch.Keys) {
            $attrDef = $cols | Where-Object { $_.AttrName -eq $attrName } | Select-Object -First 1
            if (-not $attrDef) { continue }
            $safeColName = script:Get-SafeIdentifier $attrDef.ColName
            $setClauses.Add("$safeColName = @p_$attrName")
            $val = $Patch[$attrName]
            if ($val -is [array] -and $val.Count -eq 1) { $val = $val[0] }
            $cmd.Parameters.AddWithValue("@p_$attrName", ($null -eq $val ? [DBNull]::Value : $val)) | Out-Null
        }
        if ($setClauses.Count -eq 0) { return }
        $cmd.CommandText = "UPDATE $table SET $($setClauses -join ', ') WHERE $anchorCol = @Anchor"
        $cmd.Parameters.AddWithValue('@Anchor', $Anchor) | Out-Null
        $cmd.ExecuteNonQuery() | Out-Null
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }
}

function Delete-Object {
    <#
    .SYNOPSIS
        Deletes (or soft-deletes) a row by anchor.
        If ConnectorConfig.Capabilities.supportsSoftDelete and EntityConfig has
        SoftDeleteColumn, sets that column instead of hard deleting.
    #>
    param($ConnectorConfig, $EntityConfig, $Anchor, $EngineContext)

    $cols         = script:Get-AttributeColumns $EntityConfig
    $table        = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
    $anchorCol    = script:Get-SafeIdentifier (($cols | Where-Object { $_.IsAnchor } | Select-Object -First 1)?.ColName ?? 'Id')
    $softDelRaw   = $EntityConfig.ConnectorMetadata?.SoftDeleteColumn
    $supportsSoft = $ConnectorConfig.Capabilities?.supportsSoftDelete -eq $true

    $conn = script:New-SqlConn $ConnectorConfig
    try {
        $cmd = $conn.CreateCommand()
        if ($supportsSoft -and $softDelRaw) {
            $softDelCol = script:Get-SafeIdentifier $softDelRaw
            $cmd.CommandText = "UPDATE $table SET $softDelCol = 1 WHERE $anchorCol = @Anchor"
        } else {
            $cmd.CommandText = "DELETE FROM $table WHERE $anchorCol = @Anchor"
        }
        $cmd.Parameters.AddWithValue('@Anchor', $Anchor) | Out-Null
        $cmd.ExecuteNonQuery() | Out-Null
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }
}

function Set-Attribute {
    <#
    .SYNOPSIS Updates a single attribute value for an existing row.
    #>
    param($ConnectorConfig, $EntityConfig, $Anchor, $AttributeName, $Value, $EngineContext)

    $cols      = script:Get-AttributeColumns $EntityConfig
    $table     = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
    $anchorCol = script:Get-SafeIdentifier (($cols | Where-Object { $_.IsAnchor } | Select-Object -First 1)?.ColName ?? 'Id')
    $attrDef   = $cols | Where-Object { $_.AttrName -eq $AttributeName } | Select-Object -First 1
    if (-not $attrDef) { throw "Attribute '$AttributeName' not found in entity '$($EntityConfig.EntityName)'." }
    $colName   = script:Get-SafeIdentifier $attrDef.ColName

    $conn = script:New-SqlConn $ConnectorConfig
    try {
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = "UPDATE $table SET $colName = @Val WHERE $anchorCol = @Anchor"
        $cmd.Parameters.AddWithValue('@Val',    ($null -eq $Value  ? [DBNull]::Value : $Value)) | Out-Null
        $cmd.Parameters.AddWithValue('@Anchor', $Anchor) | Out-Null
        $cmd.ExecuteNonQuery() | Out-Null
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }
}

function Test-Exists {
    <#
    .SYNOPSIS
        Checks whether an object matching the RenderedObject exists in SQL.
        Returns @{ Exists=$true|$false; Anchor=<string if found> }.
    #>
    param($ConnectorConfig, $EntityConfig, $RenderedObject, $EngineContext)

    $cols      = script:Get-AttributeColumns $EntityConfig
    $table     = script:Get-SafeIdentifier ($EntityConfig.ConnectorMetadata?.TableName ?? $EntityConfig.EntityName)
    $anchorCol = script:Get-SafeIdentifier (($cols | Where-Object { $_.IsAnchor } | Select-Object -First 1)?.ColName ?? 'Id')

    # Use ExternalId attribute for lookup if available.
    $extIdAttrName = $EntityConfig.ExternalIdAttribute ?? 'employeeId'
    $extIdRaw      = ($cols | Where-Object { $_.AttrName -eq $extIdAttrName } | Select-Object -First 1)?.ColName
    $extIdVal      = Get-RenderedObjectSingleValue -RenderedObject $RenderedObject -AttributeName $extIdAttrName

    $conn = script:New-SqlConn $ConnectorConfig
    try {
        $cmd = $conn.CreateCommand()
        if ($extIdRaw -and $extIdVal) {
            $extIdCol = script:Get-SafeIdentifier $extIdRaw
            $cmd.CommandText = "SELECT $anchorCol FROM $table WHERE $extIdCol = @ExtId"
            $cmd.Parameters.AddWithValue('@ExtId', $extIdVal) | Out-Null
        } else {
            $cmd.CommandText = "SELECT $anchorCol FROM $table WHERE $anchorCol = @Anchor"
            $cmd.Parameters.AddWithValue('@Anchor', $RenderedObject.Anchor) | Out-Null
        }
        $result = $cmd.ExecuteScalar()
        if ($null -ne $result -and $result -isnot [DBNull]) {
            return @{ Exists = $true; Anchor = $result.ToString() }
        }
        return @{ Exists = $false; Anchor = $null }
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }
}
