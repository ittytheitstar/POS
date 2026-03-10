#Requires -Version 5.1
<#
.SYNOPSIS
    Engine SQL database manager – auto-creates and maintains the Engine schema.
.DESCRIPTION
    Provides:
    - Initialize-EngineDatabase  : creates Engine schema + tables if not present.
    - Invoke-EngineSql            : executes parameterized SQL (scalar, reader, or non-query).
    - Helper functions for each Engine table (upsert run, state, etc.).

    Uses System.Data.SqlClient (Windows PowerShell 5.1) or
    Microsoft.Data.SqlClient (PS 7+, preferred). Falls back gracefully.
#>

Set-StrictMode -Version Latest

# ─────────────────────────────────────────────────────────────────────────────
# Connection factory
# ─────────────────────────────────────────────────────────────────────────────

function New-EngineDbConnection {
    <#
    .SYNOPSIS
        Creates and opens a SQL connection from a connection-string hashtable or
        plain connection string.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] $ConnectionInfo
    )

    $connStr = if ($ConnectionInfo -is [string]) {
        $ConnectionInfo
    } else {
        # Build from parts: Server, Database, IntegratedSecurity / User / Password.
        $b = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
        $b['Data Source']        = $ConnectionInfo.Server
        $b['Initial Catalog']    = $ConnectionInfo.Database
        if ($ConnectionInfo.IntegratedSecurity -eq $true) {
            $b['Integrated Security'] = $true
        } else {
            $b['User ID']   = $ConnectionInfo.Username
            $b['Password']  = $ConnectionInfo.Password
        }
        $b.ConnectionString
    }

    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $conn.Open()
    return $conn
}

# ─────────────────────────────────────────────────────────────────────────────
# Generic SQL executor
# ─────────────────────────────────────────────────────────────────────────────

function Invoke-EngineSql {
    <#
    .SYNOPSIS
        Executes a parameterized SQL command against the engine database.
    .PARAMETER Connection
        Open SqlConnection.
    .PARAMETER Sql
        SQL text (may contain @Param placeholders).
    .PARAMETER Parameters
        Hashtable of parameter name (without @) -> value.
    .PARAMETER Mode
        NonQuery | Scalar | Reader
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$Sql,
        [hashtable]$Parameters = @{},
        [ValidateSet('NonQuery','Scalar','Reader')]
        [string]$Mode = 'NonQuery'
    )

    $cmd = $Connection.CreateCommand()
    $cmd.CommandText = $Sql
    $cmd.CommandTimeout = 120

    foreach ($key in $Parameters.Keys) {
        $paramName = if ($key.StartsWith('@')) { $key } else { "@$key" }
        $p = $cmd.Parameters.AddWithValue($paramName, $(if ($null -eq $Parameters[$key]) { [DBNull]::Value } else { $Parameters[$key] }))
    }

    switch ($Mode) {
        'NonQuery' {
            $cmd.ExecuteNonQuery() | Out-Null
        }
        'Scalar'   {
            $result = $cmd.ExecuteScalar()
            if ($result -is [DBNull]) { return $null }
            return $result
        }
        'Reader'   {
            $reader = $cmd.ExecuteReader()
            $rows   = [System.Collections.Generic.List[hashtable]]::new()
            while ($reader.Read()) {
                $row = @{}
                for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                    $name  = $reader.GetName($i)
                    $value = $reader.GetValue($i)
                    $row[$name] = if ($value -is [DBNull]) { $null } else { $value }
                }
                $rows.Add($row)
            }
            $reader.Close()
            return $rows
        }
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Schema initialisation
# ─────────────────────────────────────────────────────────────────────────────

$script:DDL = @'
-- Engine schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Engine')
    EXEC('CREATE SCHEMA Engine');

-- Run history
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='Engine' AND TABLE_NAME='Run')
CREATE TABLE Engine.Run (
    RunId         UNIQUEIDENTIFIER  NOT NULL DEFAULT NEWID() PRIMARY KEY,
    ProfileName   NVARCHAR(128)     NOT NULL,
    StartTime     DATETIME2         NOT NULL DEFAULT SYSUTCDATETIME(),
    EndTime       DATETIME2         NULL,
    Status        NVARCHAR(32)      NOT NULL DEFAULT 'Running',
    Host          NVARCHAR(256)     NULL,
    SummaryJson   NVARCHAR(MAX)     NULL
);

-- Connector watermark / token state
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='Engine' AND TABLE_NAME='ConnectorState')
CREATE TABLE Engine.ConnectorState (
    ConnectorName            NVARCHAR(128) NOT NULL,
    EntityName               NVARCHAR(128) NOT NULL,
    LastRunTimestamp         DATETIME2     NULL,
    LastToken                NVARCHAR(MAX) NULL,
    LastFullImportTimestamp  DATETIME2     NULL,
    CONSTRAINT PK_ConnectorState PRIMARY KEY (ConnectorName, EntityName)
);

-- Per-object state
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='Engine' AND TABLE_NAME='ObjectState')
CREATE TABLE Engine.ObjectState (
    ConnectorName  NVARCHAR(128)  NOT NULL,
    EntityName     NVARCHAR(128)  NOT NULL,
    Anchor         NVARCHAR(512)  NOT NULL,
    ObjectType     NVARCHAR(128)  NULL,
    ExternalId     NVARCHAR(512)  NULL,
    LastSeen       DATETIME2      NULL,
    LastHash       NVARCHAR(64)   NULL,
    IsDeleted      BIT            NOT NULL DEFAULT 0,
    LastError      NVARCHAR(MAX)  NULL,
    LastErrorTime  DATETIME2      NULL,
    CONSTRAINT PK_ObjectState PRIMARY KEY (ConnectorName, EntityName, Anchor)
);

-- Metaverse (canonical identity)
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='Engine' AND TABLE_NAME='Metaverse')
CREATE TABLE Engine.Metaverse (
    MvId          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    ObjectType    NVARCHAR(128)    NOT NULL,
    CanonicalKey  NVARCHAR(512)    NULL,
    Created       DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
    Modified      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Join links (connector object -> metaverse)
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='Engine' AND TABLE_NAME='JoinLink')
CREATE TABLE Engine.JoinLink (
    MvId           UNIQUEIDENTIFIER NOT NULL,
    ConnectorName  NVARCHAR(128)    NOT NULL,
    EntityName     NVARCHAR(128)    NOT NULL,
    Anchor         NVARCHAR(512)    NOT NULL,
    JoinKey        NVARCHAR(512)    NULL,
    Confidence     DECIMAL(5,4)     NOT NULL DEFAULT 1.0,
    CONSTRAINT PK_JoinLink PRIMARY KEY (ConnectorName, EntityName, Anchor),
    CONSTRAINT FK_JoinLink_Mv FOREIGN KEY (MvId) REFERENCES Engine.Metaverse(MvId)
);

-- Dead-letter queue for failed operations
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='Engine' AND TABLE_NAME='Deadletter')
CREATE TABLE Engine.Deadletter (
    Id             BIGINT           NOT NULL IDENTITY(1,1) PRIMARY KEY,
    RunId          UNIQUEIDENTIFIER NULL,
    ConnectorName  NVARCHAR(128)    NOT NULL,
    EntityName     NVARCHAR(128)    NOT NULL,
    Anchor         NVARCHAR(512)    NULL,
    Operation      NVARCHAR(64)     NULL,
    PayloadJson    NVARCHAR(MAX)    NULL,
    Error          NVARCHAR(MAX)    NULL,
    Created        DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
    RetryCount     INT              NOT NULL DEFAULT 0,
    NextRetry      DATETIME2        NULL
);

-- Audit log
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='Engine' AND TABLE_NAME='Audit')
CREATE TABLE Engine.Audit (
    Id          BIGINT           NOT NULL IDENTITY(1,1) PRIMARY KEY,
    RunId       UNIQUEIDENTIFIER NULL,
    Operation   NVARCHAR(64)     NOT NULL,
    ObjectType  NVARCHAR(128)    NULL,
    DetailsJson NVARCHAR(MAX)    NULL,
    Time        DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
);
'@

function Initialize-EngineDatabase {
    <#
    .SYNOPSIS
        Creates (if absent) all Engine schema tables in the target SQL database.
    .PARAMETER Connection
        Open SqlConnection to the engine database.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection
    )

    Write-EngineLog -Level Info -Component 'DatabaseManager' -Message 'Initialising Engine schema…'

    # Split on GO and execute each batch.
    $batches = $script:DDL -split '(?m)^\s*GO\s*$'
    if ($batches.Count -eq 1) {
        # No GO separators – execute as single batch (works fine for the DDL above).
        Invoke-EngineSql -Connection $Connection -Sql $script:DDL -Mode NonQuery
    } else {
        foreach ($batch in $batches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -gt 0) {
                Invoke-EngineSql -Connection $Connection -Sql $trimmed -Mode NonQuery
            }
        }
    }

    Write-EngineLog -Level Info -Component 'DatabaseManager' -Message 'Engine schema initialisation complete.'
}

# ─────────────────────────────────────────────────────────────────────────────
# Run tracking helpers
# ─────────────────────────────────────────────────────────────────────────────

function New-EngineRun {
    <#
    .SYNOPSIS Creates a new run record and returns its GUID.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$ProfileName
    )

    $runId = [guid]::NewGuid().ToString()
    Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
INSERT INTO Engine.Run (RunId, ProfileName, Host)
VALUES (@RunId, @ProfileName, @Host)
'@ -Parameters @{
        RunId       = $runId
        ProfileName = $ProfileName
        Host        = $env:COMPUTERNAME
    }
    return $runId
}

function Complete-EngineRun {
    <#
    .SYNOPSIS Marks a run record as Succeeded or Failed with a JSON summary.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$RunId,
        [ValidateSet('Succeeded','Failed','Partial')]
        [string]$Status = 'Succeeded',
        [hashtable]$Summary = @{}
    )

    $summaryJson = $Summary | ConvertTo-Json -Compress -Depth 5

    Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
UPDATE Engine.Run
SET    EndTime     = SYSUTCDATETIME(),
       Status      = @Status,
       SummaryJson = @SummaryJson
WHERE  RunId = @RunId
'@ -Parameters @{
        RunId       = $RunId
        Status      = $Status
        SummaryJson = $summaryJson
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Connector state helpers
# ─────────────────────────────────────────────────────────────────────────────

function Get-ConnectorState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$ConnectorName,
        [Parameter(Mandatory)] [string]$EntityName
    )

    $rows = Invoke-EngineSql -Connection $Connection -Mode Reader -Sql @'
SELECT ConnectorName, EntityName, LastRunTimestamp, LastToken, LastFullImportTimestamp
FROM   Engine.ConnectorState
WHERE  ConnectorName = @ConnectorName AND EntityName = @EntityName
'@ -Parameters @{ ConnectorName = $ConnectorName; EntityName = $EntityName }

    if ($rows.Count -gt 0) { return $rows[0] }
    return $null
}

function Set-ConnectorState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$ConnectorName,
        [Parameter(Mandatory)] [string]$EntityName,
        $LastRunTimestamp = $null,
        $LastToken = $null,
        $LastFullImportTimestamp = $null
    )

    Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
MERGE Engine.ConnectorState AS tgt
USING (VALUES(@ConnectorName, @EntityName)) AS src(ConnectorName, EntityName)
ON  tgt.ConnectorName = src.ConnectorName AND tgt.EntityName = src.EntityName
WHEN MATCHED THEN
    UPDATE SET LastRunTimestamp        = ISNULL(@LastRunTimestamp,        tgt.LastRunTimestamp),
               LastToken               = ISNULL(@LastToken,               tgt.LastToken),
               LastFullImportTimestamp = ISNULL(@LastFullImportTimestamp, tgt.LastFullImportTimestamp)
WHEN NOT MATCHED THEN
    INSERT (ConnectorName, EntityName, LastRunTimestamp, LastToken, LastFullImportTimestamp)
    VALUES (@ConnectorName, @EntityName, @LastRunTimestamp, @LastToken, @LastFullImportTimestamp);
'@ -Parameters @{
        ConnectorName           = $ConnectorName
        EntityName              = $EntityName
        LastRunTimestamp        = $LastRunTimestamp
        LastToken               = $LastToken
        LastFullImportTimestamp = $LastFullImportTimestamp
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Object state helpers
# ─────────────────────────────────────────────────────────────────────────────

function Get-ObjectState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$ConnectorName,
        [Parameter(Mandatory)] [string]$EntityName,
        [Parameter(Mandatory)] [string]$Anchor
    )

    $rows = Invoke-EngineSql -Connection $Connection -Mode Reader -Sql @'
SELECT * FROM Engine.ObjectState
WHERE ConnectorName=@ConnectorName AND EntityName=@EntityName AND Anchor=@Anchor
'@ -Parameters @{ ConnectorName = $ConnectorName; EntityName = $EntityName; Anchor = $Anchor }

    if ($rows.Count -gt 0) { return $rows[0] }
    return $null
}

function Set-ObjectState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$ConnectorName,
        [Parameter(Mandatory)] [string]$EntityName,
        [Parameter(Mandatory)] [string]$Anchor,
        [string]$ObjectType = '',
        [string]$ExternalId = '',
        [string]$LastHash   = '',
        [bool]$IsDeleted    = $false,
        [string]$LastError  = $null
    )

    Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
MERGE Engine.ObjectState AS tgt
USING (VALUES(@ConnectorName,@EntityName,@Anchor)) AS src(ConnectorName,EntityName,Anchor)
ON  tgt.ConnectorName=src.ConnectorName AND tgt.EntityName=src.EntityName AND tgt.Anchor=src.Anchor
WHEN MATCHED THEN
    UPDATE SET ObjectType    = @ObjectType,
               ExternalId    = @ExternalId,
               LastSeen      = SYSUTCDATETIME(),
               LastHash      = @LastHash,
               IsDeleted     = @IsDeleted,
               LastError     = @LastError,
               LastErrorTime = CASE WHEN @LastError IS NOT NULL THEN SYSUTCDATETIME() ELSE tgt.LastErrorTime END
WHEN NOT MATCHED THEN
    INSERT (ConnectorName,EntityName,Anchor,ObjectType,ExternalId,LastSeen,LastHash,IsDeleted,LastError)
    VALUES (@ConnectorName,@EntityName,@Anchor,@ObjectType,@ExternalId,SYSUTCDATETIME(),@LastHash,@IsDeleted,@LastError);
'@ -Parameters @{
        ConnectorName = $ConnectorName
        EntityName    = $EntityName
        Anchor        = $Anchor
        ObjectType    = $ObjectType
        ExternalId    = $ExternalId
        LastHash      = $LastHash
        IsDeleted     = $IsDeleted
        LastError     = $LastError
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Metaverse / join link helpers
# ─────────────────────────────────────────────────────────────────────────────

function Get-MetaverseEntry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$ConnectorName,
        [Parameter(Mandatory)] [string]$EntityName,
        [Parameter(Mandatory)] [string]$Anchor
    )

    $rows = Invoke-EngineSql -Connection $Connection -Mode Reader -Sql @'
SELECT mv.MvId, mv.ObjectType, mv.CanonicalKey, jl.JoinKey, jl.Confidence
FROM   Engine.JoinLink jl
JOIN   Engine.Metaverse mv ON mv.MvId = jl.MvId
WHERE  jl.ConnectorName = @ConnectorName
AND    jl.EntityName    = @EntityName
AND    jl.Anchor        = @Anchor
'@ -Parameters @{ ConnectorName = $ConnectorName; EntityName = $EntityName; Anchor = $Anchor }

    if ($rows.Count -gt 0) { return $rows[0] }
    return $null
}

function Set-MetaverseJoin {
    <#
    .SYNOPSIS
        Creates or updates a metaverse entry + join link for the given anchor.
        Returns the MvId.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$ConnectorName,
        [Parameter(Mandatory)] [string]$EntityName,
        [Parameter(Mandatory)] [string]$Anchor,
        [Parameter(Mandatory)] [string]$ObjectType,
        [string]$CanonicalKey = '',
        [string]$JoinKey = '',
        [double]$Confidence = 1.0
    )

    # Check for existing join link.
    $existing = Get-MetaverseEntry -Connection $Connection `
        -ConnectorName $ConnectorName -EntityName $EntityName -Anchor $Anchor

    if ($existing) {
        # Update the canonical key on the metaverse entry.
        Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
UPDATE Engine.Metaverse SET CanonicalKey=@CanonicalKey, Modified=SYSUTCDATETIME()
WHERE MvId=@MvId
'@ -Parameters @{ MvId = $existing.MvId; CanonicalKey = $CanonicalKey }
        return $existing.MvId
    }

    # Check if another connector with the same JoinKey (canonical key) exists.
    $mvId = $null
    if ($JoinKey) {
        $matchRows = Invoke-EngineSql -Connection $Connection -Mode Reader -Sql @'
SELECT mv.MvId FROM Engine.Metaverse mv
WHERE mv.ObjectType=@ObjectType AND mv.CanonicalKey=@CanonicalKey
'@ -Parameters @{ ObjectType = $ObjectType; CanonicalKey = $JoinKey }
        if ($matchRows.Count -gt 0) { $mvId = $matchRows[0].MvId }
    }

    if (-not $mvId) {
        # Create new metaverse entry.
        $mvId = [guid]::NewGuid().ToString()
        Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
INSERT INTO Engine.Metaverse(MvId, ObjectType, CanonicalKey)
VALUES(@MvId, @ObjectType, @CanonicalKey)
'@ -Parameters @{ MvId = $mvId; ObjectType = $ObjectType; CanonicalKey = $CanonicalKey }
    }

    # Create join link.
    Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
INSERT INTO Engine.JoinLink(MvId, ConnectorName, EntityName, Anchor, JoinKey, Confidence)
VALUES(@MvId, @ConnectorName, @EntityName, @Anchor, @JoinKey, @Confidence)
'@ -Parameters @{
        MvId          = $mvId
        ConnectorName = $ConnectorName
        EntityName    = $EntityName
        Anchor        = $Anchor
        JoinKey       = $JoinKey
        Confidence    = $Confidence
    }

    return $mvId
}

function Get-JoinedObjects {
    <#
    .SYNOPSIS
        Given an MvId, returns all join links (connector / entity / anchor).
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [string]$MvId
    )

    return Invoke-EngineSql -Connection $Connection -Mode Reader -Sql @'
SELECT ConnectorName, EntityName, Anchor, JoinKey, Confidence
FROM   Engine.JoinLink
WHERE  MvId = @MvId
'@ -Parameters @{ MvId = $MvId }
}

# ─────────────────────────────────────────────────────────────────────────────
# Dead-letter helpers
# ─────────────────────────────────────────────────────────────────────────────

function Add-Deadletter {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$RunId = $null,
        [Parameter(Mandatory)] [string]$ConnectorName,
        [Parameter(Mandatory)] [string]$EntityName,
        [string]$Anchor    = '',
        [string]$Operation = '',
        $Payload           = $null,
        [string]$Error     = ''
    )

    $payloadJson = if ($null -ne $Payload) { $Payload | ConvertTo-Json -Compress -Depth 5 } else { $null }

    Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
INSERT INTO Engine.Deadletter(RunId, ConnectorName, EntityName, Anchor, Operation, PayloadJson, Error)
VALUES(@RunId, @ConnectorName, @EntityName, @Anchor, @Operation, @PayloadJson, @Error)
'@ -Parameters @{
        RunId         = $RunId
        ConnectorName = $ConnectorName
        EntityName    = $EntityName
        Anchor        = $Anchor
        Operation     = $Operation
        PayloadJson   = $payloadJson
        Error         = $Error
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Audit helpers
# ─────────────────────────────────────────────────────────────────────────────

function Add-AuditEntry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$RunId      = $null,
        [Parameter(Mandatory)] [string]$Operation,
        [string]$ObjectType = '',
        $Details            = $null
    )

    $detailsJson = if ($null -ne $Details) { $Details | ConvertTo-Json -Compress -Depth 5 } else { $null }

    Invoke-EngineSql -Connection $Connection -Mode NonQuery -Sql @'
INSERT INTO Engine.Audit(RunId, Operation, ObjectType, DetailsJson)
VALUES(@RunId, @Operation, @ObjectType, @DetailsJson)
'@ -Parameters @{
        RunId       = $RunId
        Operation   = $Operation
        ObjectType  = $ObjectType
        DetailsJson = $detailsJson
    }
}
