#Requires -Version 5.1
<#
.SYNOPSIS
    Active Directory Connector Plugin for the Object Sync Engine.
.DESCRIPTION
    Implements the full connector contract against Active Directory.
    Supports two operational modes selected automatically:
    1. ActiveDirectory PowerShell module (preferred, Windows Server / RSAT).
    2. ADSI fallback via [ADSI] / [DirectorySearcher] (works anywhere).

    Delta strategy:
    - USNChanged-based (most reliable; stores last USN as token)
    - Falls back to whenChanged timestamp if USN is unavailable.

    Multi-valued attributes (e.g., memberOf, proxyAddresses) are handled as
    arrays in RenderedObject.Attributes.

    Entity ConnectorMetadata (per attribute):
        LdapAttribute   - LDAP attribute name (defaults to AttributeName)
        IsAnchor        - true for objectGUID attribute
        IsTimestamp     - true for whenChanged (used for delta)
        IsMultiValued   - true for attributes like memberOf

    Entity-level ConnectorMetadata:
        SearchBase      - LDAP search base (e.g., "OU=Users,DC=corp,DC=local")
        ObjectClass     - LDAP objectClass filter (default: "user")
        PageSize        - LDAP paging (default: 500)

    Connector config Connection settings:
        Server          - DC hostname or FQDN (optional; uses domain locator if omitted)
        Domain          - domain FQDN (optional)
        CredentialRef   - name of a PSCredential variable in environment (optional)
        UseIntegrated   - true for pass-through auth
#>

Set-StrictMode -Version Latest

# ─────────────────────────────────────────────────────────────────────────────
# Mode detection
# ─────────────────────────────────────────────────────────────────────────────

$script:HasAdModule = $false
try {
    if (Get-Module -ListAvailable -Name 'ActiveDirectory' -ErrorAction SilentlyContinue) {
        Import-Module ActiveDirectory -ErrorAction SilentlyContinue
        $script:HasAdModule = $true
    }
} catch {}

# ─────────────────────────────────────────────────────────────────────────────
# Helper: build DirectorySearcher or use AD cmdlets
# ─────────────────────────────────────────────────────────────────────────────

function script:Get-AdSearcher {
    param($ConnectorConfig, $EntityConfig)

    $c          = $ConnectorConfig.Connection
    $searchBase = $EntityConfig.ConnectorMetadata?.SearchBase
    $server     = $c.Server
    $ldapPath   = if ($server -and $searchBase) {
        "LDAP://$server/$searchBase"
    } elseif ($searchBase) {
        "LDAP://$searchBase"
    } elseif ($server) {
        "LDAP://$server"
    } else {
        'LDAP://'
    }

    $de = [ADSI]$ldapPath
    $ds = New-Object System.DirectoryServices.DirectorySearcher($de)
    $ds.PageSize    = $EntityConfig.ConnectorMetadata?.PageSize ?? 500
    $ds.SearchScope = 'Subtree'

    return $ds
}

function script:ConvertTo-RenderedObjectFromEntry {
    param($Entry, $EntityConfig, $ConnectorConfig)

    # Anchor = objectGUID (base64) or distinguishedName.
    $anchorAttr = $EntityConfig.Attributes | Where-Object { $_.ConnectorMetadata?.IsAnchor -eq $true } | Select-Object -First 1
    $anchorLdap = $anchorAttr?.ConnectorMetadata?.LdapAttribute ?? 'objectGUID'

    $anchorRaw = if ($Entry.Properties[$anchorLdap]?.Count -gt 0) {
        $Entry.Properties[$anchorLdap][0]
    } else {
        $Entry.Properties['distinguishedName']?[0]
    }

    $anchor = if ($anchorRaw -is [byte[]]) {
        [guid]::new($anchorRaw).ToString()
    } else {
        $anchorRaw?.ToString() ?? ''
    }

    $obj = New-RenderedObject `
        -ObjectType         $EntityConfig.ObjectType `
        -Anchor             $anchor `
        -SourceConnector    $ConnectorConfig.ConnectorName

    # Map attributes.
    foreach ($attrDef in $EntityConfig.Attributes) {
        $ldapAttr = $attrDef.ConnectorMetadata?.LdapAttribute ?? $attrDef.AttributeName
        $vals     = $Entry.Properties[$ldapAttr]
        if ($null -eq $vals -or $vals.Count -eq 0) { continue }

        $valArray = @($vals | ForEach-Object {
            if ($_ -is [byte[]]) { [System.Text.Encoding]::UTF8.GetString($_) } else { $_ }
        })

        if ($attrDef.IsMultiValued -eq $true) {
            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName $attrDef.AttributeName -Value $valArray
        } else {
            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName $attrDef.AttributeName -Value $valArray[0]
        }
    }

    # Friendly name.
    $fn = (Get-RenderedObjectSingleValue -RenderedObject $obj -AttributeName 'displayName') ?? `
          (Get-RenderedObjectSingleValue -RenderedObject $obj -AttributeName 'cn') ?? $anchor
    $obj.ObjectFriendlyName = $fn

    # ExternalId.
    $extIdAttr = $EntityConfig.ExternalIdAttribute ?? 'employeeId'
    $obj.ExternalId = (Get-RenderedObjectSingleValue -RenderedObject $obj -AttributeName $extIdAttr) ?? ''

    $obj.Meta.Raw = $Entry

    return $obj
}

function script:Get-LdapFilter {
    param($EntityConfig)
    $objClass = $EntityConfig.ConnectorMetadata?.ObjectClass ?? 'user'
    $extraFilter = $EntityConfig.ConnectorMetadata?.LdapFilter ?? ''
    if ($extraFilter) {
        return "(&(objectClass=$objClass)$extraFilter)"
    }
    return "(objectClass=$objClass)"
}

function script:Get-LdapAttributeList {
    param($EntityConfig)
    return @($EntityConfig.Attributes | Where-Object { -not $_.Calculated } | ForEach-Object {
        $_.ConnectorMetadata?.LdapAttribute ?? $_.AttributeName
    })
}

# ─────────────────────────────────────────────────────────────────────────────
# Connector contract
# ─────────────────────────────────────────────────────────────────────────────

function Initialize-Connector {
    param($ConnectorConfig, $EngineContext)
    # TODO: add credential store / Kerberos check here.
    return @{ ConnectorConfig = $ConnectorConfig; Type = 'ActiveDirectory'; HasAdModule = $script:HasAdModule }
}

function Get-EntitySchema {
    param($Connector, $EntityConfig, $EngineContext)
    # Return attribute definitions from entity config.
    return @{ Attributes = $EntityConfig.Attributes }
}

function Import-Full {
    param($ConnectorConfig, $EntityConfig, $EngineContext)

    $filter  = script:Get-LdapFilter $EntityConfig
    $attrs   = script:Get-LdapAttributeList $EntityConfig
    $results = [System.Collections.Generic.List[hashtable]]::new()

    $ds = script:Get-AdSearcher $ConnectorConfig $EntityConfig
    $ds.Filter = $filter
    $attrs | ForEach-Object { $ds.PropertiesToLoad.Add($_) | Out-Null }
    $ds.PropertiesToLoad.Add('uSNChanged') | Out-Null

    $src = $ds.FindAll()
    foreach ($entry in $src) {
        $results.Add((script:ConvertTo-RenderedObjectFromEntry -Entry $entry -EntityConfig $EntityConfig -ConnectorConfig $ConnectorConfig))
    }
    $src.Dispose()

    return $results.ToArray()
}

function Import-Delta {
    param($ConnectorConfig, $EntityConfig, $SinceTimestamp, $LastToken, $EngineContext)

    $filter  = script:Get-LdapFilter $EntityConfig
    $attrs   = script:Get-LdapAttributeList $EntityConfig
    $results = [System.Collections.Generic.List[hashtable]]::new()

    $ds = script:Get-AdSearcher $ConnectorConfig $EntityConfig
    $ds.PropertiesToLoad.Add('uSNChanged') | Out-Null
    $attrs | ForEach-Object { $ds.PropertiesToLoad.Add($_) | Out-Null }

    # Build delta filter using uSNChanged token or whenChanged timestamp.
    if ($LastToken) {
        $lastUsn = [long]$LastToken
        $ds.Filter = "(&$filter(uSNChanged>=$lastUsn))"
    } elseif ($SinceTimestamp) {
        $ldapDate = $SinceTimestamp.ToUniversalTime().ToString('yyyyMMddHHmmss.0Z')
        $ds.Filter = "(&$filter(whenChanged>=$ldapDate))"
    } else {
        $ds.Filter = $filter
    }

    $src = $ds.FindAll()
    foreach ($entry in $src) {
        $results.Add((script:ConvertTo-RenderedObjectFromEntry -Entry $entry -EntityConfig $EntityConfig -ConnectorConfig $ConnectorConfig))
    }
    $src.Dispose()

    return $results.ToArray()
}

function Get-Object {
    param($ConnectorConfig, $EntityConfig, $Anchor, $EngineContext)

    $ds      = script:Get-AdSearcher $ConnectorConfig $EntityConfig
    $attrs   = script:Get-LdapAttributeList $EntityConfig
    $attrs | ForEach-Object { $ds.PropertiesToLoad.Add($_) | Out-Null }

    # Try to look up by GUID first (anchor), then by DN.
    try {
        $guidBytes = [guid]::Parse($Anchor).ToByteArray()
        $escaped   = ($guidBytes | ForEach-Object { '\' + $_.ToString('X2') }) -join ''
        $ds.Filter = "(objectGUID=$escaped)"
    } catch {
        $ds.Filter = "(distinguishedName=$Anchor)"
    }

    $entry = $ds.FindOne()
    if ($null -eq $entry) { return $null }
    return script:ConvertTo-RenderedObjectFromEntry -Entry $entry -EntityConfig $EntityConfig -ConnectorConfig $ConnectorConfig
}

function Create-Object {
    <#
    .SYNOPSIS Creates a new AD user (or other object class) via ADSI.
    #>
    param($ConnectorConfig, $EntityConfig, $RenderedObject, $EngineContext)

    $searchBase = $EntityConfig.ConnectorMetadata?.SearchBase ?? ''
    $server     = $ConnectorConfig.Connection?.Server
    $ldapPath   = if ($server) { "LDAP://$server/$searchBase" } else { "LDAP://$searchBase" }

    $cn  = Get-RenderedObjectSingleValue -RenderedObject $RenderedObject -AttributeName 'cn'
    $sam = Get-RenderedObjectSingleValue -RenderedObject $RenderedObject -AttributeName 'sAMAccountName'
    $name = $cn ?? $sam ?? $RenderedObject.Anchor

    $container = [ADSI]$ldapPath
    $newObj    = $container.Create($EntityConfig.ConnectorMetadata?.ObjectClass ?? 'user', "CN=$name")

    foreach ($attrDef in $EntityConfig.Attributes) {
        if ($attrDef.Calculated) { continue }
        $ldapAttr = $attrDef.ConnectorMetadata?.LdapAttribute ?? $attrDef.AttributeName
        if ($ldapAttr -in @('objectGUID','objectSid','whenCreated','whenChanged','uSNChanged')) { continue }
        $vals = Get-RenderedObjectAttribute -RenderedObject $RenderedObject -AttributeName $attrDef.AttributeName
        if ($vals.Count -eq 0) { continue }
        if ($attrDef.IsMultiValued -eq $true) {
            foreach ($v in $vals) { $newObj.Properties[$ldapAttr].Add($v) | Out-Null }
        } else {
            $newObj.Properties[$ldapAttr].Value = $vals[0]
        }
    }

    # Enable account (userAccountControl = 512).
    if ($EntityConfig.ConnectorMetadata?.ObjectClass -eq 'user') {
        $newObj.Properties['userAccountControl'].Value = 512
    }

    $newObj.CommitChanges()
}

function Update-Object {
    <#
    .SYNOPSIS Updates attributes of an existing AD object via ADSI.
    #>
    param($ConnectorConfig, $EntityConfig, $Anchor, $Patch, $EngineContext)

    if (-not $Patch -or $Patch.Count -eq 0) { return }

    $entry = _GetAdEntryByAnchor $ConnectorConfig $EntityConfig $Anchor
    if (-not $entry) { throw "AD object with anchor '$Anchor' not found." }

    foreach ($attrName in $Patch.Keys) {
        $attrDef  = $EntityConfig.Attributes | Where-Object { $_.AttributeName -eq $attrName } | Select-Object -First 1
        $ldapAttr = $attrDef?.ConnectorMetadata?.LdapAttribute ?? $attrName
        if ($ldapAttr -in @('objectGUID','objectSid')) { continue }
        $vals = $Patch[$attrName]
        if ($attrDef?.IsMultiValued -eq $true) {
            $entry.Properties[$ldapAttr].Clear()
            foreach ($v in $vals) { $entry.Properties[$ldapAttr].Add($v) | Out-Null }
        } else {
            $entry.Properties[$ldapAttr].Value = if ($vals.Count -gt 0) { $vals[0] } else { $null }
        }
    }

    $entry.CommitChanges()
}

function Delete-Object {
    param($ConnectorConfig, $EntityConfig, $Anchor, $EngineContext)

    $entry = _GetAdEntryByAnchor $ConnectorConfig $EntityConfig $Anchor
    if (-not $entry) { throw "AD object with anchor '$Anchor' not found for deletion." }

    $supportsSoft = $ConnectorConfig.Capabilities?.supportsSoftDelete -eq $true
    if ($supportsSoft) {
        # Disable account (userAccountControl |= 0x0002).
        $uac = [int]($entry.Properties['userAccountControl'].Value)
        $entry.Properties['userAccountControl'].Value = $uac -bor 0x0002
        $entry.CommitChanges()
    } else {
        $parent = $entry.Parent
        $parent.Delete($EntityConfig.ConnectorMetadata?.ObjectClass ?? 'user', $entry.Name)
        $parent.CommitChanges()
    }
}

function Set-Attribute {
    param($ConnectorConfig, $EntityConfig, $Anchor, $AttributeName, $Value, $EngineContext)

    $entry = _GetAdEntryByAnchor $ConnectorConfig $EntityConfig $Anchor
    if (-not $entry) { throw "AD object with anchor '$Anchor' not found." }

    $attrDef  = $EntityConfig.Attributes | Where-Object { $_.AttributeName -eq $AttributeName } | Select-Object -First 1
    $ldapAttr = $attrDef?.ConnectorMetadata?.LdapAttribute ?? $AttributeName

    $entry.Properties[$ldapAttr].Value = $Value
    $entry.CommitChanges()
}

function Test-Exists {
    param($ConnectorConfig, $EntityConfig, $RenderedObject, $EngineContext)

    $ds    = script:Get-AdSearcher $ConnectorConfig $EntityConfig
    $attrs = @('objectGUID', 'distinguishedName')
    $attrs | ForEach-Object { $ds.PropertiesToLoad.Add($_) | Out-Null }

    # Try by UPN or sAMAccountName.
    $upn  = Get-RenderedObjectSingleValue -RenderedObject $RenderedObject -AttributeName 'userPrincipalName'
    $sam  = Get-RenderedObjectSingleValue -RenderedObject $RenderedObject -AttributeName 'sAMAccountName'
    $extId = $RenderedObject.ExternalId

    $filterParts = @()
    if ($upn)   { $filterParts += "(userPrincipalName=$upn)" }
    if ($sam)   { $filterParts += "(sAMAccountName=$sam)" }
    if ($extId) { $filterParts += "(employeeId=$extId)" }

    if ($filterParts.Count -eq 0) { return @{ Exists = $false; Anchor = $null } }

    $ds.Filter = "(&$(script:Get-LdapFilter $EntityConfig)(|$($filterParts -join '')))"
    $entry = $ds.FindOne()

    if ($null -eq $entry) { return @{ Exists = $false; Anchor = $null } }

    $guidBytes = $entry.Properties['objectGUID'][0]
    $guid = [guid]::new($guidBytes).ToString()
    return @{ Exists = $true; Anchor = $guid }
}

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

function _GetAdEntryByAnchor {
    param($ConnectorConfig, $EntityConfig, $Anchor)

    $ds    = script:Get-AdSearcher $ConnectorConfig $EntityConfig
    $attrs = script:Get-LdapAttributeList $EntityConfig
    $attrs | ForEach-Object { $ds.PropertiesToLoad.Add($_) | Out-Null }

    try {
        $guidBytes = [guid]::Parse($Anchor).ToByteArray()
        $escaped   = ($guidBytes | ForEach-Object { '\' + $_.ToString('X2') }) -join ''
        $ds.Filter = "(objectGUID=$escaped)"
    } catch {
        $ds.Filter = "(distinguishedName=$Anchor)"
    }

    $result = $ds.FindOne()
    if ($null -eq $result) { return $null }
    return $result.GetDirectoryEntry()
}
