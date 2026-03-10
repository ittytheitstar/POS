#Requires -Version 5.1
<#
.SYNOPSIS
    Loads and validates all JSON configuration files for the engine.
.DESCRIPTION
    Reads connectors/*.json, entities/*.json, syncRules/*.json and
    runProfiles/*.json from a ConfigPath, validates required fields,
    and returns a typed EngineConfig object the rest of the engine uses.
#>

Set-StrictMode -Version Latest

function Import-EngineConfig {
    <#
    .SYNOPSIS
        Reads all config files from $ConfigPath and returns a validated
        EngineConfig hashtable.
    .PARAMETER ConfigPath
        Root directory that contains connectors/, entities/, syncRules/,
        runProfiles/ sub-folders.
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)]
        [string]$ConfigPath
    )

    if (-not (Test-Path $ConfigPath -PathType Container)) {
        throw "ConfigPath '$ConfigPath' does not exist or is not a directory."
    }

    $config = @{
        Connectors   = @{}
        Entities     = @{}
        SyncRules    = @{}
        RunProfiles  = @{}
    }

    # ── Connectors ──────────────────────────────────────────────────────────
    $connectorDir = Join-Path $ConfigPath 'connectors'
    foreach ($file in Get-JsonFiles $connectorDir) {
        $obj = Read-JsonFile $file
        Assert-RequiredFields $obj @('ConnectorName','ConnectorType','PluginPath') $file
        $config.Connectors[$obj.ConnectorName] = $obj
    }

    # ── Entities ────────────────────────────────────────────────────────────
    $entityDir = Join-Path $ConfigPath 'entities'
    foreach ($file in Get-JsonFiles $entityDir) {
        $obj = Read-JsonFile $file
        Assert-RequiredFields $obj @('EntityName','ObjectType','ConnectorRef') $file
        $config.Entities[$obj.EntityName] = $obj
    }

    # ── Sync Rules ──────────────────────────────────────────────────────────
    $syncRuleDir = Join-Path $ConfigPath 'syncRules'
    foreach ($file in Get-JsonFiles $syncRuleDir) {
        $obj = Read-JsonFile $file
        Assert-RequiredFields $obj @('RuleName','InputEntityRef','OutputEntityRef') $file
        $config.SyncRules[$obj.RuleName] = $obj
    }

    # ── Run Profiles ────────────────────────────────────────────────────────
    $profileDir = Join-Path $ConfigPath 'runProfiles'
    foreach ($file in Get-JsonFiles $profileDir) {
        $obj = Read-JsonFile $file
        Assert-RequiredFields $obj @('ProfileName','Steps') $file
        $config.RunProfiles[$obj.ProfileName] = $obj
    }

    # ── Cross-reference validation ───────────────────────────────────────────
    Confirm-CrossReferences $config

    return $config
}

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

function Get-JsonFiles {
    param([string]$DirPath)
    if (-not (Test-Path $DirPath -PathType Container)) {
        return @()
    }
    return Get-ChildItem -Path $DirPath -Filter '*.json' -File
}

function Read-JsonFile {
    param([System.IO.FileInfo]$File)
    try {
        $raw = Get-Content -Path $File.FullName -Raw -Encoding UTF8
        return $raw | ConvertFrom-Json
    }
    catch {
        throw "Failed to parse JSON file '$($File.FullName)': $_"
    }
}

function Assert-RequiredFields {
    param(
        $Obj,
        [string[]]$Fields,
        [System.IO.FileInfo]$File
    )
    foreach ($field in $Fields) {
        if ($null -eq $Obj.$field -or ($Obj.$field -is [string] -and $Obj.$field.Trim() -eq '')) {
            throw "Config file '$($File.Name)' is missing required field '$field'."
        }
    }
}

function Confirm-CrossReferences {
    param([hashtable]$Config)

    # Each entity must point to an existing connector.
    foreach ($entity in $Config.Entities.Values) {
        $ref = $entity.ConnectorRef
        if (-not $Config.Connectors.ContainsKey($ref)) {
            throw "Entity '$($entity.EntityName)' references unknown ConnectorRef '$ref'."
        }
    }

    # Each sync rule must reference existing entities.
    foreach ($rule in $Config.SyncRules.Values) {
        foreach ($refProp in @('InputEntityRef','OutputEntityRef')) {
            $ref = $rule.$refProp
            if (-not $Config.Entities.ContainsKey($ref)) {
                throw "SyncRule '$($rule.RuleName)' references unknown $refProp '$ref'."
            }
        }
    }

    # Run profile steps must reference valid actions.
    $validActions = @('ImportFull','ImportDelta','Stage','Join','Transform','Export')
    foreach ($profile in $Config.RunProfiles.Values) {
        foreach ($step in $profile.Steps) {
            if ($step.Action -notin $validActions) {
                throw "RunProfile '$($profile.ProfileName)' has unrecognised step Action '$($step.Action)'. Valid values: $($validActions -join ', ')."
            }
        }
    }
}

function Resolve-PluginPath {
    <#
    .SYNOPSIS
        Resolves a connector PluginPath relative to ConfigPath.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$PluginPath,
        [Parameter(Mandatory)] [string]$ConfigPath
    )

    if ([System.IO.Path]::IsPathRooted($PluginPath)) {
        return $PluginPath
    }
    return Join-Path $ConfigPath $PluginPath
}
