#Requires -Version 5.1
<#
.SYNOPSIS
    Structured JSON-line logger for the Object Sync Engine.
.DESCRIPTION
    Provides Write-EngineLog which emits structured JSON-line entries to the
    console (and optionally to a file). Each entry carries timestamp, level,
    runId, component and message fields so log consumers can parse or forward
    them easily.
#>

Set-StrictMode -Version Latest

# Valid log levels and their numeric weights (higher = more severe).
$script:LogLevelWeight = @{
    Trace   = 0
    Debug   = 1
    Info    = 2
    Warning = 3
    Error   = 4
}

# Current effective minimum level – callers may override via Set-EngineLogLevel.
$script:EffectiveLogLevel = 'Info'

function Set-EngineLogLevel {
    <#
    .SYNOPSIS Sets the minimum log level that will be emitted.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateSet('Trace','Debug','Info','Warning','Error')]
        [string]$Level
    )
    $script:EffectiveLogLevel = $Level
}

function Write-EngineLog {
    <#
    .SYNOPSIS
        Emits a structured JSON-line log entry.
    .PARAMETER Level
        Trace | Debug | Info | Warning | Error
    .PARAMETER Message
        Human-readable message.
    .PARAMETER Component
        Logical sub-system emitting the log (e.g., "PipelineExecutor").
    .PARAMETER RunId
        Current run identifier (GUID string). Optional – use if available.
    .PARAMETER Data
        Hashtable of additional structured fields to include in the entry.
    #>
    [CmdletBinding()]
    param(
        [ValidateSet('Trace','Debug','Info','Warning','Error')]
        [string]$Level = 'Info',

        [Parameter(Mandatory)]
        [string]$Message,

        [string]$Component = '',
        [string]$RunId = '',
        [hashtable]$Data = @{}
    )

    # Filter by effective level.
    if ($script:LogLevelWeight[$Level] -lt $script:LogLevelWeight[$script:EffectiveLogLevel]) {
        return
    }

    $entry = [ordered]@{
        timestamp = (Get-Date -Format 'o')
        level     = $Level
        component = $Component
        runId     = $RunId
        message   = $Message
    }

    # Merge extra data fields into the entry.
    foreach ($key in $Data.Keys) {
        $entry[$key] = $Data[$key]
    }

    $json = $entry | ConvertTo-Json -Compress -Depth 5

    switch ($Level) {
        'Error'   { Write-Error   $json }
        'Warning' { Write-Warning $json }
        default   { Write-Host    $json }
    }
}
