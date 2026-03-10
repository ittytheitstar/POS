#Requires -Version 5.1
<#
.SYNOPSIS
    Main public entrypoint for the Object Sync Engine.
.DESCRIPTION
    Import-Module .\src\Engine\ObjectSyncEngine.psd1
    Invoke-ObjectSync -Profile "Full" -ConfigPath .\config [-WhatIf] [-Verbose]
#>

function Invoke-ObjectSync {
    <#
    .SYNOPSIS
        Kicks off a named run profile, loading config from the given path.
    .DESCRIPTION
        1. Loads and validates all JSON configuration files.
        2. Auto-creates Engine schema tables in the engine database.
        3. Loads all referenced connector plugins.
        4. Executes the run profile pipeline.
        5. Emits a structured summary.
    .PARAMETER Profile
        Name of the run profile to execute (must match a file in runProfiles/).
    .PARAMETER ConfigPath
        Path to the directory containing connectors/, entities/, syncRules/,
        runProfiles/ sub-folders.
    .PARAMETER WhatIf
        Plan only – no export operations are executed.
    .PARAMETER LogLevel
        Minimum log level: Trace, Debug, Info, Warning, Error. Default: Info.
    .EXAMPLE
        Invoke-ObjectSync -Profile "Full" -ConfigPath ".\config"
    .EXAMPLE
        Invoke-ObjectSync -Profile "Delta" -ConfigPath ".\config" -WhatIf -LogLevel Debug
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [string]$Profile,

        [Parameter(Mandatory)]
        [string]$ConfigPath,

        [switch]$WhatIf,

        [ValidateSet('Trace','Debug','Info','Warning','Error')]
        [string]$LogLevel = 'Info'
    )

    Set-EngineLogLevel -Level $LogLevel

    Write-EngineLog -Level Info -Component 'Invoke-ObjectSync' `
        -Message "=== Object Sync Engine starting. Profile='$Profile' ConfigPath='$ConfigPath' WhatIf=$($WhatIf.IsPresent) ==="

    # ── 1. Load configuration ────────────────────────────────────────────────
    Write-EngineLog -Level Info -Component 'Invoke-ObjectSync' -Message 'Loading configuration…'
    $config = Import-EngineConfig -ConfigPath $ConfigPath
    Write-EngineLog -Level Info -Component 'Invoke-ObjectSync' `
        -Message ("Config loaded: {0} connector(s), {1} entit(ies), {2} sync rule(s), {3} run profile(s)." -f `
            $config.Connectors.Count, $config.Entities.Count, $config.SyncRules.Count, $config.RunProfiles.Count)

    # ── 2. Connect to engine database ────────────────────────────────────────
    # The engine database connection info is read from a connector named "EngineDb"
    # (special convention) or falls back to the first MSSQL connector.
    $engineDbConfig = $config.Connectors['EngineDb'] ?? ($config.Connectors.Values | Where-Object { $_.ConnectorType -eq 'MSSQL' } | Select-Object -First 1)
    if (-not $engineDbConfig) {
        throw "No engine database connector found. Add a connector named 'EngineDb' with ConnectorType='MSSQL'."
    }

    Write-EngineLog -Level Info -Component 'Invoke-ObjectSync' `
        -Message "Connecting to engine database via connector '$($engineDbConfig.ConnectorName)'…"

    $dbConn = New-EngineDbConnection -ConnectionInfo $engineDbConfig.Connection
    try {
        # ── 3. Auto-create engine schema ─────────────────────────────────────
        Initialize-EngineDatabase -Connection $dbConn

        # ── 4. Load connector plugins ─────────────────────────────────────────
        Write-EngineLog -Level Info -Component 'Invoke-ObjectSync' -Message 'Loading connector plugins…'
        $runtimes = @{}
        foreach ($connName in $config.Connectors.Keys) {
            $connConfig = $config.Connectors[$connName]
            Write-EngineLog -Level Debug -Component 'Invoke-ObjectSync' `
                -Message "Loading plugin for connector '$connName'…"
            try {
                $runtimes[$connName] = Load-ConnectorPlugin `
                    -ConnectorConfig $connConfig `
                    -ConfigPath      $ConfigPath
            }
            catch {
                Write-EngineLog -Level Warning -Component 'Invoke-ObjectSync' `
                    -Message "Could not load plugin for connector '$connName': $_. Skipping."
            }
        }

        # ── 5. Execute run profile ────────────────────────────────────────────
        $result = Invoke-RunProfile `
            -ProfileName $Profile `
            -Config      $config `
            -Connection  $dbConn `
            -Runtimes    $runtimes `
            -WhatIf      $WhatIf.IsPresent

        Write-EngineLog -Level Info -Component 'Invoke-ObjectSync' `
            -Message "=== Run complete. RunId=$($result.RunId) Status=$($result.Status) ===" `
            -Data $result.Summary

        return $result
    }
    finally {
        if ($dbConn -and $dbConn.State -eq 'Open') {
            $dbConn.Close()
            $dbConn.Dispose()
        }
    }
}
