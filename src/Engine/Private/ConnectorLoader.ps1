#Requires -Version 5.1
<#
.SYNOPSIS
    Connector plugin loader and runtime wrapper.
.DESCRIPTION
    Loads a connector plugin script (dot-sources it into an isolated module
    scope) and exposes a normalised interface so the pipeline executor doesn't
    need to know plugin internals.

    Each loaded plugin is represented as a ConnectorRuntime hashtable:
        Config       - original connector JSON config object
        PluginPath   - resolved absolute path
        Module       - the PSModuleInfo or scope object
        FunctionMap  - hashtable of standardised function name -> actual function
#>

Set-StrictMode -Version Latest

# Standardised connector function names that every plugin must implement.
$script:RequiredFunctions = @(
    'Initialize-Connector',
    'Get-EntitySchema',
    'Import-Full',
    'Import-Delta',
    'Get-Object',
    'Create-Object',
    'Update-Object',
    'Delete-Object',
    'Set-Attribute',
    'Test-Exists'
)

function Load-ConnectorPlugin {
    <#
    .SYNOPSIS
        Dot-sources a connector plugin into a dynamic module and returns a
        ConnectorRuntime hashtable.
    .PARAMETER ConnectorConfig
        The parsed connector JSON config object.
    .PARAMETER ConfigPath
        Base config path used to resolve relative PluginPath.
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)] $ConnectorConfig,
        [Parameter(Mandatory)] [string]$ConfigPath
    )

    $pluginPath = $ConnectorConfig.PluginPath
    if (-not [System.IO.Path]::IsPathRooted($pluginPath)) {
        # Resolve relative to config directory.
        $pluginPath = Join-Path $ConfigPath $pluginPath
    }

    if (-not (Test-Path $pluginPath -PathType Leaf)) {
        throw "Connector plugin script not found: '$pluginPath' (ConnectorName='$($ConnectorConfig.ConnectorName)')"
    }

    Write-EngineLog -Level Debug -Component 'ConnectorLoader' -Message "Loading plugin '$pluginPath'"

    # Create a dynamic module so each plugin gets its own scope.
    $module = New-Module -Name "Plugin_$($ConnectorConfig.ConnectorName)" -ScriptBlock {
        param($PluginPath)
        . $PluginPath
    } -ArgumentList $pluginPath | Import-Module -PassThru -Force

    # Verify required functions are exported by the module.
    $missing = $script:RequiredFunctions | Where-Object {
        -not (Get-Command -Module $module.Name -Name $_ -ErrorAction SilentlyContinue)
    }
    if ($missing) {
        throw "Connector plugin '$pluginPath' is missing required functions: $($missing -join ', ')"
    }

    $runtime = @{
        Config     = $ConnectorConfig
        PluginPath = $pluginPath
        Module     = $module
    }

    return $runtime
}

function Invoke-ConnectorFunction {
    <#
    .SYNOPSIS
        Calls a standardised connector function by name on a loaded runtime.
    .PARAMETER Runtime
        ConnectorRuntime hashtable returned by Load-ConnectorPlugin.
    .PARAMETER FunctionName
        One of the standardised function names.
    .PARAMETER Arguments
        Ordered array of positional arguments to pass.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [hashtable]$Runtime,
        [Parameter(Mandatory)] [string]$FunctionName,
        [object[]]$Arguments = @()
    )

    $fn = Get-Command -Module $Runtime.Module.Name -Name $FunctionName -ErrorAction Stop
    return & $fn @Arguments
}
