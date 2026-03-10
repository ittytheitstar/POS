#Requires -Version 5.1
<#
.SYNOPSIS
    ObjectSyncEngine module root script.
.DESCRIPTION
    Dot-sources all Private and Public scripts, making all engine functions
    available in the caller's session.
#>

# ── Private modules (order matters: Logger first, then dependencies) ──────────
$private = @(
    'Logger',
    'ConfigLoader',
    'RenderedObject',
    'DatabaseManager',
    'SnippetExecutor',
    'ConnectorLoader',
    'JoinEngine',
    'MappingEngine',
    'ExportEngine',
    'PipelineExecutor'
)

foreach ($name in $private) {
    $path = Join-Path $PSScriptRoot 'Private' "$name.ps1"
    if (Test-Path $path) {
        . $path
    } else {
        Write-Warning "ObjectSyncEngine: private script '$name.ps1' not found at '$path'."
    }
}

# ── Public functions ──────────────────────────────────────────────────────────
$public = Get-ChildItem -Path (Join-Path $PSScriptRoot 'Public') -Filter '*.ps1' -File -ErrorAction SilentlyContinue

foreach ($file in $public) {
    . $file.FullName
}

# Export only the public functions.
Export-ModuleMember -Function (
    $public | ForEach-Object { [System.IO.Path]::GetFileNameWithoutExtension($_.Name) }
)
