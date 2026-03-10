@{
    # Module metadata
    RootModule        = 'ObjectSyncEngine.psm1'
    ModuleVersion     = '1.0.0'
    GUID              = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
    Author            = 'POS Project'
    CompanyName       = 'POS Project'
    Description       = 'PowerShell Object Sync Engine – MIM-like generic object synchronisation driven by JSON config and pluggable connector scripts.'
    PowerShellVersion = '5.1'

    # Functions exported from this module
    FunctionsToExport = @(
        'Invoke-ObjectSync'
    )

    CmdletsToExport   = @()
    VariablesToExport = @()
    AliasesToExport   = @()

    PrivateData = @{
        PSData = @{
            Tags         = @('sync','identity','MIM','AD','SQL','automation')
            ProjectUri   = 'https://github.com/ittytheitstar/POS'
            ReleaseNotes = 'Initial release – full pipeline, MSSQL + AD connectors, JSON-driven config.'
        }
    }
}
