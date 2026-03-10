#Requires -Version 5.1
<#
.SYNOPSIS
    Export engine – executes planned Create/Update/Delete operations against
    output connectors with retry, backoff, and dead-letter support.
#>

Set-StrictMode -Version Latest

function Invoke-ExportStep {
    <#
    .SYNOPSIS
        Executes a list of ChangeOperation hashtables against the output connector.
    .PARAMETER Operations
        Array of ChangeOperation hashtables:
          Operation   - 'Create' | 'Update' | 'Delete' | 'SetAttribute'
          OutRuntime  - loaded connector runtime
          EntityConfig - output entity config object
          Object      - desired RenderedObject (for Create/Update)
          Anchor      - existing anchor (for Update/Delete)
          Patch       - attribute patch hashtable (for Update)
    .PARAMETER Connection
        Open engine DB connection (for dead-letter / object-state updates).
    .PARAMETER Context
        Engine context hashtable.
    .PARAMETER WhatIf
        If $true, operations are planned but not executed.
    .OUTPUTS
        ExportSummary hashtable: Created, Updated, Deleted, Errors
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)] [hashtable[]]$Operations,
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [hashtable]$Context,
        [bool]$WhatIf = $false
    )

    $summary = @{ Created = 0; Updated = 0; Deleted = 0; Errors = 0 }

    foreach ($op in $Operations) {
        if ($WhatIf) {
            Write-EngineLog -Level Info -Component 'ExportEngine' `
                -Message "[WhatIf] Would execute: $($op.Operation) on anchor '$($op.Anchor)'" `
                -RunId $Context.RunId
            continue
        }

        $maxRetry    = 3
        $retryDelay  = 2
        $attempt     = 0
        $succeeded   = $false

        while ($attempt -lt $maxRetry -and -not $succeeded) {
            $attempt++
            try {
                switch ($op.Operation) {
                    'Create' {
                        Invoke-ConnectorFunction -Runtime $op.OutRuntime `
                            -FunctionName 'Create-Object' `
                            -Arguments @($op.OutRuntime.Config, $op.EntityConfig, $op.Object, $Context)
                        $summary.Created++
                        $succeeded = $true

                        Add-AuditEntry -Connection $Connection -RunId $Context.RunId `
                            -Operation 'Create' -ObjectType $op.Object.ObjectType `
                            -Details @{ Anchor = $op.Object.Anchor; EntityName = $op.EntityConfig.EntityName }
                    }
                    'Update' {
                        Invoke-ConnectorFunction -Runtime $op.OutRuntime `
                            -FunctionName 'Update-Object' `
                            -Arguments @($op.OutRuntime.Config, $op.EntityConfig, $op.Anchor, $op.Patch, $Context)
                        $summary.Updated++
                        $succeeded = $true

                        Add-AuditEntry -Connection $Connection -RunId $Context.RunId `
                            -Operation 'Update' -ObjectType $op.EntityConfig.ObjectType `
                            -Details @{ Anchor = $op.Anchor; EntityName = $op.EntityConfig.EntityName; Patch = ($op.Patch.Keys -join ',') }
                    }
                    'Delete' {
                        Invoke-ConnectorFunction -Runtime $op.OutRuntime `
                            -FunctionName 'Delete-Object' `
                            -Arguments @($op.OutRuntime.Config, $op.EntityConfig, $op.Anchor, $Context)
                        $summary.Deleted++
                        $succeeded = $true

                        Add-AuditEntry -Connection $Connection -RunId $Context.RunId `
                            -Operation 'Delete' -ObjectType $op.EntityConfig.ObjectType `
                            -Details @{ Anchor = $op.Anchor; EntityName = $op.EntityConfig.EntityName }
                    }
                }
            }
            catch {
                $errMsg = $_.ToString()
                Write-EngineLog -Level Warning -Component 'ExportEngine' `
                    -Message "Attempt $attempt/$maxRetry failed for $($op.Operation) on '$($op.Anchor)': $errMsg" `
                    -RunId $Context.RunId

                if ($attempt -lt $maxRetry) {
                    Start-Sleep -Seconds ($retryDelay * $attempt)
                } else {
                    # Dead-letter the operation.
                    $summary.Errors++
                    Add-Deadletter -Connection $Connection `
                        -RunId         $Context.RunId `
                        -ConnectorName $op.OutRuntime.Config.ConnectorName `
                        -EntityName    $op.EntityConfig.EntityName `
                        -Anchor        $op.Anchor `
                        -Operation     $op.Operation `
                        -Payload       $op.Object `
                        -Error         $errMsg

                    # Update object state with error.
                    Set-ObjectState -Connection $Connection `
                        -ConnectorName $op.OutRuntime.Config.ConnectorName `
                        -EntityName    $op.EntityConfig.EntityName `
                        -Anchor        ($op.Anchor ?? $op.Object?.Anchor ?? '') `
                        -LastError     $errMsg
                }
            }
        }
    }

    return $summary
}

function New-ChangeOperation {
    <#
    .SYNOPSIS
        Factory helper that creates a ChangeOperation hashtable.
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [ValidateSet('Create','Update','Delete','SetAttribute')]
        [string]$Operation,
        [hashtable]$OutRuntime,
        $EntityConfig,
        [hashtable]$Object = $null,
        [string]$Anchor    = '',
        [hashtable]$Patch  = $null
    )

    return @{
        Operation    = $Operation
        OutRuntime   = $OutRuntime
        EntityConfig = $EntityConfig
        Object       = $Object
        Anchor       = $Anchor
        Patch        = $Patch
    }
}
