#Requires -Version 5.1
<#
.SYNOPSIS
    Main pipeline executor – orchestrates all run profile steps.
.DESCRIPTION
    Reads a run profile from config, iterates its Steps array, and invokes
    the appropriate engine sub-system for each step (Import, Stage, Join,
    Transform, Export). Tracks metrics and updates engine DB state.
#>

Set-StrictMode -Version Latest

function Invoke-RunProfile {
    <#
    .SYNOPSIS
        Executes all steps defined in the named run profile.
    .PARAMETER ProfileName
        Name of the run profile to execute (key in Config.RunProfiles).
    .PARAMETER Config
        Fully loaded EngineConfig hashtable.
    .PARAMETER Connection
        Open engine DB connection.
    .PARAMETER Runtimes
        Hashtable of ConnectorName -> ConnectorRuntime (pre-loaded plugins).
    .PARAMETER WhatIf
        If $true, export steps are skipped (plan only).
    .OUTPUTS
        RunSummary hashtable.
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)] [string]$ProfileName,
        [Parameter(Mandatory)] [hashtable]$Config,
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] [hashtable]$Runtimes,
        [bool]$WhatIf = $false
    )

    $profile = $Config.RunProfiles[$ProfileName]
    if (-not $profile) {
        throw "Run profile '$ProfileName' not found in configuration."
    }

    # Create engine run record.
    $runId = New-EngineRun -Connection $Connection -ProfileName $ProfileName

    Write-EngineLog -Level Info -Component 'PipelineExecutor' `
        -Message "Starting run profile '$ProfileName' (RunId=$runId)" `
        -RunId $runId

    $context = @{
        RunId      = $runId
        Config     = $Config
        WhatIf     = $WhatIf
        Runtimes   = $Runtimes
        Connection = $Connection
    }

    # Per-step accumulators.
    $importedObjects = @{}   # EntityName -> RenderedObject[]
    $totalSummary    = @{ Imports = 0; Exports = @{ Created = 0; Updated = 0; Deleted = 0; Errors = 0 } }
    $runStatus       = 'Succeeded'

    try {
        foreach ($step in $profile.Steps) {
            Write-EngineLog -Level Info -Component 'PipelineExecutor' `
                -Message "Executing step: $($step.Action)" -RunId $runId

            switch ($step.Action) {
                'ImportFull'  { Invoke-ImportStep  -Step $step -Context $context -ImportedObjects ([ref]$importedObjects) -IsDelta $false }
                'ImportDelta' { Invoke-ImportStep  -Step $step -Context $context -ImportedObjects ([ref]$importedObjects) -IsDelta $true  }
                'Stage'       { Invoke-StageStep   -Step $step -Context $context -ImportedObjects $importedObjects }
                'Join'        { Invoke-JoinAllStep -Step $step -Context $context -ImportedObjects $importedObjects }
                'Transform'   { Invoke-TransformStep -Step $step -Context $context -ImportedObjects $importedObjects }
                'Export'      {
                    $expSummary = Invoke-ExportAllStep -Step $step -Context $context `
                        -ImportedObjects $importedObjects -WhatIf $WhatIf
                    foreach ($k in $expSummary.Keys) { $totalSummary.Exports[$k] += $expSummary[$k] }
                }
                default {
                    Write-EngineLog -Level Warning -Component 'PipelineExecutor' `
                        -Message "Unknown step action '$($step.Action)' – skipping." -RunId $runId
                }
            }
        }
    }
    catch {
        $runStatus = 'Failed'
        Write-EngineLog -Level Error -Component 'PipelineExecutor' `
            -Message "Run profile '$ProfileName' failed: $_" -RunId $runId
        throw
    }
    finally {
        Complete-EngineRun -Connection $Connection -RunId $runId -Status $runStatus -Summary $totalSummary
        Write-EngineLog -Level Info -Component 'PipelineExecutor' `
            -Message "Run profile '$ProfileName' complete. Status=$runStatus" -RunId $runId
    }

    return @{ RunId = $runId; Status = $runStatus; Summary = $totalSummary }
}

# ─────────────────────────────────────────────────────────────────────────────
# Step implementations
# ─────────────────────────────────────────────────────────────────────────────

function Invoke-ImportStep {
    param($Step, [hashtable]$Context, [ref]$ImportedObjects, [bool]$IsDelta)

    $conn       = $Context.Connection
    $config     = $Context.Config
    $runId      = $Context.RunId

    # Determine which entities to import.
    $entityNames = if ($Step.Entities) {
        @($Step.Entities)
    } else {
        @($config.Entities.Keys)
    }

    foreach ($entityName in $entityNames) {
        $entityConfig = $config.Entities[$entityName]
        if (-not $entityConfig) {
            Write-EngineLog -Level Warning -Component 'PipelineExecutor' `
                -Message "Import step: entity '$entityName' not found. Skipping." -RunId $runId
            continue
        }

        $connectorName = $entityConfig.ConnectorRef
        $runtime       = $Context.Runtimes[$connectorName]
        if (-not $runtime) {
            Write-EngineLog -Level Warning -Component 'PipelineExecutor' `
                -Message "No loaded runtime for connector '$connectorName'. Skipping entity '$entityName'." -RunId $runId
            continue
        }

        Write-EngineLog -Level Info -Component 'PipelineExecutor' `
            -Message "Importing entity '$entityName' from connector '$connectorName' (Delta=$IsDelta)" -RunId $runId

        $objects = @()
        try {
            if ($IsDelta) {
                $state = Get-ConnectorState -Connection $conn -ConnectorName $connectorName -EntityName $entityName
                $since = $state?.LastRunTimestamp
                $token = $state?.LastToken
                $objects = @(Invoke-ConnectorFunction -Runtime $runtime `
                    -FunctionName 'Import-Delta' `
                    -Arguments @($runtime.Config, $entityConfig, $since, $token, $Context))
            } else {
                $objects = @(Invoke-ConnectorFunction -Runtime $runtime `
                    -FunctionName 'Import-Full' `
                    -Arguments @($runtime.Config, $entityConfig, $Context))
            }
        }
        catch {
            Write-EngineLog -Level Error -Component 'PipelineExecutor' `
                -Message "Import failed for entity '$entityName': $_" -RunId $runId
            throw
        }

        $ImportedObjects.Value[$entityName] = $objects

        # Update connector state watermark.
        Set-ConnectorState -Connection $conn `
            -ConnectorName $connectorName `
            -EntityName    $entityName `
            -LastRunTimestamp (Get-Date) `
            -LastFullImportTimestamp (if (-not $IsDelta) { Get-Date } else { $null })

        Write-EngineLog -Level Info -Component 'PipelineExecutor' `
            -Message "Imported $($objects.Count) objects for '$entityName'." -RunId $runId
    }
}

function Invoke-StageStep {
    param($Step, [hashtable]$Context, [hashtable]$ImportedObjects)

    $conn  = $Context.Connection
    $runId = $Context.RunId

    foreach ($entityName in $ImportedObjects.Keys) {
        $entityConfig  = $Context.Config.Entities[$entityName]
        $connectorName = $entityConfig.ConnectorRef
        $objects       = $ImportedObjects[$entityName]

        $deltaStrategy = $entityConfig.DeltaStrategy?.Type ?? 'Checksum'

        foreach ($obj in $objects) {
            $hash = ''
            if ($deltaStrategy -eq 'Checksum') {
                $hash = Get-RenderedObjectChecksum -RenderedObject $obj
            }

            Set-ObjectState -Connection $conn `
                -ConnectorName $connectorName `
                -EntityName    $entityName `
                -Anchor        $obj.Anchor `
                -ObjectType    $obj.ObjectType `
                -ExternalId    $obj.ExternalId `
                -LastHash      $hash `
                -IsDeleted     $obj.Meta.IsDeleted
        }
    }

    Write-EngineLog -Level Info -Component 'PipelineExecutor' `
        -Message 'Stage step complete.' -RunId $runId
}

function Invoke-JoinAllStep {
    param($Step, [hashtable]$Context, [hashtable]$ImportedObjects)

    $conn  = $Context.Connection
    $runId = $Context.RunId

    foreach ($ruleName in $Context.Config.SyncRules.Keys) {
        $rule         = $Context.Config.SyncRules[$ruleName]
        $inputEntity  = $rule.InputEntityRef
        $outputEntity = $rule.OutputEntityRef

        $inObjects  = if ($ImportedObjects.ContainsKey($inputEntity))  { $ImportedObjects[$inputEntity]  } else { @() }
        $outObjects = if ($ImportedObjects.ContainsKey($outputEntity)) { $ImportedObjects[$outputEntity] } else { @() }

        if ($inObjects.Count -eq 0) { continue }

        Invoke-JoinStep -Connection $conn -SyncRule $rule `
            -InputObjects  $inObjects `
            -OutputObjects $outObjects `
            -Context       $Context | Out-Null
    }

    Write-EngineLog -Level Info -Component 'PipelineExecutor' `
        -Message 'Join step complete.' -RunId $runId
}

function Invoke-TransformStep {
    param($Step, [hashtable]$Context, [hashtable]$ImportedObjects)
    # Transform/calculated attributes are applied here if defined in entity config.
    $runId = $Context.RunId

    foreach ($entityName in $ImportedObjects.Keys) {
        $entityConfig = $Context.Config.Entities[$entityName]
        if (-not $entityConfig.Attributes) { continue }

        foreach ($obj in $ImportedObjects[$entityName]) {
            foreach ($attrDef in $entityConfig.Attributes) {
                if ($attrDef.Calculated -and $attrDef.Calculated.Type -eq 'PowerShell') {
                    try {
                        $val = Invoke-EngineSnippet -Snippet $attrDef.Calculated.Expression -Variables @{
                            InObject = $obj
                            Context  = $Context
                        } -Label "Calculated/$entityName/$($attrDef.AttributeName)"
                        Set-RenderedObjectAttribute -RenderedObject $obj `
                            -AttributeName $attrDef.AttributeName -Value $val
                    }
                    catch {
                        Write-EngineLog -Level Warning -Component 'PipelineExecutor' `
                            -Message "Calculated attribute '$($attrDef.AttributeName)' on '$($obj.Anchor)' failed: $_" `
                            -RunId $runId
                    }
                }
            }
        }
    }

    Write-EngineLog -Level Info -Component 'PipelineExecutor' `
        -Message 'Transform step complete.' -RunId $runId
}

function Invoke-ExportAllStep {
    param($Step, [hashtable]$Context, [hashtable]$ImportedObjects, [bool]$WhatIf)

    $conn        = $Context.Connection
    $runId       = $Context.RunId
    $aggregated  = @{ Created = 0; Updated = 0; Deleted = 0; Errors = 0 }

    $ruleNames = if ($Step.SyncRules) { @($Step.SyncRules) } else { @($Context.Config.SyncRules.Keys) }

    foreach ($ruleName in $ruleNames) {
        $rule         = $Context.Config.SyncRules[$ruleName]
        $inputEntity  = $rule.InputEntityRef
        $outputEntity = $rule.OutputEntityRef

        $outEntityConfig = $Context.Config.Entities[$outputEntity]
        $outConnName     = $outEntityConfig.ConnectorRef
        $outRuntime      = $Context.Runtimes[$outConnName]
        if (-not $outRuntime) {
            Write-EngineLog -Level Warning -Component 'PipelineExecutor' `
                -Message "Export: no runtime for connector '$outConnName'. Skipping rule '$ruleName'." -RunId $runId
            continue
        }

        $inObjects  = if ($ImportedObjects.ContainsKey($inputEntity))  { $ImportedObjects[$inputEntity]  } else { @() }
        $outObjects = if ($ImportedObjects.ContainsKey($outputEntity)) { $ImportedObjects[$outputEntity] } else { @() }

        $ops = [System.Collections.Generic.List[hashtable]]::new()

        foreach ($inObj in $inObjects) {
            # Entitlement check.
            $entitled = Invoke-EntitlementCheck -SyncRule $rule -InObject $inObj -Context $Context
            if (-not $entitled) {
                # If not entitled and object exists in target, delete/deprovision.
                $match = $outObjects | Where-Object {
                    $_.ExternalId -eq $inObj.ExternalId -or $_.Anchor -eq $inObj.Anchor
                } | Select-Object -First 1
                if ($match -and $rule.DeprovisionOnDisentitle -eq $true) {
                    $ops.Add((New-ChangeOperation -Operation 'Delete' -OutRuntime $outRuntime `
                        -EntityConfig $outEntityConfig -Anchor $match.Anchor))
                }
                continue
            }

            # Find existing output object for this input anchor.
            $existingOut = $null
            $joinLinks   = @()
            try {
                $mv = Get-MetaverseEntry -Connection $conn `
                    -ConnectorName $inputEntity -EntityName $inputEntity -Anchor $inObj.Anchor
                if ($mv) {
                    $joinLinks = @(Get-JoinedObjects -Connection $conn -MvId $mv.MvId)
                    $outLink   = $joinLinks | Where-Object {
                        $_.ConnectorName -eq $outputEntity -and $_.EntityName -eq $outputEntity
                    } | Select-Object -First 1
                    if ($outLink) {
                        $existingOut = $outObjects | Where-Object { $_.Anchor -eq $outLink.Anchor } | Select-Object -First 1
                        if (-not $existingOut) {
                            # Fetch from connector if not in imported set.
                            try {
                                $existingOut = Invoke-ConnectorFunction -Runtime $outRuntime `
                                    -FunctionName 'Get-Object' `
                                    -Arguments @($outRuntime.Config, $outEntityConfig, $outLink.Anchor, $Context)
                            } catch {}
                        }
                    }
                }
            } catch {}

            # Apply mappings to produce desired state.
            $desired = Invoke-MappingStep -SyncRule $rule -InObject $inObj `
                -ExistingOutObject $existingOut -Context $Context

            if ($null -eq $existingOut) {
                # Existence check.
                $exists = $false
                $existenceSnippet = $rule.ExistenceCheck
                if ($existenceSnippet) {
                    try {
                        $exists = [bool](Invoke-EngineSnippet -Snippet $existenceSnippet -Variables @{
                            InObject  = $inObj
                            OutObject = $desired
                            Context   = $Context
                        } -Label "ExistenceCheck/$ruleName")
                    } catch {}
                } else {
                    # Delegate to connector Test-Exists.
                    try {
                        $testResult = Invoke-ConnectorFunction -Runtime $outRuntime `
                            -FunctionName 'Test-Exists' `
                            -Arguments @($outRuntime.Config, $outEntityConfig, $desired, $Context)
                        $exists = $testResult.Exists
                        if ($exists -and $testResult.Anchor) {
                            $existingOut = @{ Anchor = $testResult.Anchor; Attributes = @{} }
                        }
                    } catch {}
                }

                if ($exists -and $existingOut) {
                    # Object exists but wasn't in imported set – compute patch.
                    $patch = Get-AttributePatch -Current $existingOut -Desired $desired
                    if ($patch.Count -gt 0) {
                        $ops.Add((New-ChangeOperation -Operation 'Update' -OutRuntime $outRuntime `
                            -EntityConfig $outEntityConfig -Anchor $existingOut.Anchor -Patch $patch))
                    }
                } elseif (-not $exists) {
                    $ops.Add((New-ChangeOperation -Operation 'Create' -OutRuntime $outRuntime `
                        -EntityConfig $outEntityConfig -Object $desired))
                }
            } else {
                # Compute attribute patch.
                $patch = Get-AttributePatch -Current $existingOut -Desired $desired
                if ($patch.Count -gt 0) {
                    $ops.Add((New-ChangeOperation -Operation 'Update' -OutRuntime $outRuntime `
                        -EntityConfig $outEntityConfig -Anchor $existingOut.Anchor -Patch $patch))
                }
            }
        }

        if ($ops.Count -gt 0) {
            $expResult = Invoke-ExportStep -Operations $ops.ToArray() `
                -Connection $conn -Context $Context -WhatIf $WhatIf
            foreach ($k in $expResult.Keys) { $aggregated[$k] += $expResult[$k] }
        }

        Write-EngineLog -Level Info -Component 'PipelineExecutor' `
            -Message "Export rule '$ruleName': $($ops.Count) operations planned." -RunId $runId
    }

    return $aggregated
}
