#Requires -Version 5.1
<#
.SYNOPSIS
    Mapping engine – applies sync rule column mappings to produce a desired
    RenderedObject from a source RenderedObject.
.DESCRIPTION
    Supports:
    - Direct (1:1 attribute copy)
    - PowerShell expression mappings
    - Multi-valued attributes with Replace or Merge semantics
    - Attribute-level precedence / authoritative source rules
#>

Set-StrictMode -Version Latest

function Invoke-MappingStep {
    <#
    .SYNOPSIS
        Applies all ColumnMappings from a sync rule to the InObject,
        producing a desired state RenderedObject for the output connector.
    .PARAMETER SyncRule
        Parsed sync rule config.
    .PARAMETER InObject
        Source RenderedObject.
    .PARAMETER ExistingOutObject
        Current output RenderedObject (if object already exists); used for
        merge semantics and conflict resolution.
    .PARAMETER Context
        Engine context hashtable.
    .OUTPUTS
        A new RenderedObject representing the desired output state.
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)] $SyncRule,
        [Parameter(Mandatory)] [hashtable]$InObject,
        [hashtable]$ExistingOutObject = $null,
        [Parameter(Mandatory)] [hashtable]$Context
    )

    $outEntityConfig = $Context.Config.Entities[$SyncRule.OutputEntityRef]
    if (-not $outEntityConfig) {
        throw "OutputEntityRef '$($SyncRule.OutputEntityRef)' not found in config."
    }

    $desired = New-RenderedObject `
        -ObjectType         $outEntityConfig.ObjectType `
        -Anchor             ($ExistingOutObject ? $ExistingOutObject.Anchor : '') `
        -ObjectFriendlyName ($ExistingOutObject ? $ExistingOutObject.ObjectFriendlyName : '') `
        -SourceConnector    $outEntityConfig.ConnectorRef

    foreach ($mapping in $SyncRule.ColumnMappings) {
        $targetAttr = $mapping.TargetAttribute
        $mergeMode  = if ($mapping.MergeSemantics -eq 'Merge') { 'Merge' } else { 'Replace' }

        $newValue = $null

        switch ($mapping.Type) {
            'Direct' {
                $srcAttr  = $mapping.SourceAttribute
                $rawVals  = Get-RenderedObjectAttribute -RenderedObject $InObject -AttributeName $srcAttr
                $newValue = $rawVals
            }
            'PowerShell' {
                try {
                    $newValue = Invoke-EngineSnippet -Snippet $mapping.Expression -Variables @{
                        InObject  = $InObject
                        OutObject = $ExistingOutObject
                        Context   = $Context
                    } -Label "Mapping/$($SyncRule.RuleName)/$targetAttr"
                }
                catch {
                    Write-EngineLog -Level Warning -Component 'MappingEngine' `
                        -Message "Mapping expression failed for '$targetAttr': $_" `
                        -RunId $Context.RunId
                    $newValue = $null
                }
            }
            default {
                Write-EngineLog -Level Warning -Component 'MappingEngine' `
                    -Message "Unknown mapping type '$($mapping.Type)' for target '$targetAttr'. Skipping."
                continue
            }
        }

        # Apply value with appropriate semantics.
        if ($mergeMode -eq 'Merge' -and $null -ne $ExistingOutObject) {
            # Merge: combine existing + new, deduplicate.
            $existingVals = Get-RenderedObjectAttribute -RenderedObject $ExistingOutObject -AttributeName $targetAttr
            $combined     = @($existingVals) + @($newValue) | Select-Object -Unique
            Set-RenderedObjectAttribute -RenderedObject $desired -AttributeName $targetAttr -Value $combined
        } else {
            Set-RenderedObjectAttribute -RenderedObject $desired -AttributeName $targetAttr -Value $newValue
        }
    }

    return $desired
}

function Invoke-EntitlementCheck {
    <#
    .SYNOPSIS
        Evaluates the EntitlementCondition expression.
        Returns $true if the object should be provisioned.
    #>
    [CmdletBinding()]
    [OutputType([bool])]
    param(
        [Parameter(Mandatory)] $SyncRule,
        [Parameter(Mandatory)] [hashtable]$InObject,
        [hashtable]$Context = @{}
    )

    $snippet = $SyncRule.EntitlementCondition
    if (-not $snippet) { return $true }

    try {
        $result = Invoke-EngineSnippet -Snippet $snippet -Variables @{
            InObject = $InObject
            Context  = $Context
        } -Label "EntitlementCondition/$($SyncRule.RuleName)"

        return [bool]$result
    }
    catch {
        Write-EngineLog -Level Warning -Component 'MappingEngine' `
            -Message "EntitlementCondition evaluation error for anchor '$($InObject.Anchor)': $_" `
            -RunId $Context.RunId
        return $false
    }
}
