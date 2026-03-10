#Requires -Version 5.1
<#
.SYNOPSIS
    Join engine – builds and maintains the metaverse / join link table.
.DESCRIPTION
    For each imported RenderedObject from an input entity, attempts to find
    a matching object in the output entity (or an existing metaverse entry)
    using the sync rule's JoinCondition snippet. Creates or updates
    Engine.Metaverse and Engine.JoinLink rows via DatabaseManager helpers.
#>

Set-StrictMode -Version Latest

function Invoke-JoinStep {
    <#
    .SYNOPSIS
        Processes join for all staged objects of a sync rule's input entity.
    .PARAMETER Connection
        Open engine DB connection.
    .PARAMETER SyncRule
        Parsed sync rule config object.
    .PARAMETER InputObjects
        Array of RenderedObject hashtables from the input connector.
    .PARAMETER OutputObjects
        Array of RenderedObject hashtables from the output connector (for matching).
    .PARAMETER Context
        Engine context hashtable (RunId, Config, etc.).
    .OUTPUTS
        Array of JoinResult hashtables:
          InObject     - input RenderedObject
          OutObject    - matched output RenderedObject (or $null)
          MvId         - metaverse ID (string GUID)
          IsNewJoin    - bool
          JoinKey      - the key used
    #>
    [CmdletBinding()]
    [OutputType([System.Collections.Generic.List[hashtable]])]
    param(
        [Parameter(Mandatory)] [System.Data.SqlClient.SqlConnection]$Connection,
        [Parameter(Mandatory)] $SyncRule,
        [Parameter(Mandatory)] [hashtable[]]$InputObjects,
        [hashtable[]]$OutputObjects = @(),
        [Parameter(Mandatory)] [hashtable]$Context
    )

    $results = [System.Collections.Generic.List[hashtable]]::new()

    $joinSnippet = if ($SyncRule.JoinCondition) { $SyncRule.JoinCondition } else { '$null' }

    foreach ($inObj in $InputObjects) {
        $joinResult = @{
            InObject  = $inObj
            OutObject = $null
            MvId      = $null
            IsNewJoin = $false
            JoinKey   = $null
        }

        # First try: look up existing metaverse join for this anchor.
        $existing = Get-MetaverseEntry `
            -Connection   $Connection `
            -ConnectorName $SyncRule.InputEntityRef `
            -EntityName    $SyncRule.InputEntityRef `
            -Anchor        $inObj.Anchor

        if ($existing) {
            $joinResult.MvId    = $existing.MvId
            $joinResult.JoinKey = $existing.JoinKey
        }

        # Evaluate JoinCondition to produce a join key.
        $joinKey = $null
        try {
            $joinKey = Invoke-EngineSnippet -Snippet $joinSnippet -Variables @{
                InObject     = $inObj
                OutCandidates = $OutputObjects
                Context      = $Context
            } -Label "JoinCondition/$($SyncRule.RuleName)"
        }
        catch {
            Write-EngineLog -Level Warning -Component 'JoinEngine' `
                -Message "JoinCondition evaluation failed for anchor '$($inObj.Anchor)': $_" `
                -RunId $Context.RunId
        }

        if ($joinKey -is [bool] -and $joinKey -eq $false) {
            # No join – object remains disconnected.
            $results.Add($joinResult)
            continue
        }

        $joinKeyStr = if ($null -ne $joinKey) { $joinKey.ToString() } else { $inObj.Anchor }
        $joinResult.JoinKey = $joinKeyStr

        # Try to find a matching output object by the join key.
        if (-not $joinResult.MvId) {
            $matchedOut = $OutputObjects | Where-Object {
                ($_.ExternalId -and $_.ExternalId -eq $joinKeyStr) -or
                $_.Anchor -eq $joinKeyStr
            } | Select-Object -First 1

            if ($matchedOut) {
                $joinResult.OutObject = $matchedOut

                # Look up or create MvId from output object's existing join link.
                $outExisting = Get-MetaverseEntry `
                    -Connection    $Connection `
                    -ConnectorName $SyncRule.OutputEntityRef `
                    -EntityName    $SyncRule.OutputEntityRef `
                    -Anchor        $matchedOut.Anchor

                if ($outExisting) {
                    $joinResult.MvId = $outExisting.MvId
                }
            }
        }

        # Upsert join link in engine DB.
        $mvId = Set-MetaverseJoin `
            -Connection   $Connection `
            -ConnectorName $SyncRule.InputEntityRef `
            -EntityName    $SyncRule.InputEntityRef `
            -Anchor        $inObj.Anchor `
            -ObjectType    $inObj.ObjectType `
            -CanonicalKey  $joinKeyStr `
            -JoinKey       $joinKeyStr `
            -Confidence    1.0

        $joinResult.MvId     = $mvId
        $joinResult.IsNewJoin = (-not $existing)

        $results.Add($joinResult)
    }

    Write-EngineLog -Level Info -Component 'JoinEngine' `
        -Message "Join step complete for rule '$($SyncRule.RuleName)': $($results.Count) objects processed." `
        -RunId $Context.RunId

    return $results
}
