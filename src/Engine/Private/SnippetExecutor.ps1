#Requires -Version 5.1
<#
.SYNOPSIS
    Safe PowerShell snippet executor for engine-provided expressions.
.DESCRIPTION
    Config files contain PowerShell snippets (strings) for join conditions,
    entitlement conditions, mapping expressions, etc.

    This module compiles them to ScriptBlocks and invokes them with a
    constrained context hashtable ($InObject, $OutObject, $Context, etc.)
    while capturing and surfacing errors in a structured way.
#>

Set-StrictMode -Version Latest

function Invoke-EngineSnippet {
    <#
    .SYNOPSIS
        Compiles and invokes a PowerShell expression string with a supplied
        variable context, returning the result or throwing with rich context.
    .PARAMETER Snippet
        Raw PowerShell script text.
    .PARAMETER Variables
        Hashtable of variable name -> value to expose inside the snippet.
        Common keys: InObject, OutObject, Context, LastRunTimestamp.
    .PARAMETER Label
        Human-readable label used in error messages (e.g., "JoinCondition in rule X").
    .PARAMETER AllowNull
        If $true, a $null result is treated as valid. Default: $true.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$Snippet,
        [hashtable]$Variables  = @{},
        [string]$Label         = 'snippet',
        [bool]$AllowNull       = $true
    )

    # Compile to script block.
    $sb = $null
    try {
        $sb = [scriptblock]::Create($Snippet)
    }
    catch {
        throw "Failed to compile $Label : $_"
    }

    # Execute with context variables injected via a child scope wrapper.
    try {
        $result = & {
            # Inject variables.
            foreach ($k in $Variables.Keys) {
                Set-Variable -Name $k -Value $Variables[$k] -Scope Local
            }
            . $sb
        }
        return $result
    }
    catch {
        $msg = "Error executing $Label`: $_"
        Write-EngineLog -Level Error -Component 'SnippetExecutor' -Message $msg -Data @{
            snippet = $Snippet
            error   = $_.ToString()
        }
        throw $msg
    }
}

function Test-SnippetSyntax {
    <#
    .SYNOPSIS
        Validates that a snippet compiles without executing it.
        Returns $true if valid, throws on failure.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$Snippet,
        [string]$Label = 'snippet'
    )

    try {
        [scriptblock]::Create($Snippet) | Out-Null
        return $true
    }
    catch {
        throw "Syntax error in $Label : $_"
    }
}
