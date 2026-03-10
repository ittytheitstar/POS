#Requires -Version 5.1
<#
.SYNOPSIS
    RenderedObject schema definition, factory, and normalisation helpers.
.DESCRIPTION
    All connector plugins produce and consume RenderedObject instances.
    This file defines the canonical schema and provides New-RenderedObject,
    Set-RenderedObjectAttribute, and normalisation utilities used for
    checksum computation and deterministic comparisons.

    RenderedObject fields
    ─────────────────────
    ObjectType          string   – canonical type name  (e.g. "User")
    ObjectFriendlyName  string   – human label          (e.g. "John Smith")
    Anchor              string   – stable per-connector identifier
    ExternalId          string   – optional stable business key (e.g. employeeId)
    Attributes          hashtable<string, object[]>
                                 – ALL values stored as arrays; single-value
                                   attributes are arrays of one element.
    Meta                hashtable
      Created           datetime
      Modified          datetime
      IsDeleted         bool
      SourceConnector   string
      Raw               object   – optional original data from connector
#>

Set-StrictMode -Version Latest

function New-RenderedObject {
    <#
    .SYNOPSIS Creates a new, empty RenderedObject with canonical defaults.
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)] [string]$ObjectType,
        [Parameter(Mandatory)] [string]$Anchor,
        [string]$ObjectFriendlyName = '',
        [string]$ExternalId = '',
        [string]$SourceConnector = ''
    )

    return @{
        ObjectType         = $ObjectType
        ObjectFriendlyName = $ObjectFriendlyName
        Anchor             = $Anchor
        ExternalId         = $ExternalId
        Attributes         = @{}
        Meta               = @{
            Created         = $null
            Modified        = $null
            IsDeleted       = $false
            SourceConnector = $SourceConnector
            Raw             = $null
        }
    }
}

function Set-RenderedObjectAttribute {
    <#
    .SYNOPSIS
        Sets (or replaces) a named attribute on a RenderedObject.
        Values are always stored as arrays to support multi-valued attributes.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [hashtable]$RenderedObject,
        [Parameter(Mandatory)] [string]$AttributeName,
        [Parameter(Mandatory)] [AllowNull()] $Value
    )

    if ($null -eq $Value) {
        $RenderedObject.Attributes[$AttributeName] = @()
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $RenderedObject.Attributes[$AttributeName] = @($Value)
    } else {
        $RenderedObject.Attributes[$AttributeName] = @($Value)
    }
}

function Add-RenderedObjectAttributeValue {
    <#
    .SYNOPSIS
        Appends a value to a multi-valued attribute (merge semantics).
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [hashtable]$RenderedObject,
        [Parameter(Mandatory)] [string]$AttributeName,
        [AllowNull()] $Value
    )

    if (-not $RenderedObject.Attributes.ContainsKey($AttributeName)) {
        $RenderedObject.Attributes[$AttributeName] = @()
    }

    if ($null -ne $Value) {
        $RenderedObject.Attributes[$AttributeName] += $Value
    }
}

function Get-RenderedObjectAttribute {
    <#
    .SYNOPSIS
        Returns the stored value array for an attribute, or an empty array.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [hashtable]$RenderedObject,
        [Parameter(Mandatory)] [string]$AttributeName
    )

    if ($RenderedObject.Attributes.ContainsKey($AttributeName)) {
        # Return as array – wrap to prevent PowerShell pipeline unwrapping.
        return , @($RenderedObject.Attributes[$AttributeName])
    }
    return , @()
}

function Get-RenderedObjectSingleValue {
    <#
    .SYNOPSIS
        Returns the first element of an attribute array, or $null.
        Convenience wrapper for single-valued attributes.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [hashtable]$RenderedObject,
        [Parameter(Mandatory)] [string]$AttributeName
    )

    $vals = Get-RenderedObjectAttribute -RenderedObject $RenderedObject -AttributeName $AttributeName
    if ($vals.Count -gt 0) { return $vals[0] }
    return $null
}

function ConvertTo-NormalisedRenderedObject {
    <#
    .SYNOPSIS
        Returns a deterministic, canonical representation of a RenderedObject
        suitable for checksum computation.
    .DESCRIPTION
        - All attribute arrays are sorted (using string comparison on ToString())
        - String values are trimmed
        - Volatile fields (Meta.Raw, Meta.Modified) are excluded
        - Attributes dictionary is sorted by key
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)] [hashtable]$RenderedObject
    )

    $normAttrs = [ordered]@{}
    $sortedKeys = $RenderedObject.Attributes.Keys | Sort-Object
    foreach ($key in $sortedKeys) {
        $raw = $RenderedObject.Attributes[$key]
        $normalised = @($raw |
            ForEach-Object {
                if ($_ -is [string]) { $_.Trim() } else { $_ }
            } |
            Sort-Object { $_.ToString() })
        $normAttrs[$key] = $normalised
    }

    return [ordered]@{
        ObjectType         = $RenderedObject.ObjectType
        Anchor             = $RenderedObject.Anchor
        ExternalId         = $RenderedObject.ExternalId
        Attributes         = $normAttrs
    }
}

function Get-RenderedObjectChecksum {
    <#
    .SYNOPSIS
        Computes a SHA-256 checksum from the normalised form of a RenderedObject.
    #>
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)] [hashtable]$RenderedObject
    )

    $normalised = ConvertTo-NormalisedRenderedObject -RenderedObject $RenderedObject
    $json = $normalised | ConvertTo-Json -Depth 10 -Compress
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
    $hash  = [System.Security.Cryptography.SHA256]::Create().ComputeHash($bytes)
    return [BitConverter]::ToString($hash) -replace '-',''
}

function Compare-RenderedObjects {
    <#
    .SYNOPSIS
        Returns $true when two RenderedObjects have identical normalised content
        (ignores volatile meta fields).
    #>
    [CmdletBinding()]
    [OutputType([bool])]
    param(
        [Parameter(Mandatory)] [hashtable]$A,
        [Parameter(Mandatory)] [hashtable]$B
    )

    return (Get-RenderedObjectChecksum -RenderedObject $A) -eq `
           (Get-RenderedObjectChecksum -RenderedObject $B)
}

function Get-AttributePatch {
    <#
    .SYNOPSIS
        Compares two RenderedObjects and returns a hashtable of attributes
        that differ (keyed by attribute name, value = new value array).
        Used to build minimal update patches.
    #>
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)] [hashtable]$Current,
        [Parameter(Mandatory)] [hashtable]$Desired
    )

    $patch = @{}

    $allKeys = @($Current.Attributes.Keys) + @($Desired.Attributes.Keys) |
                Select-Object -Unique

    foreach ($key in $allKeys) {
        $cur = @($Current.Attributes[$key])
        $des = @($Desired.Attributes[$key])

        # Normalise for comparison.
        $curSorted = @($cur | ForEach-Object { if ($_ -is [string]) { $_.Trim() } else { $_ } } | Sort-Object { $_.ToString() })
        $desSorted = @($des | ForEach-Object { if ($_ -is [string]) { $_.Trim() } else { $_ } } | Sort-Object { $_.ToString() })

        $curJson = $curSorted | ConvertTo-Json -Compress -Depth 5
        $desJson = $desSorted | ConvertTo-Json -Compress -Depth 5

        if ($curJson -ne $desJson) {
            $patch[$key] = $des
        }
    }

    return $patch
}
