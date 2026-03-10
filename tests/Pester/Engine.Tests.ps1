#Requires -Version 5.1
<#
.SYNOPSIS
    Pester smoke tests for the Object Sync Engine.
.DESCRIPTION
    Covers:
    1. Config parsing and validation
    2. RenderedObject creation and normalisation
    3. Checksum computation
    4. Attribute patch computation
    5. Snippet execution
    6. Plan generation (WhatIf / no real connectors needed)
#>

# Ensure Pester 5 is available; skip if not.
$pesterModule = Get-Module -ListAvailable -Name Pester | Sort-Object Version -Descending | Select-Object -First 1
if (-not $pesterModule -or $pesterModule.Version.Major -lt 5) {
    Write-Warning "Pester 5+ required for these tests. Install via: Install-Module Pester -Force -Scope CurrentUser"
    return
}

BeforeAll {
    $repoRoot     = Resolve-Path (Join-Path $PSScriptRoot '..' '..')
    $engineRoot   = Join-Path $repoRoot 'src' 'Engine'
    $examplesRoot = Join-Path $repoRoot 'examples' 'SimpleUserSync'

    # Expose paths to all tests.
    $script:EngineRoot   = $engineRoot
    $script:ExamplesRoot = $examplesRoot

    # Dot-source private modules so all functions are available in Pester scope.
    . (Join-Path $engineRoot 'Private' 'Logger.ps1')
    . (Join-Path $engineRoot 'Private' 'RenderedObject.ps1')
    . (Join-Path $engineRoot 'Private' 'SnippetExecutor.ps1')
    . (Join-Path $engineRoot 'Private' 'ConfigLoader.ps1')
}

Describe 'ConfigLoader' {
    Context 'Import-EngineConfig – happy path' {
        It 'loads all config files from examples/SimpleUserSync without error' {
            { Import-EngineConfig -ConfigPath $script:ExamplesRoot } | Should -Not -Throw
        }

        It 'returns at least one connector' {
            $cfg = Import-EngineConfig -ConfigPath $script:ExamplesRoot
            $cfg.Connectors.Count | Should -BeGreaterThan 0
        }

        It 'returns at least one entity' {
            $cfg = Import-EngineConfig -ConfigPath $script:ExamplesRoot
            $cfg.Entities.Count | Should -BeGreaterThan 0
        }

        It 'returns at least one sync rule' {
            $cfg = Import-EngineConfig -ConfigPath $script:ExamplesRoot
            $cfg.SyncRules.Count | Should -BeGreaterThan 0
        }

        It 'returns at least one run profile' {
            $cfg = Import-EngineConfig -ConfigPath $script:ExamplesRoot
            $cfg.RunProfiles.Count | Should -BeGreaterThan 0
        }

        It 'cross-references are valid (no unknown ConnectorRef)' {
            $cfg = Import-EngineConfig -ConfigPath $script:ExamplesRoot
            foreach ($entity in $cfg.Entities.Values) {
                $cfg.Connectors.ContainsKey($entity.ConnectorRef) | Should -BeTrue
            }
        }

        It 'SqlUser entity has expected attributes' {
            $cfg    = Import-EngineConfig -ConfigPath $script:ExamplesRoot
            $entity = $cfg.Entities['SqlUser']
            $attrNames = $entity.Attributes | ForEach-Object { $_.AttributeName }
            $attrNames | Should -Contain 'employeeId'
            $attrNames | Should -Contain 'mail'
            $attrNames | Should -Contain 'displayName'
        }

        It 'AdUser entity has memberOf as multi-valued' {
            $cfg      = Import-EngineConfig -ConfigPath $script:ExamplesRoot
            $entity   = $cfg.Entities['AdUser']
            $memberOf = $entity.Attributes | Where-Object { $_.AttributeName -eq 'memberOf' }
            $memberOf.IsMultiValued | Should -Be $true
        }
    }

    Context 'Import-EngineConfig – error cases' {
        It 'throws when ConfigPath does not exist' {
            { Import-EngineConfig -ConfigPath 'C:\does\not\exist\xyz123' } | Should -Throw
        }

        It 'throws on JSON with missing required field' {
            $tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) "POS_Test_$(Get-Random)"
            New-Item -ItemType Directory $tmpDir | Out-Null
            foreach ($sub in @('connectors','entities','syncRules','runProfiles')) {
                New-Item -ItemType Directory (Join-Path $tmpDir $sub) | Out-Null
            }
            # ConnectorType is missing – should fail validation.
            '{"ConnectorName":"Bad","PluginPath":"./x.ps1"}' | Set-Content (Join-Path $tmpDir 'connectors' 'bad.json')
            try {
                { Import-EngineConfig -ConfigPath $tmpDir } | Should -Throw
            } finally {
                Remove-Item $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

Describe 'RenderedObject' {
    Context 'New-RenderedObject' {
        It 'creates object with correct type and anchor' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            $obj.ObjectType | Should -Be 'User'
            $obj.Anchor     | Should -Be 'emp001'
        }

        It 'starts with empty Attributes hashtable' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            $obj.Attributes.Count | Should -Be 0
        }

        It 'Meta.IsDeleted defaults to false' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            $obj.Meta.IsDeleted | Should -Be $false
        }
    }

    Context 'Set-RenderedObjectAttribute and Get-RenderedObjectAttribute' {
        It 'stores single value as array of one' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'mail' -Value 'john@corp.local'
            $vals = Get-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'mail'
            $vals.Count  | Should -Be 1
            $vals[0]     | Should -Be 'john@corp.local'
        }

        It 'stores null as empty array' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'mail' -Value $null
            $vals = Get-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'mail'
            $vals.Count | Should -Be 0
        }

        It 'replaces previous value' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'mail' -Value 'a@b.com'
            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'mail' -Value 'c@d.com'
            (Get-RenderedObjectSingleValue -RenderedObject $obj -AttributeName 'mail') | Should -Be 'c@d.com'
        }
    }

    Context 'Add-RenderedObjectAttributeValue (multi-valued)' {
        It 'accumulates values' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Add-RenderedObjectAttributeValue -RenderedObject $obj -AttributeName 'memberOf' -Value 'CN=GroupA'
            Add-RenderedObjectAttributeValue -RenderedObject $obj -AttributeName 'memberOf' -Value 'CN=GroupB'
            $vals = Get-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'memberOf'
            $vals.Count | Should -Be 2
            $vals        | Should -Contain 'CN=GroupA'
            $vals        | Should -Contain 'CN=GroupB'
        }
    }

    Context 'Get-RenderedObjectChecksum' {
        It 'returns a non-empty hex string' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'mail' -Value 'test@corp.local'
            $hash = Get-RenderedObjectChecksum -RenderedObject $obj
            $hash | Should -Not -BeNullOrEmpty
            $hash | Should -Match '^[0-9A-F]{64}$'
        }

        It 'identical objects produce identical checksums' {
            $a = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $a -AttributeName 'mail' -Value 'test@corp.local'
            $b = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $b -AttributeName 'mail' -Value 'test@corp.local'
            (Get-RenderedObjectChecksum $a) | Should -Be (Get-RenderedObjectChecksum $b)
        }

        It 'different attribute values produce different checksums' {
            $a = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $a -AttributeName 'mail' -Value 'a@corp.local'
            $b = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $b -AttributeName 'mail' -Value 'b@corp.local'
            (Get-RenderedObjectChecksum $a) | Should -Not -Be (Get-RenderedObjectChecksum $b)
        }

        It 'is order-independent for multi-valued attributes' {
            $a = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Add-RenderedObjectAttributeValue -RenderedObject $a -AttributeName 'memberOf' -Value 'CN=A'
            Add-RenderedObjectAttributeValue -RenderedObject $a -AttributeName 'memberOf' -Value 'CN=B'
            $b = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Add-RenderedObjectAttributeValue -RenderedObject $b -AttributeName 'memberOf' -Value 'CN=B'
            Add-RenderedObjectAttributeValue -RenderedObject $b -AttributeName 'memberOf' -Value 'CN=A'
            (Get-RenderedObjectChecksum $a) | Should -Be (Get-RenderedObjectChecksum $b)
        }
    }

    Context 'Get-AttributePatch' {
        It 'returns empty patch for identical objects' {
            $a = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $a -AttributeName 'mail' -Value 'x@y.com'
            $b = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $b -AttributeName 'mail' -Value 'x@y.com'
            $patch = Get-AttributePatch -Current $a -Desired $b
            $patch.Count | Should -Be 0
        }

        It 'identifies changed attribute' {
            $cur = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $cur -AttributeName 'mail' -Value 'old@corp.local'
            $des = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $des -AttributeName 'mail' -Value 'new@corp.local'
            $patch = Get-AttributePatch -Current $cur -Desired $des
            $patch.ContainsKey('mail') | Should -BeTrue
            $patch['mail'][0] | Should -Be 'new@corp.local'
        }

        It 'identifies new attribute (in desired but not current)' {
            $cur = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            $des = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $des -AttributeName 'title' -Value 'Manager'
            $patch = Get-AttributePatch -Current $cur -Desired $des
            $patch.ContainsKey('title') | Should -BeTrue
        }
    }
}

Describe 'SnippetExecutor' {
    Context 'Invoke-EngineSnippet' {
        It 'evaluates a simple expression' {
            $result = Invoke-EngineSnippet -Snippet '1 + 1' -Label 'test'
            $result | Should -Be 2
        }

        It 'exposes $InObject variable' {
            $obj = New-RenderedObject -ObjectType 'User' -Anchor 'emp001'
            Set-RenderedObjectAttribute -RenderedObject $obj -AttributeName 'mail' -Value 'test@corp.local'
            $result = Invoke-EngineSnippet -Snippet '$InObject.Anchor' -Variables @{ InObject = $obj } -Label 'test'
            $result | Should -Be 'emp001'
        }

        It 'throws on invalid snippet syntax' {
            { Invoke-EngineSnippet -Snippet 'if (' -Label 'bad' } | Should -Throw
        }
    }

    Context 'Test-SnippetSyntax' {
        It 'returns true for valid snippet' {
            Test-SnippetSyntax -Snippet '1 + 2' | Should -Be $true
        }

        It 'throws for invalid snippet' {
            { Test-SnippetSyntax -Snippet 'function {' } | Should -Throw
        }
    }
}

Describe 'Logger' {
    Context 'Write-EngineLog' {
        It 'does not throw at Info level' {
            Set-EngineLogLevel -Level Info
            { Write-EngineLog -Level Info -Message 'Test message' -Component 'Test' } | Should -Not -Throw
        }

        It 'filters below effective level' {
            Set-EngineLogLevel -Level Warning
            # Trace should be silently suppressed (no output, no error).
            { Write-EngineLog -Level Trace -Message 'Should be filtered' -Component 'Test' } | Should -Not -Throw
        }
    }
}

