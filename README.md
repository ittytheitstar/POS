# PowerShell Object Sync (POS)

A modern, fully-configurable, **MIM-like generic object synchronization engine** written in PowerShell.

Synchronize identities (users, groups, devices, …) between any two systems – SQL, Active Directory, REST APIs, LDAP – driven entirely by **JSON configuration files** and **pluggable connector scripts**.

---

## Features

| Feature | Detail |
|---|---|
| JSON-driven | Every rule, mapping, entity and run profile is a JSON file – no code changes needed for new use cases |
| Pluggable connectors | Drop in a `connector-plugin.ps1` for any system |
| Metaverse / join engine | Canonical object identity across connectors (like MIM/FIM) |
| Checksum + watermark delta | Efficient incremental syncs via timestamp columns or SHA-256 checksums |
| MSSQL engine schema | Auto-creates tables for run history, object state, join links, dead-letter |
| Retry + dead-letter | Failed export operations are retried with backoff and written to Engine.Deadletter |
| WhatIf support | Run `Invoke-ObjectSync … -WhatIf` to preview changes without writing |
| Structured logging | JSON-line log output for easy forwarding to SIEM / log aggregators |
| Pester tests | Smoke test suite for config parsing, checksums, snippet execution |

---

## Repository layout

```
/
  src/
    Engine/
      ObjectSyncEngine.psm1      ← module root (dot-sources Private + Public)
      ObjectSyncEngine.psd1      ← module manifest
      Private/
        Logger.ps1               ← structured JSON-line logger
        ConfigLoader.ps1         ← JSON config parser & validator
        RenderedObject.ps1       ← canonical object model + helpers
        DatabaseManager.ps1      ← engine SQL schema + CRUD helpers
        SnippetExecutor.ps1      ← safe PS snippet execution
        ConnectorLoader.ps1      ← plugin loader
        JoinEngine.ps1           ← metaverse join step
        MappingEngine.ps1        ← attribute mapping + entitlement check
        ExportEngine.ps1         ← export with retry + dead-letter
        PipelineExecutor.ps1     ← full pipeline orchestration
      Public/
        Invoke-ObjectSync.ps1    ← public entrypoint
    Plugins/
      MSSQL/connector-plugin.ps1            ← MSSQL connector
      ActiveDirectory/connector-plugin.ps1  ← AD connector (ADSI fallback)
  config/
    connectors/
      engineDb.json              ← engine database (special connector "EngineDb")
      mssql.json                 ← source SQL connector
      ad.json                    ← target AD connector
    entities/
      sqlUser.json               ← HR SQL user entity definition
      adUser.json                ← Active Directory user entity definition
    syncRules/
      sqlUser_to_adUser.json     ← join + entitlement + mappings
    runProfiles/
      full.json                  ← full import → stage → join → export
      delta.json                 ← delta import → stage → join → export
  examples/
    SimpleUserSync/              ← copy of config + README for quick start
  tests/
    Pester/
      Engine.Tests.ps1           ← smoke tests
  README.md
```

---

## Quick start

### Prerequisites

- **PowerShell 5.1+** or **PowerShell 7+**
- **SQL Server** accessible for the engine schema database (`ObjectSyncEngine` database)
- *Source*: a SQL table of users (see schema below)
- *Target*: an Active Directory domain (or just test with `–WhatIf`)

### 1. Clone / download

```powershell
git clone https://github.com/ittytheitstar/POS
cd POS
```

### 2. Configure

Edit the example config in `examples/SimpleUserSync/`:

| File | What to change |
|---|---|
| `connectors/engineDb.json` | Point `Connection.Server` + `Connection.Database` at your engine DB |
| `connectors/mssql.json` | Point at your HR SQL database |
| `connectors/ad.json` | Set `Connection.Server` and `Connection.Domain` |
| `entities/sqlUser.json` | Update `ConnectorMetadata.TableName` and column names if needed |
| `entities/adUser.json` | Update `ConnectorMetadata.SearchBase` for your OU |

### 3. Run (WhatIf first)

```powershell
Import-Module .\src\Engine\ObjectSyncEngine.psd1

# Preview only – nothing is written
Invoke-ObjectSync -Profile "Full" -ConfigPath ".\examples\SimpleUserSync" -WhatIf

# Execute for real
Invoke-ObjectSync -Profile "Full" -ConfigPath ".\examples\SimpleUserSync"

# Subsequent incremental runs
Invoke-ObjectSync -Profile "Delta" -ConfigPath ".\examples\SimpleUserSync"
```

---

## Configuration reference

### connectors/\*.json

```jsonc
{
  "ConnectorName": "SqlSource",          // unique name referenced by entities
  "ConnectorType": "MSSQL",             // "MSSQL" | "ActiveDirectory" (informational)
  "PluginPath": "../../src/Plugins/MSSQL/connector-plugin.ps1",
  "Connection": {
    "Server": "localhost\\SQLEXPRESS",
    "Database": "HRDatabase",
    "IntegratedSecurity": true           // false → supply Username + PasswordEnvVar
  },
  "Capabilities": {
    "supportsDeltaByTimestamp": true,
    "supportsDeltaByToken": false,
    "supportsAttributeCrud": true,
    "supportsSoftDelete": false
  },
  "Defaults": {
    "RetryCount": 3,
    "RetryDelaySeconds": 2,
    "PageSize": 1000
  }
}
```

> **Special connector**: a connector named `EngineDb` is used as the engine state database.

### entities/\*.json

```jsonc
{
  "EntityName": "SqlUser",
  "ObjectType": "User",                  // canonical type, shared across connectors
  "ConnectorRef": "SqlSource",           // must match a ConnectorName
  "ExternalIdAttribute": "employeeId",   // stable business key
  "ConnectorMetadata": { "TableName": "dbo.Users" },
  "DeltaStrategy": {
    "Type": "Timestamp",                 // "Timestamp" | "Token" | "Checksum"
    "TimestampAttribute": "modifiedDate"
  },
  "Attributes": [
    {
      "AttributeName": "employeeId",
      "AttributeFriendlyName": "Employee ID",
      "DataType": "string",              // string | int | bool | datetime | guid | json
      "IsMultiValued": false,
      "IsKey": true,
      "ConnectorMetadata": { "ColumnName": "EmployeeId", "IsAnchor": true }
    }
  ]
}
```

### syncRules/\*.json

```jsonc
{
  "RuleName": "SqlUser_to_AdUser",
  "InputEntityRef": "SqlUser",
  "OutputEntityRef": "AdUser",
  "JoinCondition": "Get-RenderedObjectSingleValue -RenderedObject $InObject -AttributeName 'employeeId'",
  "EntitlementCondition": "$isEnabled = Get-RenderedObjectSingleValue -RenderedObject $InObject -AttributeName 'isEnabled'; [bool]$isEnabled",
  "ExistenceCheck": null,
  "DeprovisionOnDisentitle": true,
  "ColumnMappings": [
    { "Type": "Direct",     "SourceAttribute": "displayName", "TargetAttribute": "displayName" },
    { "Type": "PowerShell", "TargetAttribute": "sAMAccountName", "Expression": "..." }
  ]
}
```

### runProfiles/\*.json

```jsonc
{
  "ProfileName": "Full",
  "Steps": [
    { "Action": "ImportFull",  "Entities": ["SqlUser", "AdUser"] },
    { "Action": "Stage"  },
    { "Action": "Join"   },
    { "Action": "Transform" },
    { "Action": "Export", "SyncRules": ["SqlUser_to_AdUser"] }
  ]
}
```

Valid `Action` values: `ImportFull`, `ImportDelta`, `Stage`, `Join`, `Transform`, `Export`.

---

## Connector plugin contract

Every connector plugin must implement the following functions:

| Function | Signature | Purpose |
|---|---|---|
| `Initialize-Connector` | `($ConnectorConfig, $EngineContext)` | Validate prereqs, return runtime object |
| `Get-EntitySchema` | `($Connector, $EntityConfig, $EngineContext)` | Return schema metadata |
| `Import-Full` | `($ConnectorConfig, $EntityConfig, $EngineContext)` | Return all RenderedObjects |
| `Import-Delta` | `($ConnectorConfig, $EntityConfig, $SinceTimestamp, $LastToken, $EngineContext)` | Return changed objects |
| `Get-Object` | `($ConnectorConfig, $EntityConfig, $Anchor, $EngineContext)` | Fetch single object |
| `Create-Object` | `($ConnectorConfig, $EntityConfig, $RenderedObject, $EngineContext)` | Create new object |
| `Update-Object` | `($ConnectorConfig, $EntityConfig, $Anchor, $Patch, $EngineContext)` | Apply attribute patch |
| `Delete-Object` | `($ConnectorConfig, $EntityConfig, $Anchor, $EngineContext)` | Delete (or soft-delete) |
| `Set-Attribute` | `($ConnectorConfig, $EntityConfig, $Anchor, $AttributeName, $Value, $EngineContext)` | Set single attribute |
| `Test-Exists` | `($ConnectorConfig, $EntityConfig, $RenderedObject, $EngineContext)` | Returns `@{Exists=$bool; Anchor=<string>}` |

---

## Engine database schema

The engine auto-creates these tables in the `EngineDb` database on first run:

| Table | Purpose |
|---|---|
| `Engine.Run` | Run history (start/end, status, summary JSON) |
| `Engine.ConnectorState` | Per-connector/entity watermark and token |
| `Engine.ObjectState` | Per-object anchor, hash, tombstone, last error |
| `Engine.Metaverse` | Canonical object identity (MvId) |
| `Engine.JoinLink` | Links connector anchor to MvId |
| `Engine.Deadletter` | Failed export operations for retry |
| `Engine.Audit` | Structured audit log of Create/Update/Delete operations |

---

## Running tests

```powershell
# Install Pester 5 if not present
Install-Module Pester -Force -Scope CurrentUser -MinimumVersion 5.0

# Run all smoke tests
Invoke-Pester .\tests\Pester\Engine.Tests.ps1 -Output Detailed
```

---

## Example SQL schema (source table)

```sql
CREATE TABLE dbo.Users (
    EmployeeId   NVARCHAR(64)   NOT NULL PRIMARY KEY,
    FirstName    NVARCHAR(128)  NOT NULL,
    LastName     NVARCHAR(128)  NOT NULL,
    DisplayName  NVARCHAR(256)  NOT NULL,
    Email        NVARCHAR(256)  NULL,
    Department   NVARCHAR(128)  NULL,
    Title        NVARCHAR(128)  NULL,
    IsEnabled    BIT            NOT NULL DEFAULT 1,
    ModifiedDate DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    CreatedDate  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
```

---

## Security notes

- **Never hard-code passwords** in JSON files. Use `PasswordEnvVar` to reference an environment variable name.
- Connector plugins execute PowerShell snippets from config files. Treat config files as trusted input (equivalent to executable code).
- The engine uses parameterized SQL queries throughout to prevent SQL injection.

---

## TODO / known limitations

- **AD connector**: `Create-Object` and `Update-Object` require ADSI write access; test with a service account.
- **Credentials**: implement a secure credential store (Windows DPAPI / Azure Key Vault) for production.
- **Concurrency**: `-MaxParallel` option is scaffolded but full parallel pipeline execution is not yet implemented; each step runs serially.
- **Schema evolution**: adding new Engine tables requires a schema migration step (currently idempotent CREATE IF NOT EXISTS).
- **REST / SCIM connector**: not yet provided; follow the connector plugin contract to add your own.
