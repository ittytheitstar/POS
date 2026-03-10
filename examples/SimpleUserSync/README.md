# SimpleUserSync Example

This example demonstrates a complete SQL → Active Directory user synchronization
using the Object Sync Engine.

## What it does

- **Source**: `dbo.Users` table in an MSSQL HR database (`HRDatabase`)
- **Target**: Active Directory users in `OU=SyncedUsers,DC=corp,DC=local`
- **Join**: on `employeeId`
- **Entitlement**: user is active (`IsEnabled = 1`) AND has a non-empty `Department`
- **Mappings**: `displayName`, `mail`, `department`, `title`, `sAMAccountName` (computed), `userPrincipalName`, `cn`, `memberOf`
- **Deprovision**: disables AD account (soft-delete) when entitlement fails

## Prerequisites

1. **SQL Server** accessible at `localhost\SQLEXPRESS` with a database called `HRDatabase`
   containing a `dbo.Users` table (schema: `EmployeeId`, `FirstName`, `LastName`,
   `DisplayName`, `Email`, `Department`, `Title`, `IsEnabled`, `ModifiedDate`, `CreatedDate`).

2. **Engine database** at `localhost\SQLEXPRESS` in database `ObjectSyncEngine`
   (engine auto-creates all required tables).

3. **Active Directory** accessible from the running machine.
   Adjust `Connection.Server` and `Connection.Domain` in `connectors/ad.json`.
   Ensure the OU `OU=SyncedUsers,DC=corp,DC=local` exists, or update `SearchBase` in
   `entities/adUser.json`.

4. **PowerShell 5.1+** or **PowerShell 7+** running with sufficient privileges.

## Quick start

```powershell
# 1. Import the engine module
Import-Module .\src\Engine\ObjectSyncEngine.psd1

# 2. Run a full sync (plan only first to review)
Invoke-ObjectSync -Profile "Full" `
    -ConfigPath ".\examples\SimpleUserSync" `
    -WhatIf

# 3. Run for real
Invoke-ObjectSync -Profile "Full" `
    -ConfigPath ".\examples\SimpleUserSync"

# 4. Subsequent runs can use Delta
Invoke-ObjectSync -Profile "Delta" `
    -ConfigPath ".\examples\SimpleUserSync"
```

## Customisation

| What to change | File | Field |
|---|---|---|
| SQL Server / database name | `connectors/mssql.json` | `Connection.Server`, `Connection.Database` |
| SQL authentication | `connectors/mssql.json` | set `IntegratedSecurity: false`, `Username`, `PasswordEnvVar` |
| AD domain controller | `connectors/ad.json` | `Connection.Server`, `Connection.Domain` |
| AD search base / OU | `entities/adUser.json` | `ConnectorMetadata.SearchBase` |
| SQL table name | `entities/sqlUser.json` | `ConnectorMetadata.TableName` |
| SQL column names | `entities/sqlUser.json` | each attribute's `ConnectorMetadata.ColumnName` |
| Join key | `syncRules/sqlUser_to_adUser.json` | `JoinCondition` expression |
| Entitlement logic | `syncRules/sqlUser_to_adUser.json` | `EntitlementCondition` expression |
| Attribute mappings | `syncRules/sqlUser_to_adUser.json` | `ColumnMappings` array |

## SQL schema example

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
