# Scripts

Account-level Databricks administration helpers. These scripts target the Databricks Accounts SCIM API (`/api/2.0/accounts/{accountId}/scim/v2`) and authenticate via the current Az session — no separate client credentials path.

---

## New-DatabricksAccountAdmin.ps1

Provisions a user or service principal into the Databricks account (if not already present) and assigns the built-in `account_admin` role via SCIM PATCH.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `AccountID` | `string` | Yes (or `$env:DATABRICKS_ACCOUNT_ID`) | Databricks account UUID. Visible in the Accounts Console at [accounts.azuredatabricks.net](https://accounts.azuredatabricks.net) → profile menu (top-right). |
| `PrincipalObjectId` | `string` | Yes (ByObjectId set) | Entra ID object ID of the principal. For service principals, supply the **application (client) ID**, not the enterprise object ID. Mutually exclusive with `-UserPrincipalName`. |
| `UserPrincipalName` | `string` | Yes (ByUPN set) | UPN of the user (e.g. `alice@contoso.com`). Resolved to an Entra ID object ID via Microsoft Graph `v1.0/users?$filter=userPrincipalName eq ...`. Mutually exclusive with `-PrincipalObjectId`. |
| `PrincipalType` | `string` | No | `'User'` (default) or `'ServicePrincipal'`. Determines which SCIM endpoint is used (`/Users` vs `/ServicePrincipals`) and which lookup field is used (`userName` vs `applicationId`). |
| `WhatIfEnabled` | `bool` | No | When `$true`, reports what would happen without making changes. Defaults to `$env:IS_PULL_REQUEST` — set automatically on PR pipeline runs. |

### Execution Flow

1. **Token acquisition** — `Get-AzAccessToken` against `https://accounts.azuredatabricks.net/`. Falls back to resource GUID `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d` for older Az module versions. Az 12+ `SecureString` return is handled transparently.
2. **UPN resolution** (ByUPN parameter set only) — Microsoft Graph `v1.0/users` lookup using `$filter` to avoid path-encoding issues with special characters in UPNs.
3. **SCIM lookup** — searches the Databricks account for the principal by `userName` (users) or `applicationId` (service principals).
4. **SCIM provision** (if not found) — `POST /scim/v2/Users` or `/scim/v2/ServicePrincipals`.
5. **Role assignment** — `PATCH /scim/v2/{Users|ServicePrincipals}/{id}` with `op: add` on `roles`, value `account_admin`.

### Usage

```powershell
# Add a user by UPN
.\New-DatabricksAccountAdmin.ps1 `
    -AccountID         '00000000-0000-0000-0000-000000000000' `
    -UserPrincipalName 'alice@contoso.com'

# Add a service principal by Entra ID application (client) ID
.\New-DatabricksAccountAdmin.ps1 `
    -AccountID         '00000000-0000-0000-0000-000000000000' `
    -PrincipalObjectId 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' `
    -PrincipalType     'ServicePrincipal'

# Preview without changes
.\New-DatabricksAccountAdmin.ps1 `
    -AccountID         '00000000-0000-0000-0000-000000000000' `
    -UserPrincipalName 'alice@contoso.com' `
    -WhatIfEnabled     $true
```

### Notes

- The executing identity must already hold `Account Admin` in the Databricks Accounts Console.
- UPN resolution requires `Directory.Read.All` on Microsoft Graph (delegated or application).
- The script is idempotent: if the principal is already provisioned and already holds `account_admin`, the PATCH operation is still issued but has no net effect.
- `SupportsShouldProcess` is declared — the native PowerShell `-WhatIf` switch is also respected alongside `-WhatIfEnabled`.
