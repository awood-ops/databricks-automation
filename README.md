# Databricks Automation for Azure Private Connectivity

Production-grade PowerShell automation for Azure Databricks account administration and Network Connectivity Configuration (NCC) rollout. Targets the Databricks Accounts REST API (`api/2.0`) and Accounts SCIM API (`/scim/v2`), with Azure DevOps pipeline orchestration.

## What This Repo Covers

- Account-level admin assignment for users or service principals via the Databricks Accounts SCIM API.
- NCC creation and workspace binding via the Databricks Accounts REST API.
- Managed private endpoint rule registration per target resource type.
- Automated approval of pending private endpoint connections in Azure.
- An Azure DevOps two-stage pipeline that orchestrates the full flow.

## Why This Exists

Databricks NCC + managed private endpoint setup spans two control planes — the Databricks Accounts API and the Azure Resource Manager API — with no unified tooling. The typical workflow requires:

1. OAuth2 token acquisition against the Databricks OIDC endpoint.
2. NCC lifecycle management (create-or-reuse, workspace assignment).
3. Per-resource private endpoint rule creation with correct sub-resource (`groupId`) values.
4. A separate Azure-side approval pass for each pending private endpoint connection.

This repo codifies that sequence into idempotency-aware, CI/CD-safe scripts with optional auto-discovery of PE targets from a resource group.

## Repository Structure

```text
.
├── .gitignore
├── CHANGELOG.md
├── LICENSE
├── README.md
├── pipeline-scripts/
│   └── deployment/
│       ├── README.md
│       ├── Deploy-DatabricksNCC.ps1
│       └── Approve-DatabricksPrivateEndpoints.ps1
├── pipelines/
│   ├── README.md
│   └── Deploy-Databricks-NCC.yaml
└── scripts/
    ├── README.md
    └── New-DatabricksAccountAdmin.ps1
```

## Components

### 1. Account Admin Bootstrap

**File:** `scripts/New-DatabricksAccountAdmin.ps1`

Adds a user or service principal as a Databricks `account_admin` via the Accounts SCIM API (`POST /scim/v2/Users` or `/scim/v2/ServicePrincipals`), provisioning the principal first if they do not already exist in the account.

- Accepts a UPN or Entra ID object ID; resolves UPN → object ID via Microsoft Graph (`v1.0/users?$filter=userPrincipalName eq ...`) when needed.
- For service principals, looks up by `applicationId` (Entra app/client ID), not the enterprise object ID.
- Role assignment uses SCIM PATCH with `op: add` on `roles`.
- Authenticates via current Az session (`Get-AzAccessToken`); the executing identity must already be a Databricks Account Admin.
- Handles Az 12+ `SecureString` token return transparently.

### 2. NCC Deployment and Rule Registration

**File:** `pipeline-scripts/deployment/Deploy-DatabricksNCC.ps1`

Creates or reuses an NCC, assigns it to a workspace, then registers managed private endpoint rules for each target resource.

**Modes:**

| Mode | Trigger | Description |
|------|---------|-------------|
| Manual | `-Resources` | Caller supplies a typed array of `@{ ResourceID; ResourceType }` hashtables. |
| Auto-discover | `-AutoDiscover` | Queries the target resource group via Az PowerShell and builds the resource list. Also resolves `WorkspaceID` from the deployed Databricks workspace resource. |

**Auto-discovery resource coverage and `groupId` mapping:**

| Resource type | Az cmdlet | `ResourceType` (`groupId`) |
|---------------|-----------|---------------------------|
| Storage Account | `Get-AzStorageAccount` | `blob`, `dfs` |
| Key Vault | `Get-AzKeyVault` | `vault` |
| SQL Server | `Get-AzSqlServer` | `sqlServer` |
| Data Factory | `Get-AzDataFactoryV2` | `dataFactory` |
| Cognitive Services / Azure OpenAI | `Get-AzCognitiveServicesAccount` | `account` |
| Event Hub namespace | `Get-AzEventHubNamespace` | `namespace` |
| Service Bus namespace | `Get-AzServiceBusNamespace` | `namespace` |
| Synapse workspace | `Get-AzSynapseWorkspace` | `Sql`, `SqlOnDemand`, `Dev` |

Missing Az sub-modules are warned and skipped; they do not abort the run.

**Authentication:** Supports client credentials flow (service principal `clientId`/`clientSecret` → Databricks OIDC endpoint `https://accounts.azuredatabricks.net/oidc/accounts/{accountId}/v1/token`) or falls back to the current Az session token against the Databricks first-party resource (`2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`).

**Retry:** All Databricks REST calls go through `Invoke-DatabricksRest` — 3 attempts, 5-second back-off.

### 3. Private Endpoint Approval

**File:** `pipeline-scripts/deployment/Approve-DatabricksPrivateEndpoints.ps1`

Approves pending private endpoint connections surfaced on Azure resources after NCC rule registration. Supports the same manual / auto-discovery modes as `Deploy-DatabricksNCC.ps1`.

Additional options:
- `-DescriptionFilter` — regex applied to the PE connection description; useful when a resource hosts PE connections from multiple systems.
- `-WhatIfEnabled` — reports approvals without executing them. Automatically set to `$true` when the `IS_PULL_REQUEST` environment variable is truthy (pull request pipeline runs).

### 4. Azure DevOps Pipeline

**File:** `pipelines/Deploy-Databricks-NCC.yaml`

Two-stage pipeline:

| Stage | Steps |
|-------|-------|
| `Deploy_NCC` | Load `.env` files → resolve resource group and NCC names → run `Deploy-DatabricksNCC.ps1` via `AzurePowerShell@5` task. |
| `Approve_PEs` | Wait `approvalWaitSeconds` → run `Approve-DatabricksPrivateEndpoints.ps1`. |

Pipeline parameters:

| Parameter | Values | Default |
|-----------|--------|---------|
| `env` | `dev`, `tst`, `prd` | — |
| `nccName` | string (optional override) | Derived from naming convention |
| `approvalWaitSeconds` | integer | `300` |

## Authentication Model

### Interactive / Local

```powershell
Connect-AzAccount
# Token is acquired from the Az session.
# Resource URL: https://accounts.azuredatabricks.net/
# Fallback resource GUID: 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d (Databricks first-party app)
```

### CI/CD (Service Principal, Client Credentials)

```powershell
# Token endpoint:
# POST https://accounts.azuredatabricks.net/oidc/accounts/{accountId}/v1/token
# Body: grant_type=client_credentials&client_id=...&client_secret=...&scope=all-apis

$env:CLIENT_ID     = '<sp-client-id>'
$env:CLIENT_SECRET = '<sp-client-secret>'
```

The service principal must hold `Account Admin` in the Databricks Accounts Console. Store `CLIENT_SECRET` as a pipeline secret variable or Key Vault-backed variable group entry — never in source.

## Prerequisites

### PowerShell Modules

```powershell
Install-Module Az -Scope CurrentUser  # Minimum: Az.Accounts, Az.Resources
```

Required sub-modules for `AutoDiscover` mode:

| Module | Purpose |
|--------|---------|
| `Az.Accounts` | Token acquisition, subscription context |
| `Az.Databricks` | Workspace ID resolution |
| `Az.Storage` | Storage Account discovery |
| `Az.KeyVault` | Key Vault discovery |
| `Az.Sql` | SQL Server discovery |
| `Az.DataFactory` | Data Factory discovery |
| `Az.CognitiveServices` | Cognitive Services / Azure OpenAI discovery |
| `Az.EventHub` | Event Hub namespace discovery |
| `Az.ServiceBus` | Service Bus namespace discovery |

`Az.Synapse` is used opportunistically — Synapse workspaces are skipped if the module is absent.

### Permissions

| Operation | Required permission |
|-----------|--------------------|
| Run scripts interactively | `Account Admin` in Databricks Accounts Console |
| Approve private endpoints | `Owner` or `Network Contributor` on target Azure resources |
| Auto-discover resources | `Reader` on the target resource group |
| Resolve UPN via Graph | `Directory.Read.All` (delegated or application) |

### Pipeline Requirements

- Azure DevOps service connections configured per environment (`azureConnection.*`).
- A service connection for the Databricks admin SP (`databricksAdminConnection.*`).
- A variable group named after `databricksAdminConnection` containing `databricksAccountId`.
- `.env` files in the repo root for each environment: `dev.env`, `tst.env`, `prd.env`.

## Quick Start

### Assign Databricks Account Admin

```powershell
# By UPN
.\scripts\New-DatabricksAccountAdmin.ps1 `
    -AccountID         '00000000-0000-0000-0000-000000000000' `
    -UserPrincipalName 'alice@contoso.com'

# By Entra ID object ID (service principal)
.\scripts\New-DatabricksAccountAdmin.ps1 `
    -AccountID         '00000000-0000-0000-0000-000000000000' `
    -PrincipalObjectId 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' `
    -PrincipalType     'ServicePrincipal'
```

### Deploy NCC (Auto-Discovery)

```powershell
.\pipeline-scripts\deployment\Deploy-DatabricksNCC.ps1 `
    -AccountID         '00000000-0000-0000-0000-000000000000' `
    -NccName           'ncc-dlz-prod-uksouth-01' `
    -AutoDiscover `
    -SubscriptionId    '00000000-0000-0000-0000-000000000000' `
    -ResourceGroupName 'rg-contoso-dap-dev-uks-01'
```

### Deploy NCC (Manual Resource List)

```powershell
$resources = @(
    @{ ResourceID = '/subscriptions/.../storageAccounts/stexample'; ResourceType = 'blob' },
    @{ ResourceID = '/subscriptions/.../storageAccounts/stexample'; ResourceType = 'dfs'  },
    @{ ResourceID = '/subscriptions/.../vaults/kv-example';         ResourceType = 'vault' }
)
.\pipeline-scripts\deployment\Deploy-DatabricksNCC.ps1 `
    -AccountID    '00000000-0000-0000-0000-000000000000' `
    -NccName      'ncc-dlz-prod-uksouth-01' `
    -WorkspaceID  '1234567890123456' `
    -ClientId     $env:CLIENT_ID `
    -ClientSecret $env:CLIENT_SECRET `
    -Resources    $resources
```

### Approve Pending Private Endpoints

```powershell
.\pipeline-scripts\deployment\Approve-DatabricksPrivateEndpoints.ps1 `
    -AutoDiscover `
    -SubscriptionId    '00000000-0000-0000-0000-000000000000' `
    -ResourceGroupName 'rg-contoso-dap-dev-uks-01'

# Narrow to PE connections from this NCC only
.\pipeline-scripts\deployment\Approve-DatabricksPrivateEndpoints.ps1 `
    -AutoDiscover `
    -SubscriptionId    '00000000-0000-0000-0000-000000000000' `
    -ResourceGroupName 'rg-contoso-dap-dev-uks-01' `
    -DescriptionFilter 'databricks'
```

## Design Notes

- **Idempotency:** NCC creation, workspace assignment, and PE rule registration all follow a create-or-reuse pattern — re-running against the same environment is safe.
- **Fail-fast preflight:** Scripts validate PowerShell version, Az module availability, Az login state, and required parameter presence before any API calls.
- **WhatIf / PR safety:** `Approve-DatabricksPrivateEndpoints.ps1` reads `$env:IS_PULL_REQUEST` to auto-enable WhatIf on pull request pipeline runs. Pass `-WhatIfEnabled $true` to activate manually.
- **NCC region constraint:** The NCC region must match the Databricks workspace region. Default is `uksouth`; override with `-Region`.
- **Az 12+ compatibility:** `Get-AzAccessToken` returns a `SecureString` in Az 12+. All token acquisition paths handle both `SecureString` and plain `string` returns.

## Security Considerations

- Store all client secrets as Azure DevOps secret variables or Key Vault-backed variable group entries.
- Never commit `.env` files containing secrets.
- Scope service principals to the minimum required permissions (see Permissions table above).
- Audit Databricks Account Admin assignments regularly — the role grants full account-level control.

## Troubleshooting

| Symptom | Likely cause | Resolution |
|---------|--------------|------------|
| `AccountID is required` | Neither `-AccountID` nor `$env:DATABRICKS_ACCOUNT_ID` is set | Supply `-AccountID` or export `DATABRICKS_ACCOUNT_ID` |
| `401 Unauthorized` from Databricks API | Token acquired for wrong resource, or SP not Account Admin | Verify `Get-AzContext`; confirm SP role in Accounts Console |
| Missing Az sub-module warnings in AutoDiscover | Module not installed | `Install-Module <module> -Scope CurrentUser`; resources of that type are skipped |
| No pending PE connections found | Azure-side propagation delay | Increase `approvalWaitSeconds`; connections typically appear within 2–5 minutes of rule creation |
| Graph lookup failure for UPN | Wrong tenant context or missing `Directory.Read.All` | Run `Get-AzContext` to verify tenant; check SP Graph API permissions |
| NCC region mismatch error | `-Region` does not match workspace region | Set `-Region` to the workspace's Azure region (e.g. `uksouth`, `westeurope`) |

## Additional Documentation

- Script reference: `scripts/README.md`
- Deployment script reference: `pipeline-scripts/deployment/README.md`
- Pipeline reference: `pipelines/README.md`
- Release history: `CHANGELOG.md`
