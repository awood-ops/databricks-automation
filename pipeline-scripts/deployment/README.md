# Deployment Scripts

Operational scripts for NCC deployment and private endpoint approval. Both scripts target the Databricks Accounts REST API (`api/2.0`) and the Azure Resource Manager API, and support client credentials (service principal) or Az session authentication.

All Databricks REST calls are routed through a shared `Invoke-DatabricksRest` helper with a 3-attempt retry and 5-second back-off.

---

## Deploy-DatabricksNCC.ps1

Creates or reuses a Databricks Network Connectivity Configuration (NCC), assigns it to a workspace, and registers managed private endpoint rules for each target resource.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `AccountID` | `string` | Yes (or `$env:DATABRICKS_ACCOUNT_ID`) | Databricks account UUID. |
| `NccName` | `string` | Yes | Display name for the NCC. Existing NCCs with this name are reused; the script validates the found NCC is in the correct region before proceeding. |
| `WorkspaceID` | `string` | Yes (Manual); optional (AutoDiscover) | Numeric Databricks workspace ID from the workspace URL (`adb-<WorkspaceID>.<random>.azuredatabricks.net`). Resolved automatically from the deployed workspace resource when `-AutoDiscover` is set. |
| `ClientId` | `string` | No (or `$env:CLIENT_ID`) | Entra ID application (client) ID for client credentials auth. When omitted, falls back to the current Az session. |
| `ClientSecret` | `string` | No (or `$env:CLIENT_SECRET`) | Client secret for the service principal. Required when `ClientId` is supplied. |
| `Region` | `string` | No | Azure region for NCC creation. Defaults to `uksouth`. Must match the Databricks workspace region. |
| `Resources` | `array` | Yes (Manual set) | Array of `@{ ResourceID = '<ARM resource ID>'; ResourceType = '<groupId>' }` hashtables. |
| `AutoDiscover` | `switch` | Yes (AutoDiscover set) | Queries the target resource group for PE-eligible resources and resolves `WorkspaceID` automatically. |
| `SubscriptionId` | `string` | Yes with AutoDiscover (or `$env:SUBSCRIPTION_ID`) | Azure subscription containing the workspace and PE targets. |
| `ResourceGroupName` | `string` | Yes with AutoDiscover (or `$env:RESOURCE_GROUP_NAME`) | Resource group containing the workspace and PE targets. |

### Authentication

- **Client credentials:** `clientId` + `clientSecret` → `POST https://accounts.azuredatabricks.net/oidc/accounts/{accountId}/v1/token` with `scope=all-apis`.
- **Az session fallback:** `Get-AzAccessToken -ResourceUrl 'https://accounts.azuredatabricks.net/'`; falls back to resource GUID `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d` for older Az module versions. Az 12+ `SecureString` return handled transparently.

### Execution Flow

1. **Preflight checks** — PowerShell version, Az module presence, Az login state, required parameters. Fails fast with a consolidated error list.
2. **Auto-discovery** (if `-AutoDiscover`) — sets subscription context, queries each supported resource type, and resolves `WorkspaceID` from the deployed Databricks workspace in the resource group.
3. **OAuth token** — acquired via client credentials or Az session (see above).
4. **NCC lifecycle** — `GET /api/2.0/accounts/{id}/network-connectivity-configs`, create if absent.
5. **Workspace assignment** — `PUT` or `PATCH` binding the NCC to the workspace.
6. **PE rule registration** — for each resource in `$Resources`, checks for an existing rule and creates it if absent.

### Auto-Discovery Resource Coverage

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

Missing Az sub-modules produce a warning and are skipped gracefully via `Get-Command` guards; they do not abort the run.

### Usage

```powershell
# Auto-discover all PE targets
.\Deploy-DatabricksNCC.ps1 `
    -AccountID         '00000000-0000-0000-0000-000000000000' `
    -NccName           'ncc-dlz-prod-uksouth-01' `
    -AutoDiscover `
    -SubscriptionId    '00000000-0000-0000-0000-000000000000' `
    -ResourceGroupName 'rg-contoso-dap-dev-uks-01'

# Manual resource list with service principal auth
$resources = @(
    @{ ResourceID = '/subscriptions/.../storageAccounts/stexample'; ResourceType = 'blob' },
    @{ ResourceID = '/subscriptions/.../storageAccounts/stexample'; ResourceType = 'dfs'  },
    @{ ResourceID = '/subscriptions/.../vaults/kv-example';         ResourceType = 'vault' }
)
.\Deploy-DatabricksNCC.ps1 `
    -AccountID    '00000000-0000-0000-0000-000000000000' `
    -NccName      'ncc-dlz-prod-uksouth-01' `
    -WorkspaceID  '1234567890123456' `
    -ClientId     $env:CLIENT_ID `
    -ClientSecret $env:CLIENT_SECRET `
    -Resources    $resources
```

---

## Approve-DatabricksPrivateEndpoints.ps1

Approves pending private endpoint connections on Azure resources after NCC-managed rule registration. Supports the same manual / auto-discovery parameter sets as `Deploy-DatabricksNCC.ps1`.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `Resources` | `array` | Yes (Manual set) | Array of `@{ ResourceID = '<ARM resource ID>'; ResourceType = '<groupId>' }` hashtables. Same format as `Deploy-DatabricksNCC.ps1`. |
| `AutoDiscover` | `switch` | Yes (AutoDiscover set) | Queries the target resource group for PE-eligible resources. |
| `SubscriptionId` | `string` | Yes with AutoDiscover (or `$env:SUBSCRIPTION_ID`) | Azure subscription containing PE target resources. |
| `ResourceGroupName` | `string` | Yes with AutoDiscover (or `$env:RESOURCE_GROUP_NAME`) | Resource group containing PE target resources. |
| `DescriptionFilter` | `string` | No | Regex applied to the pending PE connection **name**. Use to narrow approvals when a resource hosts PE connections from multiple systems (e.g. `'databricks'`). |
| `WhatIfEnabled` | `bool` | No | When `$true`, reports pending connections without approving. Defaults to `$env:IS_PULL_REQUEST` — automatically `$true` on PR pipeline runs. |

### Execution Flow

1. **Auto-discovery** (if `-AutoDiscover`) — same resource group query as `Deploy-DatabricksNCC.ps1`.
2. **Per-resource PE connection query** — uses the appropriate Az cmdlet for each resource type to list pending private endpoint connections.
3. **Name filter** — if `-DescriptionFilter` is set, connections whose **name** does not match the regex are skipped.
4. **Approval** — calls the resource-type-specific Az approval cmdlet for each matching connection, or prints a WhatIf message if `WhatIfEnabled` is `$true`.

### Usage

```powershell
# Approve all pending PE connections in a resource group
.\Approve-DatabricksPrivateEndpoints.ps1 `
    -AutoDiscover `
    -SubscriptionId    '00000000-0000-0000-0000-000000000000' `
    -ResourceGroupName 'rg-contoso-dap-dev-uks-01'

# Narrow to connections from Databricks only
.\Approve-DatabricksPrivateEndpoints.ps1 `
    -AutoDiscover `
    -SubscriptionId    '00000000-0000-0000-0000-000000000000' `
    -ResourceGroupName 'rg-contoso-dap-dev-uks-01' `
    -DescriptionFilter 'databricks'

# Preview without approving
.\Approve-DatabricksPrivateEndpoints.ps1 `
    -AutoDiscover `
    -SubscriptionId    '00000000-0000-0000-0000-000000000000' `
    -ResourceGroupName 'rg-contoso-dap-dev-uks-01' `
    -WhatIfEnabled     $true
```

### Notes

- Run `Deploy-DatabricksNCC.ps1` first. Azure typically takes 2–5 minutes to surface pending PE connections after rule registration.
- The `IS_PULL_REQUEST` environment variable is read at startup — set it to any truthy value to force WhatIf without passing `-WhatIfEnabled`.
- Requires `Owner` or `Network Contributor` on each target resource to approve PE connections.
