# Pipelines

Azure DevOps pipeline definitions for end-to-end Databricks NCC deployment.

---

## Deploy-Databricks-NCC.yaml

Two-stage pipeline that deploys an NCC to a Databricks workspace and approves the resulting private endpoint connections on the target Azure resources. Trigger is `none` — run on-demand after your data platform infrastructure deployment.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `env` | `string` (`dev`, `tst`, `prd`) | `dev` | Target environment. Controls which `azureConnection` and `databricksAdminConnection` values are used. |
| `nccName` | `string` | _(empty)_ | NCC display name. When blank, derived from naming convention: `ncc-{companyCode}-{workloadCode}-{environmentCode}-{locationShort}-01`. |
| `approvalWaitSeconds` | `number` | `300` | Seconds to wait between Stage 1 completion and PE approval. Azure typically surfaces pending connections within 2–5 minutes of rule registration. |
| `sharedEnvFile` | `string` | `config/shared.env` | Repo-relative path to the shared environment variable file. |
| `envFile` | `string` | `config/{env}/.env` | Repo-relative path to the environment-specific variable file. |

### Variable Requirements

Two sets of variables must be configured before first run:

**Pipeline variables (set in `variables:` block):**

| Variable | Description |
|----------|-------------|
| `azureConnection` | Azure DevOps service connection name for the target environment. Set one value per `env` in the `${{ if }}` block. |
| `databricksAdminConnection` | Azure DevOps service connection for the Databricks admin SP. Must match the name of the ADO variable group that contains `databricksAccountId`. |

**Variable group** (named after `databricksAdminConnection`):

| Variable | Description |
|----------|-------------|
| `databricksAccountId` | Databricks account UUID. Store as a secret. |

**`.env` files** (consumed by the Import Environment Variables step):

| Variable | Required in | Description |
|----------|-------------|-------------|
| `LOCATION` | Both stages | Azure region string (e.g. `uksouth`). Used to derive `locationShort` and passed as `-Region`. |
| `COMPANY_CODE` | Both stages | Short code used in naming convention (e.g. `contoso`). |
| `WORKLOAD_CODE` | Both stages | Workload identifier (e.g. `dap`). |
| `ENVIRONMENT_CODE` | Both stages | Environment shortcode (e.g. `dev`, `tst`, `prd`). |
| `SUBSCRIPTION_ID` | Both stages | Azure subscription ID. |

`.env` files use `KEY=VALUE` format. Double-quotes are stripped by the sanitise step. Lines beginning with `#` are ignored.

**Example `config/shared.env`:**

```env
# Shared variables across all environments
COMPANY_CODE=contoso
WORKLOAD_CODE=dap
```

**Example `config/dev/.env`:**

```env
# Environment-specific variables for dev
LOCATION=uksouth
ENVIRONMENT_CODE=dev
SUBSCRIPTION_ID=00000000-0000-0000-0000-000000000000
```

> **Note:** The `LOCATION` value must match the Azure region of your Databricks workspace exactly. This value is also used as the NCC region — a mismatch will cause a hard API error. See [Design Notes](../README.md#design-notes) in the root README.

### Stage Detail

#### Stage 1 — `Deploy_NCC`

| Step | Task | Description |
|------|------|-------------|
| Sanitise env files | `pwsh` | Strips surrounding double-quotes from `.env` values to prevent variable injection issues. |
| Import env variables | `pwsh` | Parses `shared.env` then the environment-specific `.env`; publishes each `KEY=VALUE` pair as a pipeline variable via `##vso[task.setvariable]`. |
| Validate required variables | `pwsh` | Asserts `LOCATION`, `COMPANY_CODE`, `WORKLOAD_CODE`, `ENVIRONMENT_CODE`, `SUBSCRIPTION_ID`, `databricksAccountId` are all non-empty. Fails fast if any are missing. |
| Resolve resource group name | `AzurePowerShell@5` | Derives `RESOURCE_GROUP_NAME` from naming convention. `northeurope` maps to `eun`; all other regions use the first three characters of the region string. Derives `NCC_NAME` from `nccName` parameter or the same convention. |
| Deploy NCC and PE rules | `AzurePowerShell@5` | Invokes `Deploy-DatabricksNCC.ps1` with `-AutoDiscover`, passing `databricksAccountId`, resolved NCC name, `LOCATION`, `SUBSCRIPTION_ID`, and `RESOURCE_GROUP_NAME`. |

#### Stage 2 — `Approve_PEs` (depends on `Deploy_NCC`)

| Step | Task | Description |
|------|------|-------------|
| Sanitise / import env variables | `pwsh` | Repeated from Stage 1 — each Azure DevOps job starts with a clean environment. |
| Validate required variables | `pwsh` | Same check as Stage 1 (without `databricksAccountId`). |
| Resolve resource group name | `AzurePowerShell@5` | Re-derives `RESOURCE_GROUP_NAME` using the same convention. |
| Wait for PE connections | `pwsh` | `Start-Sleep -Seconds approvalWaitSeconds`. Increase this if PE connections are consistently not found. |
| Approve PE connections | `AzurePowerShell@5` | Invokes `Approve-DatabricksPrivateEndpoints.ps1` with `-AutoDiscover` and `-WhatIfEnabled $false`. No `-DescriptionFilter` is applied — auto-discovery already scopes to the correct resource group; NCC PE connection names do not consistently include `databricks`. |

### Naming Convention

Resource group and NCC names are derived using this pattern:

```
rg-{COMPANY_CODE}-{WORKLOAD_CODE}-{ENVIRONMENT_CODE}-{locationShort}-01
ncc-{COMPANY_CODE}-{WORKLOAD_CODE}-{ENVIRONMENT_CODE}-{locationShort}-01
```

`locationShort` mapping:
- `northeurope` → `eun` (special case: Azure's `northeurope` abbreviation is `ne`, which conflicts with common naming conventions — `eun` avoids the collision and is used by convention in this project)
- All other regions → first 3 characters (e.g. `uksouth` → `uks`, `westeurope` → `wes`)

### First-Run Checklist

1. Set `azureConnection` values per environment in the `variables:` block.
2. Set `databricksAdminConnection` values per environment in the `variables:` block.
3. Create an ADO variable group with the same name as `databricksAdminConnection`.
4. Add `databricksAccountId` (as a secret) to that variable group.
5. Create `config/shared.env` and `config/{env}/.env` files with the required variables listed above.
6. Ensure the Databricks admin SP holds `Account Admin` in the Databricks Accounts Console.
7. Ensure the Azure service connection SP has `Reader` on the resource group and `Owner` or `Network Contributor` on PE target resources.
