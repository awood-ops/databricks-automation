<#
.SYNOPSIS
    Creates a Databricks Network Connectivity Configuration (NCC), assigns it to a workspace,
    and registers managed private endpoint rules for the specified Azure resources.

.DESCRIPTION
    This script uses the Databricks Accounts REST API to:
      1. Obtain an OAuth2 token using a service principal (client credentials flow).
      2. Ensure a Network Connectivity Configuration (NCC) exists, creating it if absent.
      3. Assign the NCC to the target Databricks workspace.
      4. For each supplied Azure resource, ensure a private endpoint rule exists within the NCC,
         creating it if absent.

    The service principal must be a Databricks Account Admin.
    A variable group (or environment variables) named after the service connection can expose
    'CLIENT_ID' and 'CLIENT_SECRET'. If these are omitted, the script can use the token from
    the current Az/Azure CLI session instead.

    When -AutoDiscover is specified the script queries Azure for all Storage Accounts
    (blob, dfs), Key Vaults (vault), SQL Servers (sqlServer), Data Factories (dataFactory),
    Cognitive Services / Azure OpenAI (account), Event Hub namespaces (namespace), Service
    Bus namespaces (namespace), and Synapse workspaces (Sql, SqlOnDemand, Dev) in the target
    resource group and builds the Resources list automatically. It also resolves the Databricks
    WorkspaceID from the deployed workspace resource, so -WorkspaceID can be omitted when
    using -AutoDiscover.

.PARAMETER AccountID
    The Databricks account UUID, visible in the Accounts Console:
    https://accounts.azuredatabricks.net — top-right profile menu.
    Required. The Databricks Accounts API has no endpoint to list accounts, so this
    value cannot be discovered automatically.
    In pipelines, store it as the 'databricksAccountId' variable group secret.

.PARAMETER NccName
    The display name for the Network Connectivity Configuration.
    If an NCC with this name already exists it will be reused.

.PARAMETER WorkspaceID
    The numeric Databricks workspace ID, taken from the workspace URL:
    adb-<WorkspaceID>.<random>.azuredatabricks.net
    Optional when -AutoDiscover is specified — the workspace ID will be resolved from the
    deployed Databricks workspace resource in the supplied resource group.

.PARAMETER AutoDiscover
    When set, queries Azure (using the current Az context) for deployable PE targets in the
    resource group and resolves the Databricks WorkspaceID automatically. Mutually exclusive
    with supplying -Resources and -WorkspaceID manually.

.PARAMETER SubscriptionId
    The Azure subscription ID that contains the Databricks workspace and PE target resources.
    Required when -AutoDiscover is used. Falls back to the SUBSCRIPTION_ID environment variable.

.PARAMETER ResourceGroupName
    The resource group containing the Databricks workspace and PE target resources.
    Required when -AutoDiscover is used. Falls back to the RESOURCE_GROUP_NAME environment variable.

.PARAMETER ClientId
    The Entra ID application (client) ID of the service principal used to authenticate.
    Optional — when omitted the script uses the token from the current Az/Azure CLI session
    (Connect-AzAccount is sufficient for interactive use).

.PARAMETER ClientSecret
    The client secret of the service principal.
    Optional — only required when ClientId is supplied.

.PARAMETER Region
    The Azure region in which the NCC should be created. Defaults to 'uksouth'.

.PARAMETER Resources
    An array of hashtables, each describing one private endpoint rule to create:
      @{ ResourceID = '<ARM resource ID>'; ResourceType = '<sub-resource / group ID>' }
    Example sub-resource values: 'blob', 'dfs', 'vault', 'sql', 'sqlServer'.

.EXAMPLE
    $resources = @(
        @{ ResourceID = '/subscriptions/00000000.../resourceGroups/rg-dlz/providers/Microsoft.Storage/storageAccounts/stexample'; ResourceType = 'blob' },
        @{ ResourceID = '/subscriptions/00000000.../resourceGroups/rg-dlz/providers/Microsoft.Storage/storageAccounts/stexample'; ResourceType = 'dfs'  }
    )
    .\Deploy-DatabricksNCC.ps1 `
        -AccountID  '00000000-0000-0000-0000-000000000000' `
        -NccName    'ncc-dlz-prod-uksouth-01' `
        -WorkspaceID '1234567890123456' `
        -ClientId   $env:CLIENT_ID `
        -ClientSecret $env:CLIENT_SECRET `
        -Resources  $resources
.EXAMPLE
    # Auto-discover all PE targets from an already-deployed resource group
    .\.Deploy-DatabricksNCC.ps1 `
        -AccountID        '00000000-0000-0000-0000-000000000000' `
        -NccName          'ncc-dlz-prod-uksouth-01' `
        -AutoDiscover `
        -SubscriptionId   '00000000-0000-0000-0000-000000000000' `
        -ResourceGroupName 'rg-contoso-dap-dev-uks-01' `
        -ClientId         $env:CLIENT_ID `
        -ClientSecret     $env:CLIENT_SECRET
.NOTES
    - Requires PowerShell 7+ or Windows PowerShell 5.1 with Invoke-RestMethod
    - Requires Az.Databricks, Az.Storage, Az.KeyVault, Az.Sql, Az.DataFactory,
      Az.CognitiveServices, Az.EventHub, Az.ServiceBus, Az.Synapse modules (missing
      modules are skipped gracefully during auto-discovery)
    - The service principal must hold the Account Admin role in the Databricks Accounts Console
    - Az 12+ returns Get-AzAccessToken tokens as SecureString — handled automatically
    - After this script runs, pending private endpoint connections on the target Azure resources
      must be approved — use Approve-DatabricksPrivateEndpoints.ps1 for that step
    - NCC region must match the Databricks workspace region
#>
[CmdletBinding(SupportsShouldProcess, DefaultParameterSetName = 'Manual')]
param (
    # Optional when supplied via -AccountID or the DATABRICKS_ACCOUNT_ID environment variable
    [Parameter()]
    [string]$AccountID = "$($env:DATABRICKS_ACCOUNT_ID)",

    [Parameter(Mandatory)]
    [string]$NccName,

    # Optional when -AutoDiscover is used — resolved from the deployed workspace resource
    [Parameter(ParameterSetName = 'Manual', Mandatory)]
    [Parameter(ParameterSetName = 'AutoDiscover')]
    [string]$WorkspaceID,

    [Parameter()]
    [string]$ClientId = "$($env:CLIENT_ID)",

    [Parameter()]
    [string]$ClientSecret = "$($env:CLIENT_SECRET)",

    [Parameter()]
    [string]$Region = 'uksouth',

    # ── Manual mode ──────────────────────────────────────────────────────────
    [Parameter(ParameterSetName = 'Manual')]
    [array]$Resources = @(),

    # ── Auto-discover mode ───────────────────────────────────────────────────
    [Parameter(ParameterSetName = 'AutoDiscover', Mandatory)]
    [switch]$AutoDiscover,

    [Parameter(ParameterSetName = 'AutoDiscover')]
    [string]$SubscriptionId = "$($env:SUBSCRIPTION_ID)",

    [Parameter(ParameterSetName = 'AutoDiscover')]
    [string]$ResourceGroupName = "$($env:RESOURCE_GROUP_NAME)"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

#region ── Pre-flight checks ─────────────────────────────────────────────────

Write-Host 'Running pre-flight checks...' -ForegroundColor Cyan
$preflightErrors = [System.Collections.Generic.List[string]]::new()

# ── PowerShell version ────────────────────────────────────────────────────────
if ($PSVersionTable.PSVersion.Major -lt 5 -or
    ($PSVersionTable.PSVersion.Major -eq 5 -and $PSVersionTable.PSVersion.Minor -lt 1)) {
    $preflightErrors.Add("PowerShell 5.1 or 7+ is required. Current version: $($PSVersionTable.PSVersion)")
} else {
    Write-Host "  [OK] PowerShell $($PSVersionTable.PSVersion)" -ForegroundColor Green
}

# ── Az module installed ───────────────────────────────────────────────────────
$azModule = Get-Module -Name Az -ListAvailable | Select-Object -First 1
if (-not $azModule) {
    $preflightErrors.Add("The 'Az' module is not installed. Run: Install-Module Az -Scope CurrentUser")
} else {
    Write-Host "  [OK] Az module v$($azModule.Version)" -ForegroundColor Green
}

# ── Az sub-modules required by AutoDiscover ───────────────────────────────────
if ($AutoDiscover) {
    $requiredModules = @(
        'Az.Accounts',
        'Az.Databricks',
        'Az.Storage',
        'Az.KeyVault',
        'Az.Sql',
        'Az.DataFactory',
        'Az.CognitiveServices',
        'Az.EventHub',
        'Az.ServiceBus'
    )
    foreach ($mod in $requiredModules) {
        $found = Get-Module -Name $mod -ListAvailable | Select-Object -First 1
        if (-not $found) {
            # Warn but don't block — script skips missing modules gracefully
            Write-Host "  [WARN] Module '$mod' not found — related resources will be skipped during auto-discovery." -ForegroundColor Yellow
        } else {
            Write-Host "  [OK] $mod v$($found.Version)" -ForegroundColor Green
        }
    }
}

# ── Az login check ────────────────────────────────────────────────────────────
if ($azModule) {
    try {
        $azContext = Get-AzContext -ErrorAction Stop
        if ($null -eq $azContext -or $null -eq $azContext.Account) {
            $preflightErrors.Add("Not logged in to Azure. Run: Connect-AzAccount")
        } else {
            Write-Host "  [OK] Logged in as '$($azContext.Account.Id)' (subscription: $($azContext.Subscription.Name))" -ForegroundColor Green
        }
    } catch {
        $preflightErrors.Add("Could not determine Az login state: $_")
    }
}

# ── AccountID present ─────────────────────────────────────────────────────────
if (-not $AccountID) {
    $preflightErrors.Add("No -AccountID supplied and `$env:DATABRICKS_ACCOUNT_ID is not set.")
}

# ── AutoDiscover: required params ─────────────────────────────────────────────
if ($AutoDiscover) {
    if (-not $SubscriptionId) {
        $preflightErrors.Add("No -SubscriptionId supplied and `$env:SUBSCRIPTION_ID is not set.")
    }
    if (-not $ResourceGroupName) {
        $preflightErrors.Add("No -ResourceGroupName supplied and `$env:RESOURCE_GROUP_NAME is not set.")
    }
}

# ── Fail fast if any hard errors ──────────────────────────────────────────────
if ($preflightErrors.Count -gt 0) {
    Write-Host "`nPre-flight checks FAILED:" -ForegroundColor Red
    foreach ($err in $preflightErrors) {
        Write-Host "  [ERROR] $err" -ForegroundColor Red
    }
    throw "Pre-flight checks failed. Resolve the issues above and re-run."
}

Write-Host "Pre-flight checks passed.`n" -ForegroundColor Green

#endregion

#region ── Auto-discover: query Azure for PE targets and workspace ID ─────────

if ($AutoDiscover) {
    Write-Host 'Auto-discover mode: querying Azure for PE-enabled resources...' -ForegroundColor Cyan

    $null = Set-AzContext -SubscriptionId $SubscriptionId

    $discovered = [System.Collections.Generic.List[hashtable]]::new()

    # ── Storage Accounts → blob + dfs ────────────────────────────────────────
    if (Get-Command -Name 'Get-AzStorageAccount' -ErrorAction SilentlyContinue) {
        $storageAccounts = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        foreach ($sa in $storageAccounts) {
            Write-Host "  Storage: $($sa.StorageAccountName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $sa.Id; ResourceType = 'blob' })
            $discovered.Add(@{ ResourceID = $sa.Id; ResourceType = 'dfs'  })
        }
    } else { Write-Warning "Az.Storage module not available — Storage Accounts will be skipped." }

    # ── Key Vaults → vault ───────────────────────────────────────────────────
    if (Get-Command -Name 'Get-AzKeyVault' -ErrorAction SilentlyContinue) {
        $keyVaults = Get-AzKeyVault -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        foreach ($kv in $keyVaults) {
            Write-Host "  Key Vault: $($kv.VaultName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $kv.ResourceId; ResourceType = 'vault' })
        }
    } else { Write-Warning "Az.KeyVault module not available — Key Vaults will be skipped." }

    # ── SQL Servers → sqlServer ──────────────────────────────────────────────
    if (Get-Command -Name 'Get-AzSqlServer' -ErrorAction SilentlyContinue) {
        $sqlServers = Get-AzSqlServer -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        foreach ($sql in $sqlServers) {
            Write-Host "  SQL Server: $($sql.ServerName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $sql.ResourceId; ResourceType = 'sqlServer' })
        }
    } else { Write-Warning "Az.Sql module not available — SQL Servers will be skipped." }

    # ── Data Factories → dataFactory ─────────────────────────────────────────
    if (Get-Command -Name 'Get-AzDataFactoryV2' -ErrorAction SilentlyContinue) {
        $dataFactories = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        foreach ($adf in $dataFactories) {
            Write-Host "  Data Factory: $($adf.DataFactoryName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $adf.DataFactoryId; ResourceType = 'dataFactory' })
        }
    } else { Write-Warning "Az.DataFactory module not available — Data Factories will be skipped." }

    # ── Cognitive Services / Azure OpenAI → account ───────────────────────────
    if (Get-Command -Name 'Get-AzCognitiveServicesAccount' -ErrorAction SilentlyContinue) {
        $cogAccounts = Get-AzCognitiveServicesAccount -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        foreach ($cog in $cogAccounts) {
            $cogKind    = try { $cog.Kind        } catch { try { $cog.AccountType } catch { 'Unknown' } }
            $cogName    = try { $cog.AccountName } catch { try { $cog.Name        } catch { $cog.Id   } }
            $label      = if ($cogKind -eq 'OpenAI') { "Azure OpenAI" } else { "Cognitive Services ($cogKind)" }
            Write-Host "  $label`: $cogName" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $cog.Id; ResourceType = 'account' })
        }
    } else { Write-Warning "Az.CognitiveServices module not available — Cognitive Services will be skipped." }

    # ── Event Hub namespaces → namespace ──────────────────────────────────────
    if (Get-Command -Name 'Get-AzEventHubNamespace' -ErrorAction SilentlyContinue) {
        $eventHubs = Get-AzEventHubNamespace -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        foreach ($eh in $eventHubs) {
            Write-Host "  Event Hub: $($eh.Name)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $eh.Id; ResourceType = 'namespace' })
        }
    } else { Write-Warning "Az.EventHub module not available — Event Hub namespaces will be skipped." }

    # ── Service Bus namespaces → namespace ────────────────────────────────────
    if (Get-Command -Name 'Get-AzServiceBusNamespace' -ErrorAction SilentlyContinue) {
        $serviceBusNs = Get-AzServiceBusNamespace -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        foreach ($sb in $serviceBusNs) {
            Write-Host "  Service Bus: $($sb.Name)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $sb.Id; ResourceType = 'namespace' })
        }
    } else { Write-Warning "Az.ServiceBus module not available — Service Bus namespaces will be skipped." }

    # ── Synapse workspaces → Sql, SqlOnDemand, Dev ────────────────────────────
    if (Get-Command -Name 'Get-AzSynapseWorkspace' -ErrorAction SilentlyContinue) {
        $synapseWorkspaces = Get-AzSynapseWorkspace -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        foreach ($syn in $synapseWorkspaces) {
            Write-Host "  Synapse: $($syn.Name)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $syn.Id; ResourceType = 'Sql'          })
            $discovered.Add(@{ ResourceID = $syn.Id; ResourceType = 'SqlOnDemand'  })
            $discovered.Add(@{ ResourceID = $syn.Id; ResourceType = 'Dev'          })
        }
    } else { Write-Warning "Az.Synapse module not available — Synapse workspaces will be skipped." }

    $Resources = $discovered.ToArray()
    Write-Host "  Total PE rules to register: $($Resources.Count)" -ForegroundColor Green

    # ── Resolve Databricks workspace ID if not supplied ───────────────────────
    if (-not $WorkspaceID) {
        Write-Host '  Resolving Databricks workspace ID from deployed workspace...' -ForegroundColor Gray
        if (-not (Get-Command -Name 'Get-AzDatabricksWorkspace' -ErrorAction SilentlyContinue)) {
            throw "Az.Databricks module is required to auto-resolve WorkspaceID. Install it or supply -WorkspaceID explicitly."
        }
        $dbwResource = Get-AzDatabricksWorkspace -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($null -eq $dbwResource) {
            throw "No Databricks workspace found in resource group '$ResourceGroupName'. Supply -WorkspaceID explicitly."
        }
        # WorkspaceId is the numeric ID (e.g. 7405618989272934)
        $WorkspaceID = $dbwResource.WorkspaceId
        Write-Host "  Auto-discovered WorkspaceID : $WorkspaceID" -ForegroundColor Green
        Write-Host "  Workspace URL               : $($dbwResource.Url)" -ForegroundColor Green
    }
}

#endregion

#region ── Helper: Invoke Databricks REST with retry ──────────────────────────

function Invoke-DatabricksRest {
    param (
        [string]$Method,
        [string]$Uri,
        [hashtable]$Headers,
        [string]$Body,
        [int]$MaxRetries = 3
    )

    $attempt = 0
    do {
        $attempt++
        try {
            $params = @{
                Method      = $Method
                Uri         = $Uri
                Headers     = $Headers
                ContentType = 'application/json'
            }
            if ($Body) { $params['Body'] = $Body }
            return Invoke-RestMethod @params
        }
        catch {
            if ($attempt -ge $MaxRetries) { throw }
            Write-Warning "Request failed (attempt $attempt / $MaxRetries): $_  Retrying in 5 s..."
            Start-Sleep -Seconds 5
        }
    } while ($attempt -lt $MaxRetries)
}

#endregion

#region ── Step 1: Obtain Databricks OAuth token ──────────────────────────────

Write-Host "Step 1: Obtaining Databricks OAuth token..." -ForegroundColor Cyan

if ($ClientId -and $ClientSecret) {
    # ── Service principal with client secret (pipeline / local testing) ────────────
    Write-Host '  Using client credentials (service principal).' -ForegroundColor Gray
    $tokenBody = @{
        grant_type    = 'client_credentials'
        client_id     = $ClientId
        client_secret = $ClientSecret
        scope         = 'all-apis'
    }
    $tokenResponse = Invoke-RestMethod `
        -Method      Post `
        -Uri         "https://accounts.azuredatabricks.net/oidc/accounts/$AccountID/v1/token" `
        -ContentType 'application/x-www-form-urlencoded' `
        -Body        $tokenBody
    $dbToken = $tokenResponse.access_token
}
else {
    # ── Logged-in user / managed identity (interactive / local testing) ───────
    # Resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d is the Azure Databricks
    # first-party app — valid audience for both workspace and account APIs.
    Write-Host '  No ClientId/ClientSecret supplied — using current Az session token.' -ForegroundColor Gray
    try {
        $rawToken = (Get-AzAccessToken -ResourceUrl 'https://accounts.azuredatabricks.net/' -ErrorAction Stop).Token
    }
    catch {
        # Fallback: older Az module versions use -Resource instead of -ResourceUrl
        $rawToken = (Get-AzAccessToken -Resource '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d' -ErrorAction Stop).Token
    }
    # Az 12+ returns Token as SecureString — convert to plain text if needed
    $dbToken = if ($rawToken -is [System.Security.SecureString]) {
        [System.Net.NetworkCredential]::new('', $rawToken).Password
    } else {
        $rawToken
    }
}

Write-Host "Databricks OAuth token obtained successfully." -ForegroundColor Green

$headers = @{ Authorization = "Bearer $dbToken" }

#endregion

#region ── Step 1b: Resolve Account ID if not supplied ────────────────────

if (-not $AccountID) {
    throw @"
-AccountID is required. The Databricks Accounts API has no 'list accounts' endpoint,
so the account ID cannot be discovered automatically.

To find it:
  1. Go to https://accounts.azuredatabricks.net
  2. Click your profile icon (top-right)
  3. Copy the account ID (UUID format)

Then re-run with:  -AccountID '<your-account-uuid>'

In pipelines, store it as the 'databricksAccountId' variable group secret.
"@
}

#endregion

#region ── Step 2: Ensure NCC exists ─────────────────────────────────────────

Write-Host "`nStep 2: Checking for existing NCC '$NccName'..." -ForegroundColor Cyan

$nccListResponse = Invoke-DatabricksRest `
    -Method  'Get' `
    -Uri     "https://accounts.azuredatabricks.net/api/2.0/accounts/$AccountID/network-connectivity-configs" `
    -Headers $headers
$existingNCCs = if ($nccListResponse.PSObject.Properties['items']) { @($nccListResponse.PSObject.Properties['items'].Value) } else { @() }

$matchedNCC = $existingNCCs | Where-Object { $_.name -eq $NccName } | Select-Object -First 1
$nccID = if ($matchedNCC) { $matchedNCC.network_connectivity_config_id } else { $null }

if ($null -eq $nccID) {
    Write-Host "NCC '$NccName' not found — creating in region '$Region'..." -ForegroundColor Yellow

    if ($PSCmdlet.ShouldProcess("Databricks account $AccountID", "Create NCC '$NccName'")) {
        $nccBody = @{ name = $NccName; region = $Region } | ConvertTo-Json
        $newNCC  = Invoke-DatabricksRest `
            -Method  'Post' `
            -Uri     "https://accounts.azuredatabricks.net/api/2.0/accounts/$AccountID/network-connectivity-configs" `
            -Headers $headers `
            -Body    $nccBody

        $nccID = $newNCC.network_connectivity_config_id
        Write-Host "NCC '$NccName' created. ID: $nccID" -ForegroundColor Green
    }
}
else {
    Write-Host "NCC '$NccName' already exists. ID: $nccID" -ForegroundColor Green
}

#endregion

#region ── Step 3: Assign NCC to workspace ───────────────────────────────────

Write-Host "`nStep 3: Assigning NCC to workspace $WorkspaceID..." -ForegroundColor Cyan

if ($PSCmdlet.ShouldProcess("Workspace $WorkspaceID", "Assign NCC '$NccName' ($nccID)")) {
    $workspaceBody = @{ network_connectivity_config_id = $nccID } | ConvertTo-Json

    $null = Invoke-DatabricksRest `
        -Method  'Patch' `
        -Uri     "https://accounts.azuredatabricks.net/api/2.0/accounts/$AccountID/workspaces/$WorkspaceID" `
        -Headers $headers `
        -Body    $workspaceBody

    Write-Host "NCC assigned to workspace $WorkspaceID." -ForegroundColor Green
}

#endregion

#region ── Step 4: Ensure private endpoint rules exist ───────────────────────

if ($Resources.Count -eq 0) {
    Write-Host "`nNo resources supplied — skipping private endpoint rule creation." -ForegroundColor Yellow
}
else {
    Write-Host "`nStep 4: Processing $($Resources.Count) private endpoint rule(s)..." -ForegroundColor Cyan

    # Retrieve current PE rules once to avoid repeated GET calls
    $peRulesResponse = Invoke-DatabricksRest `
        -Method  'Get' `
        -Uri     "https://accounts.azuredatabricks.net/api/2.0/accounts/$AccountID/network-connectivity-configs/$nccID/private-endpoint-rules" `
        -Headers $headers
    $existingRules = [System.Collections.ArrayList]::new()
    if ($peRulesResponse.PSObject.Properties['items']) {
        foreach ($r in @($peRulesResponse.PSObject.Properties['items'].Value)) {
            $null = $existingRules.Add($r)
        }
    }

    foreach ($resource in $Resources) {
        $resourceID        = $resource.ResourceID
        $resourceType      = $resource.ResourceType
        $resourceShortName = $resourceID.Split('/')[-1]
        $label             = "$resourceShortName ($resourceType)"

        Write-Host "  Checking PE rule: $label" -ForegroundColor Cyan

        $existingRule = $existingRules | Where-Object {
            $_.resource_id -eq $resourceID -and $_.group_id -eq $resourceType
        } | Select-Object -First 1

        if ($null -ne $existingRule) {
            Write-Host "  PE rule already exists (rule_id: $($existingRule.rule_id)) — skipping." -ForegroundColor Green
            continue
        }

        Write-Host "  PE rule not found — creating..." -ForegroundColor Yellow

        if ($PSCmdlet.ShouldProcess($resourceID, "Create PE rule for group_id '$resourceType'")) {
            $peBody = @{
                group_id    = $resourceType
                resource_id = $resourceID
            } | ConvertTo-Json

            $newRule = Invoke-DatabricksRest `
                -Method  'Post' `
                -Uri     "https://accounts.azuredatabricks.net/api/2.0/accounts/$AccountID/network-connectivity-configs/$nccID/private-endpoint-rules" `
                -Headers $headers `
                -Body    $peBody

            Write-Host "  PE rule created (rule_id: $($newRule.rule_id))." -ForegroundColor Green

            # Append to local list so duplicate detection works within the same run
            $null = $existingRules.Add($newRule)
        }
    }
}

#endregion

Write-Host "`nDeploy-DatabricksNCC complete." -ForegroundColor Cyan
Write-Host "NCC ID : $nccID" -ForegroundColor Cyan
Write-Host "Next step: run Approve-DatabricksPrivateEndpoints.ps1 to approve the pending connections on each Azure resource." -ForegroundColor Yellow
