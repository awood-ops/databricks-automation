<#
.SYNOPSIS
    Approves pending private endpoint connections on Azure resources that were created
    by a Databricks Network Connectivity Configuration (NCC).

.DESCRIPTION
    When a Databricks NCC creates managed private endpoint rules, Azure places a corresponding
    pending private endpoint connection on each target resource. These connections must be
    explicitly approved before traffic can flow.

    This script:
      1. Parses each supplied ARM resource ID to determine the subscription, resource group,
         resource type, and resource name.
      2. Calls Get-AzPrivateEndpointConnection to list all private endpoint connections on
         that resource.
      3. Filters for connections in the 'Pending' state whose **name** matches the optional
         -DescriptionFilter pattern (useful when multiple systems create private endpoints
         against the same resource).
      4. Approves each matching connection using Approve-AzPrivateEndpointConnection.

    Run this script after Deploy-DatabricksNCC.ps1. Allow a few minutes after the NCC rules
    are created for the pending connections to appear on the Azure resources.

    When -AutoDiscover is specified the script queries the supplied resource group for all
    Storage Accounts (blob, dfs), Key Vaults (vault), SQL Servers (sqlServer), Data Factories
    (dataFactory), Cognitive Services / Azure OpenAI (account), Event Hub namespaces
    (namespace), Service Bus namespaces (namespace), and Synapse workspaces (Sql, SqlOnDemand,
    Dev) and builds the resource list automatically, mirroring the discovery behaviour in
    Deploy-DatabricksNCC.ps1.

.PARAMETER Resources
    An array of hashtables describing the resources whose pending PE connections should be
    approved. Each entry must have a 'ResourceID' key containing the full ARM resource ID.
    The 'ResourceType' key is accepted but not required for approval.

    Example:
      @(
          @{ ResourceID = '/subscriptions/.../providers/Microsoft.Storage/storageAccounts/stexample'; ResourceType = 'blob' },
          @{ ResourceID = '/subscriptions/.../providers/Microsoft.KeyVault/vaults/kv-example';        ResourceType = 'vault' }
      )

.PARAMETER AutoDiscover
    When set, queries Azure for PE-enabled resources in the target resource group rather than
    requiring -Resources to be supplied manually.

.PARAMETER ResourceGroupName
    The resource group to query when -AutoDiscover is used.
    Falls back to the RESOURCE_GROUP_NAME environment variable.

.PARAMETER SubscriptionId
    The Azure subscription ID. Used both to set the Az context and as the scope for
    -AutoDiscover queries. Falls back to the SUBSCRIPTION_ID environment variable.
    Resources in other subscriptions within Resources[] are handled automatically.

.PARAMETER ApprovalDescription
    The description text sent with each approval request.
    Defaults to 'Approved by Deploy-DatabricksNCC automation'.

.PARAMETER DescriptionFilter
    Optional regex pattern to narrow which pending connections are approved.
    Applies to the private endpoint connection name. Leave blank to approve ALL
    pending connections on each resource (useful when only Databricks creates PEs here).
    Example: 'databricks|ncc'

.PARAMETER WhatIfEnabled
    When $true, reports which connections would be approved without making changes.
    Defaults to the IS_PULL_REQUEST environment variable so it integrates with
    the existing pipeline WhatIf pattern.

.EXAMPLE
    $resources = @(
        @{ ResourceID = '/subscriptions/00000000.../resourceGroups/rg-dlz/providers/Microsoft.Storage/storageAccounts/stexample'; ResourceType = 'blob' },
        @{ ResourceID = '/subscriptions/00000000.../resourceGroups/rg-dlz/providers/Microsoft.KeyVault/vaults/kv-dlz-prod-01';    ResourceType = 'vault' }
    )
    .\Approve-DatabricksPrivateEndpoints.ps1 -Resources $resources

.EXAMPLE    # Auto-discover resources and approve all pending Databricks PE connections
    .\.Approve-DatabricksPrivateEndpoints.ps1 `
        -AutoDiscover `
        -SubscriptionId    '00000000-0000-0000-0000-000000000000' `
        -ResourceGroupName 'rg-contoso-dap-dev-uks-01' `
        -DescriptionFilter 'databricks'

.EXAMPLE    # WhatIf run to preview approvals
    .\Approve-DatabricksPrivateEndpoints.ps1 -Resources $resources -WhatIfEnabled $true

.NOTES
    - Requires the Az PowerShell module (Az.Network 4.0+ for Get/Approve-AzPrivateEndpointConnection)
    - Requires Az.Storage, Az.KeyVault, Az.Sql, Az.DataFactory, Az.CognitiveServices,
      Az.EventHub, Az.ServiceBus, Az.Synapse for auto-discovery (missing modules skipped gracefully)
    - The executing identity must have 'Microsoft.Network/privateEndpoints/privateLinkServiceConnections/write'
      on each target resource (typically Owner or Network Contributor)
    - Pending connections may take 2-5 minutes to appear after NCC rule creation
    - Resources spanning multiple subscriptions are handled automatically
#>
[CmdletBinding(SupportsShouldProcess, DefaultParameterSetName = 'Manual')]
param (
    # ── Manual mode ──────────────────────────────────────────────────────────
    [Parameter(ParameterSetName = 'Manual', Mandatory)]
    [array]$Resources,

    # ── Auto-discover mode ──────────────────────────────────────────────────
    [Parameter(ParameterSetName = 'AutoDiscover', Mandatory)]
    [switch]$AutoDiscover,

    [Parameter(ParameterSetName = 'AutoDiscover')]
    [string]$ResourceGroupName = "$($env:RESOURCE_GROUP_NAME)",

    # ── Common ───────────────────────────────────────────────────────────────
    [Parameter()]
    [string]$SubscriptionId = "$($env:SUBSCRIPTION_ID)",

    [Parameter()]
    [string]$ApprovalDescription = 'Approved by Deploy-DatabricksNCC automation',

    [Parameter()]
    [string]$DescriptionFilter = '',

    [Parameter()]
    [bool]$WhatIfEnabled = [System.Convert]::ToBoolean($($env:IS_PULL_REQUEST))
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

#region ── Pre-flight checks ──────────────────────────────────────────────────

$preflightErrors = [System.Collections.Generic.List[string]]::new()

# ── PowerShell version ───────────────────────────────────────────────────────
if ($PSVersionTable.PSVersion.Major -lt 5 -or
    ($PSVersionTable.PSVersion.Major -eq 5 -and $PSVersionTable.PSVersion.Minor -lt 1)) {
    $preflightErrors.Add("PowerShell 5.1 or later is required. Running: $($PSVersionTable.PSVersion)")
}

# ── Az module installed ──────────────────────────────────────────────────────
if (-not (Get-Module -Name Az -ListAvailable)) {
    $preflightErrors.Add("The 'Az' PowerShell module is not installed. Run: Install-Module -Name Az -Scope CurrentUser -Repository PSGallery -Force")
} else {
    # ── Az.Network (required — core PE approval cmdlets) ────────────────────
    if (-not (Get-Module -Name Az.Network -ListAvailable)) {
        $preflightErrors.Add("The 'Az.Network' module is not installed (required for Get/Approve-AzPrivateEndpointConnection). Run: Install-Module -Name Az.Network -Scope CurrentUser -Force")
    }

    # ── Optional sub-modules (warn only — missing modules skipped in auto-discover) ──
    $optionalModules = @(
        'Az.Accounts',
        'Az.Storage',
        'Az.KeyVault',
        'Az.Sql',
        'Az.DataFactory',
        'Az.CognitiveServices',
        'Az.EventHub',
        'Az.ServiceBus',
        'Az.Synapse'
    )
    foreach ($mod in $optionalModules) {
        if (-not (Get-Module -Name $mod -ListAvailable)) {
            Write-Warning "Optional module '$mod' is not installed. Resources of that type will be skipped during auto-discovery. Run: Install-Module -Name $mod -Scope CurrentUser -Force"
        }
    }
}

# ── Az login / active context ────────────────────────────────────────────────
$azContext = Get-AzContext -ErrorAction SilentlyContinue
if (-not $azContext -or -not $azContext.Account) {
    $preflightErrors.Add("No active Azure login detected. Run: Connect-AzAccount")
}

# ── Auto-discover mode: required parameters ──────────────────────────────────
if ($PSCmdlet.ParameterSetName -eq 'AutoDiscover') {
    if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
        $preflightErrors.Add("'-SubscriptionId' is required in AutoDiscover mode (or set the SUBSCRIPTION_ID environment variable).")
    }
    if ([string]::IsNullOrWhiteSpace($ResourceGroupName)) {
        $preflightErrors.Add("'-ResourceGroupName' is required in AutoDiscover mode (or set the RESOURCE_GROUP_NAME environment variable).")
    }
}

# ── Manual mode: Resources array must be non-empty ───────────────────────────
if ($PSCmdlet.ParameterSetName -eq 'Manual') {
    if (-not $Resources -or $Resources.Count -eq 0) {
        $preflightErrors.Add("'-Resources' must contain at least one entry in Manual mode.")
    } else {
        $invalidEntries = $Resources | Where-Object { -not $_['ResourceID'] }
        if ($invalidEntries) {
            $preflightErrors.Add("One or more entries in '-Resources' are missing a 'ResourceID' key.")
        }
    }
}

# ── Report all errors at once ────────────────────────────────────────────────
if ($preflightErrors.Count -gt 0) {
    Write-Host "`nPre-flight check failed. Please resolve the following issue(s):" -ForegroundColor Red
    $preflightErrors | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
    throw "Pre-flight checks failed with $($preflightErrors.Count) error(s). See above for details."
}

Write-Host 'Pre-flight checks passed.' -ForegroundColor Green

#endregion

#region ── Auto-discover: build resource list from deployed RG ────────────────

if ($AutoDiscover) {
    Write-Host 'Auto-discover mode: querying Azure for PE-enabled resources...' -ForegroundColor Cyan

    $null = Set-AzContext -SubscriptionId $SubscriptionId

    $discovered = [System.Collections.Generic.List[hashtable]]::new()

    if (Get-Command -Name 'Get-AzStorageAccount' -ErrorAction SilentlyContinue) {
        foreach ($sa in (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue)) {
            Write-Host "  Storage: $($sa.StorageAccountName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $sa.Id; ResourceType = 'blob' })
            $discovered.Add(@{ ResourceID = $sa.Id; ResourceType = 'dfs'  })
        }
    } else { Write-Warning "Az.Storage module not available — Storage Accounts will be skipped." }
    if (Get-Command -Name 'Get-AzKeyVault' -ErrorAction SilentlyContinue) {
        foreach ($kv in (Get-AzKeyVault -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue)) {
            Write-Host "  Key Vault: $($kv.VaultName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $kv.ResourceId; ResourceType = 'vault' })
        }
    } else { Write-Warning "Az.KeyVault module not available — Key Vaults will be skipped." }
    if (Get-Command -Name 'Get-AzSqlServer' -ErrorAction SilentlyContinue) {
        foreach ($sql in (Get-AzSqlServer -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue)) {
            Write-Host "  SQL Server: $($sql.ServerName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $sql.ResourceId; ResourceType = 'sqlServer' })
        }
    } else { Write-Warning "Az.Sql module not available — SQL Servers will be skipped." }
    if (Get-Command -Name 'Get-AzDataFactoryV2' -ErrorAction SilentlyContinue) {
        foreach ($adf in (Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue)) {
            Write-Host "  Data Factory: $($adf.DataFactoryName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $adf.DataFactoryId; ResourceType = 'dataFactory' })
        }
    } else { Write-Warning "Az.DataFactory module not available — Data Factories will be skipped." }
    if (Get-Command -Name 'Get-AzCognitiveServicesAccount' -ErrorAction SilentlyContinue) {
        foreach ($cog in (Get-AzCognitiveServicesAccount -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue)) {
            $cogKind = if ($cog.PSObject.Properties['Kind']) { $cog.Kind } else { 'Unknown' }
            $label = if ($cogKind -eq 'OpenAI') { "Azure OpenAI" } else { "Cognitive Services ($cogKind)" }
            Write-Host "  $label`: $($cog.AccountName)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $cog.Id; ResourceType = 'account' })
        }
    } else { Write-Warning "Az.CognitiveServices module not available — Cognitive Services will be skipped." }
    if (Get-Command -Name 'Get-AzEventHubNamespace' -ErrorAction SilentlyContinue) {
        foreach ($eh in (Get-AzEventHubNamespace -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue)) {
            Write-Host "  Event Hub: $($eh.Name)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $eh.Id; ResourceType = 'namespace' })
        }
    } else { Write-Warning "Az.EventHub module not available — Event Hub namespaces will be skipped." }
    if (Get-Command -Name 'Get-AzServiceBusNamespace' -ErrorAction SilentlyContinue) {
        foreach ($sb in (Get-AzServiceBusNamespace -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue)) {
            Write-Host "  Service Bus: $($sb.Name)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $sb.Id; ResourceType = 'namespace' })
        }
    } else { Write-Warning "Az.ServiceBus module not available — Service Bus namespaces will be skipped." }
    if (Get-Command -Name 'Get-AzSynapseWorkspace' -ErrorAction SilentlyContinue) {
        foreach ($syn in (Get-AzSynapseWorkspace -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue)) {
            Write-Host "  Synapse: $($syn.Name)" -ForegroundColor Gray
            $discovered.Add(@{ ResourceID = $syn.Id; ResourceType = 'Sql'         })
            $discovered.Add(@{ ResourceID = $syn.Id; ResourceType = 'SqlOnDemand' })
            $discovered.Add(@{ ResourceID = $syn.Id; ResourceType = 'Dev'         })
        }
    } else { Write-Warning "Az.Synapse module not available — Synapse workspaces will be skipped." }

    $Resources = $discovered.ToArray()
    Write-Host "  Total resources to check: $($Resources.Count)" -ForegroundColor Green
}

#endregion

#region ── Helper: parse an ARM resource ID into its components ───────────────

function Get-ArmResourceComponents {
    param ([string]$ResourceId)

    # Expected format:
    # /subscriptions/{sub}/resourceGroups/{rg}/providers/{namespace}/{type}/{name}
    # Also handle nested child resources:
    # /subscriptions/{sub}/resourceGroups/{rg}/providers/{namespace}/{type}/{name}/{childType}/{childName}
    $pattern = '^/subscriptions/(?<sub>[^/]+)/resourceGroups/(?<rg>[^/]+)/providers/(?<ns>[^/]+)/(?<type>[^/]+)/(?<name>[^/]+)'
    if ($ResourceId -notmatch $pattern) {
        throw "Cannot parse ARM resource ID: $ResourceId"
    }
    return @{
        SubscriptionId     = $Matches['sub']
        ResourceGroupName  = $Matches['rg']
        ProviderNamespace  = $Matches['ns']
        ResourceType       = $Matches['type']
        ResourceName       = $Matches['name']
        PrivateLinkType    = "$($Matches['ns'])/$($Matches['type'])"
    }
}

#endregion

#region ── Helper: set Az context to a specific subscription ──────────────────

function Set-ContextForSubscription {
    param ([string]$Sub)

    $current = (Get-AzContext).Subscription.Id
    if ($current -ne $Sub) {
        Write-Verbose "Switching Az context to subscription $Sub"
        $null = Set-AzContext -SubscriptionId $Sub
    }
}

#endregion

#region ── Optional: set default subscription context ────────────────────────

if ($SubscriptionId) {
    Write-Host "Setting default Az context to subscription $SubscriptionId..." -ForegroundColor Cyan
    $null = Set-AzContext -SubscriptionId $SubscriptionId
}

#endregion

#region ── Main: iterate resources and approve pending connections ─────────────

$totalApproved = 0
$totalSkipped  = 0
$totalPending  = 0

foreach ($resource in $Resources) {
    $resourceId        = $resource.ResourceID
    $resourceShortName = $resourceId.Split('/')[-1]

    Write-Host "`n──────────────────────────────────────────────────────────" -ForegroundColor DarkGray
    Write-Host "Resource: $resourceShortName" -ForegroundColor Cyan

    # Parse ARM ID
    try {
        $components = Get-ArmResourceComponents -ResourceId $resourceId
    }
    catch {
        Write-Warning "Skipping resource — $_"
        continue
    }

    # Ensure we're in the correct subscription for this resource
    Set-ContextForSubscription -Sub $components.SubscriptionId

    # Retrieve all private endpoint connections on this resource
    Write-Host "  Retrieving private endpoint connections..." -ForegroundColor Gray
    try {
        $allConnections = Get-AzPrivateEndpointConnection `
            -PrivateLinkResourceId $resourceId `
            -ErrorAction Stop
    }
    catch {
        Write-Warning "  Could not retrieve connections for $resourceId : $_"
        continue
    }

    if (-not $allConnections -or $allConnections.Count -eq 0) {
        Write-Host "  No private endpoint connections found." -ForegroundColor Yellow
        continue
    }

    Write-Host "  Found $($allConnections.Count) connection(s) total." -ForegroundColor Gray

    # Filter to pending connections
    $pendingConnections = @($allConnections | Where-Object {
        $_.PrivateLinkServiceConnectionState.Status -eq 'Pending'
    })

    if ($pendingConnections.Count -eq 0) {
        Write-Host "  No pending connections — nothing to approve." -ForegroundColor Green
        continue
    }

    Write-Host "  $($pendingConnections.Count) pending connection(s) found." -ForegroundColor Yellow
    $totalPending += $pendingConnections.Count

    # Optionally narrow by name pattern
    if ($DescriptionFilter) {
        $pendingConnections = @($pendingConnections | Where-Object {
            $_.Name -match $DescriptionFilter
        })
        Write-Host "  $($pendingConnections.Count) match filter '$DescriptionFilter'." -ForegroundColor Gray
    }

    foreach ($conn in $pendingConnections) {
        $connName = $conn.Name
        Write-Host "  Approving: $connName" -ForegroundColor Yellow
        Write-Host "    Current status : $($conn.PrivateLinkServiceConnectionState.Status)"
        Write-Host "    PE resource     : $($conn.PrivateEndpoint.Id)"

        if ($WhatIfEnabled) {
            Write-Host "    [WhatIf] Would approve connection '$connName'." -ForegroundColor Magenta
            $totalSkipped++
            continue
        }

        if ($PSCmdlet.ShouldProcess($connName, "Approve private endpoint connection")) {
            $maxRetries  = 6
            $retryDelaySecs = 20
            $approved    = $false

            for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
                try {
                    $null = Approve-AzPrivateEndpointConnection `
                        -ResourceId  $conn.Id `
                        -Description $ApprovalDescription `
                        -ErrorAction Stop

                    Write-Host "    Approved successfully." -ForegroundColor Green
                    $totalApproved++
                    $approved = $true
                    break
                }
                catch {
                    $errMsg = $_.ToString()
                    if ($errMsg -match 'provisioning state is not terminal' -or
                        ($_.Exception.PSObject.Properties['Response'] -and
                         $_.Exception.Response.StatusCode.value__ -eq 409)) {
                        if ($attempt -lt $maxRetries) {
                            Write-Host "    Resource still provisioning — waiting ${retryDelaySecs}s before retry ($attempt/$($maxRetries - 1))..." -ForegroundColor DarkYellow
                            Start-Sleep -Seconds $retryDelaySecs
                        }
                    } else {
                        Write-Warning "    Failed to approve '$connName': $_"
                        break
                    }
                }
            }

            if (-not $approved) {
                Write-Warning "    Could not approve '$connName' after $maxRetries attempts — resource provisioning state never became terminal."
            }
        }
    }
}

#endregion

#region ── Summary ────────────────────────────────────────────────────────────

Write-Host "`n══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "Approve-DatabricksPrivateEndpoints complete." -ForegroundColor Cyan
Write-Host "  Total pending found : $totalPending"
Write-Host "  Approved            : $totalApproved" -ForegroundColor Green
if ($totalSkipped -gt 0) {
    Write-Host "  Skipped (WhatIf)    : $totalSkipped" -ForegroundColor Magenta
}
Write-Host "══════════════════════════════════════════════════════════" -ForegroundColor Cyan

#endregion
