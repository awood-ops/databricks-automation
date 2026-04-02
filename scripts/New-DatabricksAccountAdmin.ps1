<#
.SYNOPSIS
    Adds a user or service principal as a Databricks Account Admin.

.DESCRIPTION
    This script uses the Databricks Accounts SCIM API to:
      1. Obtain an OAuth2 token using a service principal (client credentials flow) or the
         current Az session.
      2. Look up the target principal in the Databricks account by Entra ID object ID.
         If the user/service principal does not yet exist in the Databricks account it is
         created (provisioned).
      3. Assign the built-in 'account_admin' role to the principal.

    The executing service principal must already be a Databricks Account Admin.

    Supports both user accounts (UserPrincipalName or object ID) and service principals
    (application/client object ID).

.PARAMETER AccountID
    The Databricks account UUID, visible in the Accounts Console:
    https://accounts.azuredatabricks.net — top-right profile menu.
    Falls back to the DATABRICKS_ACCOUNT_ID environment variable.

.PARAMETER PrincipalObjectId
    The Entra ID object ID of the user or service principal to promote.
    Mutually exclusive with -UserPrincipalName.

.PARAMETER UserPrincipalName
    The UPN (e.g. user@contoso.com) of the user to promote.
    The script resolves this to an Entra ID object ID using the Microsoft Graph API.
    Mutually exclusive with -PrincipalObjectId.

.PARAMETER PrincipalType
    The type of principal: 'User' or 'ServicePrincipal'.
    Defaults to 'User'.

.PARAMETER WhatIfEnabled
    When $true, reports what would happen without making changes.
    Defaults to the IS_PULL_REQUEST environment variable so it integrates with
    the existing pipeline WhatIf pattern.

.EXAMPLE
    # Add a user by UPN
    .\New-DatabricksAccountAdmin.ps1 `
        -AccountID         '00000000-0000-0000-0000-000000000000' `
        -UserPrincipalName 'alice@contoso.com'

.EXAMPLE
    # Add a service principal by Entra ID object ID
    .\New-DatabricksAccountAdmin.ps1 `
        -AccountID         '00000000-0000-0000-0000-000000000000' `
        -PrincipalObjectId 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' `
        -PrincipalType     'ServicePrincipal'

.EXAMPLE
    # WhatIf — preview without making changes
    .\New-DatabricksAccountAdmin.ps1 `
        -AccountID          '00000000-0000-0000-0000-000000000000' `
        -UserPrincipalName  'alice@contoso.com' `
        -WhatIfEnabled      $true

.NOTES
    - Requires PowerShell 7+ or Windows PowerShell 5.1 with Invoke-RestMethod
    - The executing identity must hold the Account Admin role in the Databricks Accounts Console
    - Az 12+ returns Get-AzAccessToken tokens as SecureString — handled automatically
    - UPN resolution requires Microsoft Graph access (delegated or application Directory.Read.All)
    - Service principal object IDs can be found in Entra ID → Enterprise Applications
#>
[CmdletBinding(SupportsShouldProcess, DefaultParameterSetName = 'ByObjectId')]
param (
    [Parameter()]
    [string]$AccountID = "$($env:DATABRICKS_ACCOUNT_ID)",

    # ── Identify the principal ────────────────────────────────────────────────
    [Parameter(ParameterSetName = 'ByObjectId', Mandatory)]
    [string]$PrincipalObjectId,

    [Parameter(ParameterSetName = 'ByUPN', Mandatory)]
    [string]$UserPrincipalName,

    [Parameter()]
    [ValidateSet('User', 'ServicePrincipal')]
    [string]$PrincipalType = 'User',

    [Parameter()]
    [bool]$WhatIfEnabled = [System.Convert]::ToBoolean($($env:IS_PULL_REQUEST ?? 'false'))
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

#region ── Validate AccountID ────────────────────────────────────────────────

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

#region ── Helper: Invoke Databricks REST with retry ─────────────────────────

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

#region ── Step 1: Obtain Databricks OAuth token ─────────────────────────────

Write-Host 'Step 1: Obtaining Databricks OAuth token...' -ForegroundColor Cyan

try {
    $rawToken = (Get-AzAccessToken -ResourceUrl 'https://accounts.azuredatabricks.net/' -ErrorAction Stop).Token
}
catch {
    $rawToken = (Get-AzAccessToken -Resource '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d' -ErrorAction Stop).Token
}
$dbToken = if ($rawToken -is [System.Security.SecureString]) {
    $rawToken | ConvertFrom-SecureString -AsPlainText
} else {
    $rawToken
}

Write-Host 'Databricks OAuth token obtained successfully.' -ForegroundColor Green
$headers = @{ Authorization = "Bearer $dbToken" }

#endregion

#region ── Step 2: Resolve UPN → Entra ID object ID (if needed) ──────────────

if ($PSCmdlet.ParameterSetName -eq 'ByUPN') {
    Write-Host "`nStep 2: Resolving UPN '$UserPrincipalName' via Microsoft Graph..." -ForegroundColor Cyan

    try {
        $rawGraphToken = (Get-AzAccessToken -ResourceUrl 'https://graph.microsoft.com/' -ErrorAction Stop).Token
    }
    catch {
        $rawGraphToken = (Get-AzAccessToken -Resource 'https://graph.microsoft.com/' -ErrorAction Stop).Token
    }
    $graphToken = if ($rawGraphToken -is [System.Security.SecureString]) {
        $rawGraphToken | ConvertFrom-SecureString -AsPlainText
    } else {
        $rawGraphToken
    }

    # Use $filter rather than a direct path lookup — more resilient for guests and
    # UPNs that contain characters which behave differently when path-encoded.
    $encodedUpn    = [Uri]::EscapeDataString("'$UserPrincipalName'")
    $graphResponse = Invoke-RestMethod `
        -Method  Get `
        -Uri     "https://graph.microsoft.com/v1.0/users?`$filter=userPrincipalName+eq+$encodedUpn&`$select=id,displayName,userPrincipalName" `
        -Headers @{ Authorization = "Bearer $graphToken" }

    $graphUser = if ($graphResponse.value) { @($graphResponse.value)[0] } else { $null }

    if ($null -eq $graphUser) {
        throw "User '$UserPrincipalName' was not found in the directory. Verify the UPN and that you are logged into the correct tenant (run Get-AzContext to check)."
    }

    $PrincipalObjectId = $graphUser.id
    Write-Host "  Resolved '$UserPrincipalName' → object ID: $PrincipalObjectId ($($graphUser.displayName))" -ForegroundColor Green
}
else {
    Write-Host "`nStep 2: Using supplied object ID: $PrincipalObjectId" -ForegroundColor Cyan
}

#endregion

#region ── Step 3: Look up / provision the principal in the Databricks account ─

Write-Host "`nStep 3: Looking up principal in Databricks account..." -ForegroundColor Cyan

# Account-level SCIM API base — note the /scim/v2 prefix required by Databricks
$apiBase  = "https://accounts.azuredatabricks.net/api/2.0/accounts/$AccountID"
$scimBase = "$apiBase/scim/v2"
$databricksId = $null

if ($PrincipalType -eq 'ServicePrincipal') {
    # Service principals are looked up by applicationId (the Entra app/client ID,
    # NOT the enterprise-object ID). Pass -PrincipalObjectId as the applicationId.
    $encodedId = [Uri]::EscapeDataString("'$PrincipalObjectId'")
    $spList = Invoke-DatabricksRest `
        -Method  'Get' `
        -Uri     "$scimBase/ServicePrincipals?filter=applicationId+eq+$encodedId" `
        -Headers $headers

    $existingSP = if ($spList.PSObject.Properties['Resources']) { @($spList.Resources) | Select-Object -First 1 } else { $null }

    if ($existingSP) {
        $databricksId = $existingSP.id
        Write-Host "  Service principal already exists in account. Databricks ID: $databricksId" -ForegroundColor Green
    }
    else {
        Write-Host "  Service principal not found — provisioning..." -ForegroundColor Yellow
        if ($WhatIfEnabled) {
            Write-Host "  [WhatIf] Would create service principal with applicationId '$PrincipalObjectId'." -ForegroundColor DarkYellow
        }
        else {
            if ($PSCmdlet.ShouldProcess("Databricks account $AccountID", "Provision service principal $PrincipalObjectId")) {
                $createBody = @{
                    schemas       = @('urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal')
                    applicationId = $PrincipalObjectId
                } | ConvertTo-Json
                $created = Invoke-DatabricksRest `
                    -Method  'Post' `
                    -Uri     "$scimBase/ServicePrincipals" `
                    -Headers $headers `
                    -Body    $createBody
                $databricksId = $created.id
                Write-Host "  Service principal provisioned. Databricks ID: $databricksId" -ForegroundColor Green
            }
        }
    }
}
else {
    # Users are looked up by userName (= UPN). When only an object ID was supplied,
    # $UserPrincipalName was already resolved by the Graph call in Step 2; if the
    # ByObjectId parameter set was used without a UPN we resolve it here.
    if (-not $UserPrincipalName) {
        try {
            $rawGraphToken = (Get-AzAccessToken -ResourceUrl 'https://graph.microsoft.com/' -ErrorAction Stop).Token
        }
        catch {
            $rawGraphToken = (Get-AzAccessToken -Resource 'https://graph.microsoft.com/' -ErrorAction Stop).Token
        }
        $graphToken = if ($rawGraphToken -is [System.Security.SecureString]) {
            $rawGraphToken | ConvertFrom-SecureString -AsPlainText
        } else {
            $rawGraphToken
        }
        $encodedUpn    = [Uri]::EscapeDataString("'$PrincipalObjectId'")
        $graphResponse = Invoke-RestMethod `
            -Method  Get `
            -Uri     "https://graph.microsoft.com/v1.0/users?`$filter=id+eq+$encodedUpn&`$select=id,displayName,userPrincipalName" `
            -Headers @{ Authorization = "Bearer $graphToken" }
        $graphUser = if ($graphResponse.value) { @($graphResponse.value)[0] } else { $null }
        if ($null -eq $graphUser) { throw "Could not resolve object ID '$PrincipalObjectId' to a UPN via Microsoft Graph." }
        $UserPrincipalName = $graphUser.userPrincipalName
        Write-Host "  Resolved object ID → UPN: $UserPrincipalName" -ForegroundColor Gray
    }

    $encodedName = [Uri]::EscapeDataString("'$UserPrincipalName'")
    $userList = Invoke-DatabricksRest `
        -Method  'Get' `
        -Uri     "$scimBase/Users?filter=userName+eq+$encodedName" `
        -Headers $headers

    $existingUser = if ($userList.PSObject.Properties['Resources']) { @($userList.Resources) | Select-Object -First 1 } else { $null }

    if ($existingUser) {
        $databricksId = $existingUser.id
        Write-Host "  User already exists in account. Databricks ID: $databricksId" -ForegroundColor Green
    }
    else {
        Write-Host "  User not found — provisioning..." -ForegroundColor Yellow

        if ($WhatIfEnabled) {
            Write-Host "  [WhatIf] Would create user '$UserPrincipalName'." -ForegroundColor DarkYellow
        }
        else {
            if ($PSCmdlet.ShouldProcess("Databricks account $AccountID", "Provision user $UserPrincipalName")) {
                $createBody = @{
                    schemas  = @('urn:ietf:params:scim:schemas:core:2.0:User')
                    userName = $UserPrincipalName
                } | ConvertTo-Json
                $created = Invoke-DatabricksRest `
                    -Method  'Post' `
                    -Uri     "$scimBase/Users" `
                    -Headers $headers `
                    -Body    $createBody
                $databricksId = $created.id
                Write-Host "  User provisioned. Databricks ID: $databricksId" -ForegroundColor Green
            }
        }
    }
}

#endregion

#region ── Step 4: Assign account_admin role ─────────────────────────────────

Write-Host "`nStep 4: Assigning 'account_admin' role..." -ForegroundColor Cyan

if ($WhatIfEnabled) {
    Write-Host "  [WhatIf] Would assign 'account_admin' role to Databricks ID '$databricksId'." -ForegroundColor DarkYellow
}
elseif ($databricksId) {
    # The Accounts SCIM API uses PATCH with 'add' to assign roles.
    # The role group ID for account_admin is the well-known name 'account_admin'.
    $patchBody = @{
        schemas    = @('urn:ietf:params:scim:api:messages:2.0:PatchOp')
        Operations = @(
            @{
                op    = 'add'
                path  = 'roles'
                value = @(
                    @{ value = 'account_admin' }
                )
            }
        )
    } | ConvertTo-Json -Depth 5

    $endpoint = if ($PrincipalType -eq 'ServicePrincipal') { 'ServicePrincipals' } else { 'Users' }

    if ($PSCmdlet.ShouldProcess("Databricks principal $databricksId", "Assign role 'account_admin'")) {
        $null = Invoke-DatabricksRest `
            -Method  'Patch' `
            -Uri     "$scimBase/$endpoint/$databricksId" `
            -Headers $headers `
            -Body    $patchBody

        Write-Host "  'account_admin' role assigned successfully." -ForegroundColor Green
    }
}
else {
    Write-Warning "Databricks ID could not be determined — skipping role assignment."
}

#endregion

Write-Host "`nNew-DatabricksAccountAdmin complete." -ForegroundColor Cyan
if ($PrincipalType -eq 'ServicePrincipal') {
    Write-Host "Principal (SP)  : $PrincipalObjectId" -ForegroundColor Cyan
} else {
    Write-Host "Principal (User): $($UserPrincipalName ?? $PrincipalObjectId)" -ForegroundColor Cyan
}
Write-Host "Databricks ID   : $databricksId" -ForegroundColor Cyan
Write-Host "Role            : account_admin" -ForegroundColor Cyan
