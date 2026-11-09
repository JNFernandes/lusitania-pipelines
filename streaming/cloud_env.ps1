# CLOUD MODE for this PowerShell window: read every cloud setting from Key Vault into memory.
#
#     . .\streaming\cloud_env.ps1          cloud on  (note the DOT and the space: "dot-sourcing",
#                                           so the variables stay in THIS window after the script)
#     . .\streaming\cloud_env.ps1 -Off     cloud off: back to local Kafka and the laptop folder
#
# Then, in the same window: the producer, `docker compose up -d consumer`, look_stream.py.
# Nothing is written to a file: close the window and the secrets are gone.
param([switch]$Off)

$vault = "kv-lusitania-dev-jf01"
$lake  = "stlusitaniadevjf01"
$names = "EVENTHUBS_CONNECTION_STRING", "LAKE_ACCOUNT", "LAKE_TENANT_ID", "LAKE_CLIENT_ID", "LAKE_CLIENT_SECRET"

if ($Off) {
    foreach ($n in $names) { Remove-Item "Env:$n" -ErrorAction SilentlyContinue }
    Write-Host "Cloud mode OFF: local Kafka, tables on the laptop."
    return
}

function Get-VaultSecret([string]$name) {
    $value = az keyvault secret show --vault-name $vault --name $name --query value -o tsv
    if (-not $value) { throw "Could not read '$name' from $vault (does the vault exist? is your role active?)" }
    return $value
}

$env:EVENTHUBS_CONNECTION_STRING = Get-VaultSecret "eventhubs-connection-string"
$env:LAKE_ACCOUNT       = $lake
$env:LAKE_TENANT_ID     = Get-VaultSecret "consumer-tenant-id"
$env:LAKE_CLIENT_ID     = Get-VaultSecret "consumer-client-id"
$env:LAKE_CLIENT_SECRET = Get-VaultSecret "consumer-client-secret"

Write-Host "Cloud mode ON for this window:"
Write-Host "  Kafka   = Event Hubs (connection string: $($env:EVENTHUBS_CONNECTION_STRING.Length) characters)"
Write-Host "  tables  = abfss://stream@$lake.dfs.core.windows.net/ (as service principal $env:LAKE_CLIENT_ID)"
