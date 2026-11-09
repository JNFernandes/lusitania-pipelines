# Build the consumer image and put it in the Azure Container Registry.
# Run from C:\Projects\LusitaniaPipelines, after `terraform apply` created the registry:
#
#     .\cloud\build_consumer_image.ps1 -Registry acrlusitaniadevjf01 -Tag v1
#
# Default: build with Docker Desktop on this laptop, then PUSH to the registry. (Building IN Azure
# with "ACR Tasks" — the -InAzure switch — is blocked on Free Trial subscriptions.)
#
# Only what the image needs is copied into a temporary build folder (Project 2's src + pyproject,
# this project's streaming/): no data, .venv or logs.
param(
    [Parameter(Mandatory = $true)][string]$Registry,
    [string]$Tag = "v1",
    [switch]$InAzure
)
$ErrorActionPreference = "Stop"
$build = Join-Path $env:TEMP "lusitania-consumer-build"
if (Test-Path $build) { Remove-Item -Recurse -Force $build }
New-Item -ItemType Directory -Path "$build\lakehouse" | Out-Null

Copy-Item -Recurse ..\LusitaniaLakehouse\src "$build\lakehouse\src"
Copy-Item ..\LusitaniaLakehouse\pyproject.toml, ..\LusitaniaLakehouse\README.md "$build\lakehouse\"
Copy-Item -Recurse streaming "$build\streaming"
Get-ChildItem $build -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force
Copy-Item cloud\Dockerfile.consumer "$build\Dockerfile"

$image = "$Registry.azurecr.io/lusitania-consumer:$Tag"

if ($InAzure) {
    az acr build --registry $Registry --image "lusitania-consumer:$Tag" $build
    if ($LASTEXITCODE -ne 0) { throw "az acr build failed" }
    return
}

Write-Host "1/3 log in to the registry with your Azure login (no registry password: admin is disabled)"
az acr login --name $Registry
if ($LASTEXITCODE -ne 0) { throw "az acr login failed (is Docker Desktop running?)" }

Write-Host "2/3 build $image with Docker Desktop (first time ~5-10 min: Java, Spark, connectors)"
docker build --platform linux/amd64 -t $image $build
if ($LASTEXITCODE -ne 0) { throw "docker build failed" }

Write-Host "3/3 push to the registry (~1.5 GB the first time; later builds only send changed layers)"
docker push $image
if ($LASTEXITCODE -ne 0) { throw "docker push failed" }

Write-Host "Done: $image"
