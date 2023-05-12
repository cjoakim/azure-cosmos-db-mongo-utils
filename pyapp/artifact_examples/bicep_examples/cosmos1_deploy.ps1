
$resource_group='gbbchriscosmos1'
$region1='eastus'
$region2='westus'
$tmp_dir='tmp'

if (Test-Path -Path $tmp_dir) {
    # the directory already exists
}
else {
    Write-Host("creating directory: {0} ..." -f $tmp_dir)
    New-Item -ItemType "directory" -Path $tmp_dir
}

Write-Host ("creating resource group: {0} ..." -f $resource_group)
az group create `
    --location     $region1 `
    --name         $resource_group `
    --subscription $Env:AZURE_SUBSCRIPTION_ID

Write-Host ("creating bicep deployment...")
az deployment group create `
    --resource-group $resource_group `
    --template-file cosmos1.bicep `
    --parameters `
        primaryRegion=$region1 `
        secondaryRegion=$region2 `
        databaseName='db2' `
        collection1Name='c3' `
        collection2Name='c4'  > tmp\cosmos1_deploy.json 

echo 'done'
