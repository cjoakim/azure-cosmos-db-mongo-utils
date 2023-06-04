#!/bin/bash

# PowerShell script to open a mongo shell CLI pointing one of several
# named environments - atlas, cosmos_ru, cosmos_vcore, localhost.
#
# Usage:
#   .\mongosh.ps1 <envname>
#
# Chris Joakim, Microsoft, 2023

param(
    [Parameter()]
    [String]$env_name  = "xxx"
)

Write-Output "using env_name $env_name"

if ('atlas' -eq $env_name) {
    mongosh $env:AZURE_ATLAS_CONN_STR --ssl
}
elseif ('cosmos_ru' -eq $env_name) {
    mongosh $env:AZURE_COSMOSDB_MONGODB_CONN_STRING --ssl
}
elseif ('cosmos_vcore' -eq $env_name) {
    mongosh $env:AZURE_COSMOSDB_MONGO_VCORE_CONN_STR --ssl
}
elseif ('local' -eq $env_name) {
    mongosh $env:MONGODB_LOCAL_URL
}
else {
    Write-Output "unknown env_name $env_name, terminating"
    Write-Output ""
    Write-Output "Usage:"
    Write-Output ".\mongosh.ps1 <env> where env is atlas, cosmos_ru, cosmos_vcore, or localhost"
    Write-Output ".\mongosh.ps1 cosmos_ru"
    Write-Output ""
}
