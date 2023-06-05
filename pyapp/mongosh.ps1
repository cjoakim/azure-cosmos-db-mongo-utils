#!/bin/bash

# PowerShell script to open a mongo shell CLI pointing one of several
# named environments - atlas, cosmos_ru, cosmos_vcore, localhost.
#
# Usage:
#   .\mongosh.ps1 <envname> <optional-commands-file>
#
# Chris Joakim, Microsoft, 2023

param(
    [Parameter()]
    [String]$env_name  = "xxx",
    [String]$commands_file = ""
)

Write-Output "using env_name '$env_name' and commands file '$commands_file'"

if ('atlas' -eq $env_name) {
    if ("" -eq $commands_file) {
        mongosh $env:AZURE_ATLAS_CONN_STR --ssl
    }
    else {
        mongosh $env:AZURE_ATLAS_CONN_STR --ssl -f $commands_file
    } 
}
elseif ('cosmos_ru' -eq $env_name) {
    if ("" -eq $commands_file) {
        mongosh $env:AZURE_COSMOSDB_MONGODB_CONN_STRING --ssl
    }
    else {
        mongosh $env:AZURE_COSMOSDB_MONGODB_CONN_STRING --ssl -f $commands_file
    } 
}
elseif ('cosmos_vcore' -eq $env_name) {
    if ("" -eq $commands_file) {
        mongosh $env:AZURE_COSMOSDB_MONGO_VCORE_CONN_STR --tls
    }
    else {
        mongosh $env:AZURE_COSMOSDB_MONGO_VCORE_CONN_STR --tls -f $commands_file
    } 
}
elseif ('local' -eq $env_name) {
    if ("" -eq $commands_file) {
        mongosh $env:MONGODB_LOCAL_URL
    }
    else {
        mongosh $env:MONGODB_LOCAL_URL -f $commands_file
    } 
}
else {
    Write-Output "unknown env_name $env_name, terminating"
    Write-Output ""
    Write-Output "Usage:"
    Write-Output ".\mongosh.ps1 <env> <optional-commands-file> where env is atlas, cosmos_ru, cosmos_vcore, or localhost"
    Write-Output ".\mongosh.ps1 cosmos_ru"
    Write-Output ""
}
