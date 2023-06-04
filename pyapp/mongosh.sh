#!/bin/bash

# Bash script to open a mongo shell CLI pointing one of several
# named environments - atlas, cosmos_ru, cosmos_vcore, localhost.
#
# Usage:
#   ./mongosh <envname>
#
# Chris Joakim, Microsoft, 2023

if [ "$1" == 'atlas' ]
then 
    echo 'connecting to: '$AZURE_ATLAS_CONN_STR
    mongosh $AZURE_ATLAS_CONN_STR --ssl
fi

if [ "$1" == 'cosmos_ru' ]
then 
    echo 'connecting to: '$AZURE_COSMOSDB_MONGODB_CONN_STRING
    mongosh $AZURE_COSMOSDB_MONGODB_CONN_STRING --ssl
fi

if [ "$1" == 'cosmos_vcore' ]
then 
    echo 'connecting to: '$AZURE_COSMOSDB_MONGO_VCORE_CONN_STR
    mongosh $AZURE_COSMOSDB_MONGO_VCORE_CONN_STR --ssl
fi

if [ "$1" == 'local' ]
then 
    echo 'connecting to '$MONGODB_LOCAL_URL
    mongosh $MONGODB_LOCAL_URL
fi
