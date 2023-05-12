#!/bin/bash

# Scan the MMA output directory for html and json files, and
# collect various output files - containers, shards, indexes, etc..
# Chris Joakim, Microsoft, 2023

echo 'activating the python venv ...'
source venv/bin/activate
python --version

echo 'deleting tmp\ files ...'
rm tmp\*.*

echo 'aggregate_mma_execution_output'
python main.py aggregate_mma_execution_output

echo 'gather_shard_info'
python main.py gather_shard_info

echo 'gather_index_info'
python main.py gather_index_info

echo 'gather_cluster_db_collection_info'
python main.py gather_cluster_db_collection_info

echo 'done'
