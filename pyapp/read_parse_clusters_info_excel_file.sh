#!/bin/bash

# Read the Customer-specific Excel file which describes their MongoDB clusters
# and corresponding Cosmos DB targets.
# Chris Joakim, Microsoft, 2023

echo 'activating the python venv ...'
source venv/bin/activate
python --version

echo 'read_parse_clusters_info_excel_file ...'
python main.py read_parse_clusters_info_excel_file

echo 'done'
