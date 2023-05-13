#!/bin/bash

# Generate the PowerShell script to execute MongoMigrationAssessment.exe
# once for each cluster configured in file 'in/swift-config.json'.
# Chris Joakim, Microsoft, 2023

Write-Output 'activating the python venv ...'
.\venv\Scripts\Activate.ps1

Write-Output 'python main.py read_parse_clusters_info_excel_file ...'
python main.py read_parse_clusters_info_excel_file

Write-Output 'done'
