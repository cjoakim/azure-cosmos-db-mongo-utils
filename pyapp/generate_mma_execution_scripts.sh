#!/bin/bash

# Generate the PowerShell script to execute MongoMigrationAssessment.exe
# This is driven by the customer-specific Excel file.
# Chris Joakim, Microsoft, 2023

echo 'activating the python venv ...'
source venv/bin/activate

echo 'python main.py check_env ...'
python main.py check_env

echo 'python main.py gen_mma_execution_script ...'
python main.py gen_mma_execution_scripts

echo 'done'
