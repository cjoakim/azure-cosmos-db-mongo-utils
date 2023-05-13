#!/bin/bash

# Create the RU Provisioning report from aggregated MMA outputs.
# Chris Joakim, Microsoft, 2023

echo 'activating the python venv ...'
source venv/bin/activate
python --version

echo 'docscan_results_report ...'
python main.py docscan_results_report

echo 'migration_wave_report ...'
python main.py migration_wave_report

echo 'done'
