#!/bin/bash

# Create the RU Provisioning report from aggregated MMA outputs.
# Chris Joakim, Microsoft, 2023

echo 'activating the python venv ...'
source venv/bin/activate
python --version

echo 'docscan_results_report ...'
python main.py docscan_results_report

echo 'clusters_report (migration waves) ...'
python main.py clusters_report

echo 'done'
