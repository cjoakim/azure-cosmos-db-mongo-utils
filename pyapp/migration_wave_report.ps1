# Create the RU Provisioning report from aggregated MMA outputs.
# Chris Joakim, Microsoft, 2023

Write-Output 'activating the python venv ...'
.\venv\Scripts\Activate.ps1

Write-Output 'docscan_results_report ...'
python main.py docscan_results_report

Write-Output 'migration_wave_report ...'
python main.py migration_wave_report

Write-Output 'done'