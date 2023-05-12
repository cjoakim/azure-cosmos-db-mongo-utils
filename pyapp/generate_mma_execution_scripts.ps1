# Generate the PowerShell script to execute MongoMigrationAssessment.exe
# once for each cluster configured in file 'in/swift-config.json'.
# Chris Joakim, Microsoft, 2023

Write-Output 'activating the python venv ...'
.\venv\Scripts\Activate.ps1

Write-Output 'python main.py check_env ...'
python main.py check_env

Write-Output 'python main.py gen_mma_execution_script ...'
python main.py gen_mma_execution_scripts

Write-Output 'done'
