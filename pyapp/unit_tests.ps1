# Excute the unit tests for the application.
# Chris Joakim, Microsoft, 2023

Write-Output 'activating the python venv ...'
.\venv\Scripts\Activate.ps1

pytest
