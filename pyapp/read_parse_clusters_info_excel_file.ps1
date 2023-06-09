# Read the Customer-specific Excel file which describes their MongoDB clusters
# and corresponding Cosmos DB targets.
# Chris Joakim, Microsoft, 2023

Write-Output 'activating the python venv ...'
.\venv\Scripts\Activate.ps1

Write-Output 'python main.py read_parse_clusters_info_excel_file ...'
python main.py read_parse_clusters_info_excel_file

Write-Output 'done'
