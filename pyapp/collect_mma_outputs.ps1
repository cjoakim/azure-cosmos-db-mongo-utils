# Scan the MMA output directory for html and json files, and
# collect various output files - containers, shards, indexes, etc..
# Chris Joakim, Microsoft, 2023

Write-Output 'activating the python venv ...'
.\venv\Scripts\Activate.ps1

$aggregate=1
foreach ($arg in $args) {
    write-host $arg
    if ($arg -eq '--no-agg') {
        $aggregate=0
    }
}

if ($aggregate -gt 0) {
    Write-Output 'aggregate_mma_execution_output'
    python main.py aggregate_mma_execution_output
}
else {
    Write-Output 'bypassing aggregate_mma_execution_output, using previously merged file'
}

Write-Output 'gather_shard_info'
python main.py gather_shard_info

Write-Output 'gather_index_info'
python main.py gather_index_info

Write-Output 'gather_cluster_db_collection_info'
python main.py gather_cluster_db_collection_info

Write-Output 'done'
