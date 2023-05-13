# PowerShell script to execute the MongoDB document-scanning process to identify
# the largest documents in each container in the given cluster connection string.
# Chris Joakim, Microsoft, 2023

param(
    [Parameter()]
    [String]$clean  = "no-clean",
    [String]$build  = "no-build"
)

if ('clean' -eq $clean) {
    Write-Output "cleaning the out\ directory..."
    rm out\*.json
}

if ('build' -eq $build) {
    Write-Output "compiling and creating uberJar file per command-line param..."
    del    .\build\libs\*.jar
    del    .\app-docscan.jar
    gradle clean
    gradle build
    gradle uberJar
    copy  .\build\libs\app-docscan.jar .
}

ForEach ($line in Get-Content clusters.txt) {
    # java -jar .\app-docscan.jar scan_without_sizes $line
    # java -jar .\app-docscan.jar scan_with_largest $line
    java -jar .\app-docscan.jar scan_with_top_ten_largest $line
}

$date = Get-Date -Format "yyyyMMdd-HHmm"
$zipfile = ".\docscan-{0}.zip" -f $date
Write-Output "creating file $zipfile with the out\ directory files..."
jar cvf $zipfile out\

Write-Output "done"
