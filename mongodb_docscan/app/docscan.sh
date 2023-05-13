# Bash script to execute the MongoDB document-scanning process to identify
# the largest documents in each container in the given cluster connection string.
# Chris Joakim, Microsoft, 2023

for var in "$@"
do
    if [ "clean" == "$var" ]; then
        echo "cleaning the out\ directory..."
        rm out/*.json
    fi
done

for var in "$@"
do
    if [ "build" == "$var" ]; then
        echo "compiling and creating uberJar file per command-line param..."
        rm     build/libs/*.jar
        rm     app-docscan.jar
        gradle clean
        gradle build
        gradle uberJar
        cp     build/libs/app-docscan.jar .
    fi
done

cluster_lines=$(cat clusters.txt)
for line in $cluster_lines
do
    # java -jar .\app-docscan.jar scan_without_sizes $line
    # java -jar .\app-docscan.jar scan_with_largest $line
    java -jar app-docscan.jar scan_with_top_ten_largest $line
done

d=`date '+%Y%m%d-%H%M'`
zipfile="docscan-$d.zip"
echo "creating file $zipfile with the out\ directory files..."
jar cvf $zipfile out\

echo "done"
