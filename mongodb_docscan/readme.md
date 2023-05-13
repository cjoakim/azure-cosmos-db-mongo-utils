# mongodb-docscan

MongoDB Document Scannner utility program

## Usage Instructions

### System Requirements

- Git
- Java Runtime
  - JDK/JRE version 11 minimum, see https://www.mongodb.com/docs/drivers/java/sync/current/faq/
  - Compiler is optional
_ Gradle build tool, but only IF you choose to compile the code
  - **This repo already contains the compiled app-docscan.jar uberJar, so compilation is not necessary**
  - see https://gradle.org/
- Network access to the source MongoDB clusters
- Windows, macOS, or Linux OS
  - But the provided *.ps1 scripts are intended for Windows 11

Note: 'openjdk 11.0.16.1 2022-08-12 LTS' was used to build this project

### Clone this GitHub Repo

```
> git clone https://github.com/cjoakim/mongodb-docscan.git
```

### Edit your clusters.txt file 

- Copy app/clusters-sample.txt to app/clusters.txt
- Edit file app/clusters.txt with your target MongoDB cluster configuration
  - note the format of each line in this file: **friendly-name then a vertibar then the connection string**

Note: File **clusters.txt** is intentionally **git-ignored** in this repo.

Important: The friendly-names in clusters.txt must be unique.

#### Example clusters.txt file

```
<friendly-cluster-name-1>|mongodb+srv://<user>:<password>@<host1>
<friendly-cluster-name-2>|mongodb+srv://<user>:<password>@<host2>
<friendly-cluster-name-3>|mongodb+srv://<user>:<password>@<host3>
```

### Compiling is Optional

See the docscan.ps1 and docscan.sh **build** option below.

### Execution 

Both a **docscan.ps1** PowerShell script for Windows and a **docscan.sh** Bash script
for macOS and Linux are in this repo.  These scripts perform equivalent functionality.

The following instructions are for Windows and PowerShell, but the macOS/Linux usage
is similar.

```
> cd app
> .\docscan.ps1                  Executes the scans per your clusters.txt
  or
> .\docscan.ps1 clean            Same as above, but also cleans\deletes previous output in the out\ directory
  or
> .\docscan.ps1 clean build      Same as above, but also compiles the code and creates the uberJar with Gradle
                                 This repo already contains the compiled app-docscan.jar uberJar, so compilation is not necessary
```

The Java program will execute once for each line in clusters.txt, and create an output
JSON file in the out\ directory.  One output file is created per execution.
The output filenames contain the friendly name, mongodb host name, and the epoch timestamp.

Example output filename:

```
DocScanResults_someclustername.somehost_1680439419463.json
```

At the end of the script, docscan.ps1 will create a Jar/Zip file containing the contents
of the out\ directory.  The zip file is created in the same directory as docscan.ps1.
The zip file will contain the date and time; for example:

```
docscan-20230402-1014.zip
```

Please share this zip file with your Microsoft team.

---

## Links

- https://github.com/mongodb-developer/java-quick-start
- https://stackoverflow.com/questions/34545555/how-to-get-size-in-bytes-of-bson-documents
