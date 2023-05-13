# azure-cosmos-db-mongo-utils

Public repo for Cosmos DB Mongo Utilities

---

## Functionality

### pyapp subproject core functionality

This functionality has no dependency on customer source/target Excel mapping files.

- **Delete/Define the MMA output directory**
  - See pyapp/recreate_mma_output_directory.xml
- **Collect/aggregate verbose MMA outputs**
  - See pyapp/collect_mma_outputs.ps1 and pyapp/collect_mma_outputs.sh
- **Generate a single Excel Report from many MMA executions**
  - See pyapp/migration_wave_report.ps1 and pyapp/migration_wave_report.sh
- **pyapp/verify.py** - verify document counts and indices of target vs source DBs and collections
- **pyapp/indexes.py** - extract and compare source vs target indexes

### pyapp subproject extended functionality

This functionality depends on a customer-specific source/target Excel mapping files.

- **Parse Excel file into JSON data files**
  - See pyapp/read_parse_clusters_info_excel_file.ps1 and pyapp/read_parse_clusters_info_excel_file.sh
- **Generate MMA execution scripts from this Excel data**
  - See pyapp/generate_mma_execution_scripts.ps1 and pyapp/generate_mma_execution_scripts.sh
  - Some customers have hundreds of clusters, so automation enables efficiency and accuracy
- **Normalize the contents of a customer-created MMA zip file**
  - See pyapp/normalize_mma_zip_example.xml
  - Then unzip the normalized zip file into your MMA output directory
- **Excel reporting on the captured MMA outputs**
  - See pyapp/migration_wave_report.ps1
  - See pyapp/migration_wave_report.ps1
  - Calculates migration and post-migration RU settings for each collection/container
  - Integrates the various MMA assessments into the report (indexing, shards, etc)
  - Integrates mongodb-docscan (see above repo) into the report (wip)
  - Also creates PostgreSQL sql/csv files for the captured MMA data
- **throughput.py** - display and reduce Cosmos DB Request Unit (RU) settings post-migration

---

## Related GitHub Repositories

### MongoMigrationAssessment.exe (i.e. - MMA)

- https://github.com/AzureCosmosDB/Azure-CosmosDB-Migration-Assessment-for-MongoDB  (private)
- Contact your Microsoft team to get access to this repo
- Scans a source MongoDB cluster, identifies DB and Collection metadata, Cosmos DB assessments

### Docscan

- https://github.com/cjoakim/mongodb-docscan  (public)
- Reads source MongoDB clusters, identifies largest 10 documents in each container

### MongoDB Data Generator

- https://github.com/cjoakim/azure-cosmosdb-swift-data-generator
- Generates testing data, including large and very large documents

### Original Batch/Transforming Migration Process

- https://github.com/Azure-Samples/azure-cosmos-db-mongo-migration

---

## Directory Structure of this Repo

```
├── changestream_consumer     <-- coming soon; CosmosDB Mongo API or MongoDB change-stream consumer, implemented in Java
│
├── changestream_producer     <-- coming soon; producer of DB activity for the above changestream_consumer, implemented in Java
│
├── mongodb_docscan           <-- work-in-progress; a MongoDB large document scanner, implemented in Java
│
└── pyapp                     <-- Most Python and Ant scripts you'll execute are here; Python app root directory
    ├── artifact_examples
    │   └── bicep_examples
    ├── artifacts             <-- generated code artifacts; this is not fully implemented
    │   ├── bicep
    │   └── spark
    ├── current               <-- application state files, git-ignored
    │   ├── docscan           <-- unzipped output of mongodb-docscan program
    │   ├── mmaout            <-- redirected output of the MMA program
    │   └── psql              <-- generated PostgreSQL csv and scripts
    ├── pysrc                 <-- python application source code
    ├── templates
    ├── tests
    ├── tmp                   <-- temporary files, git-ignored
    └── venv                  <-- python virtual environment directory
```

---

## Required Software

- **git** - any recent version
  - see https://github.com/git-guides/install-git
- **Python 3**  (this repo was developed and tested with Python 3.11.1)
  - see https://www.python.org/downloads/

### Required Software - for MMA execution

- **Windows 11** with PowerShell script execution enabled
- **The MongoMigrationAssessment.exe is installed**
  - see private repo https://github.com/AzureCosmosDB/Azure-CosmosDB-Migration-Assessment-for-MongoDB
- **The Windows computer requires network access to the source MongoDB cluster(s)**

Note that the MongoMigrationAssessment.exe program must be executed on Windows,
but the other functionality on this repo does **not** require Windows.  A common
workflow is that the customer executes the MMA in their environment, and then
shares the zipped MMA output directory with Microsoft for subsequent analysis.

### Required Software - for Ant/xml script execution

- **Java and Apache Ant**
  - Java 11+ and Ant 1.10.12 or higher is recommeded
  - Microsoft OpenJDK is recommended; see https://www.microsoft.com/openjdk
  - Apache Ant requires Java, see https://ant.apache.org

---

## Getting Started

### Clone this repo

```
> git clone https://github.com/cjoakim/azure-cosmos-db-mongo-utils
> cd azure-cosmos-db-mongo-utils
```

### Navigate to the Python Application directory, and create the Python Virtual Environment

#### On Windows 

```
> cd pyapp
> .\create_venv_setup.ps1
```

#### on Linux or macOS

```
$ cd pyapp
$ ./create_venv_setup.sh
```

### Edit your verify.json configuration file

In the pyapp directory, copy file **verify-example.json** to **verify.json**.
File verify.json is intentionally git-ignored.

The format of this file is self-explanitory.  You can have multiple keys in 
the file, and key values are used as command-line arguments for some python scripts.

For example, verify.json can look like this:

```
{
  "migration1": {
    "cluster": "1-US-DEV (SOMETHING)",
    "source": "mongodb+srv://mongodb-source1...",
    "target": "mongodb://cosmosdb-target1...",
    "databases": [],
    "collections": []
  },
  "migration2": {
    "cluster": "1-US-UAT (SOMETHING)",
    "source": "mongodb+srv://mongodb-source2...",
    "target": "mongodb://cosmosdb-target2...",
    "databases": [
      "customers",
      "sales"
    ],
    "collections": []
  }
}
```

And these keys and configuration values are used like this:

```
python verify.py migration1
```

### Edit your mdb-cred.txt file

This is optional; a default value will be used if necessary.

Create a one-line file **mdb-cred.txt** in directory pyapp/current/cred
File mdb-cred.txt is intentionally git-ignored.

Format is <username>:<passord>

Example:
```
chris:superSecr3T
```

---

## Additional Documentation

See the header comments in each script.  For example, in **verify.py**, **indexes.py**,
and **througput.py**


