"""
Usage:
  Command-Line Format:     
    python verify.py <key>     <--- where <key> is a key in the verify.json dictionary
  Examples:
    python verify.py brown
Notes:
  1) First, edit your file verify.json (see verify-example.json)
     Then execute the following commands in PowerShell:
     > .\venv\Scripts\activate    <--- activate the Python Virtual Environment
     > python verify.py <key>     <--- where <key> is a key in the verify.json dict
  2) file verify.json is intentionally git-ignored (see file .gitignore)
  3) be sure to execute script 'create_venv_setup.ps1' to create the
     Python Virtual Environment with the necessary libraries in requirements.in
  4) Python 3 is required
     > python --version
       Python 3.11.1
"""

import json
import os.path
import sys

from pysrc.constants import Colors
from pysrc.mongo import Mongo, MongoDBInstance, MongoDBDatabase, MongoDBCollection

def compare_instances(
        source_instance: MongoDBInstance,
        target_instance: MongoDBInstance,
        specified_databases,
        specified_collections):
    source_dbs = set(source_instance.databases)
    target_dbs = set(target_instance.databases)
    filtered_source_dbs = filter_specified_databases(specified_databases, source_dbs)

    if len(specified_databases) == 0:
        # report these diffs only if there are no specified databases
        databases_not_in_both   = filtered_source_dbs.symmetric_difference(target_dbs)
        databases_not_in_source = databases_not_in_both.intersection(target_dbs)
        databases_not_in_target = databases_not_in_both.intersection(filtered_source_dbs)
        if databases_not_in_source:
            print(f'Databases not in source:',
                  Colors.WARNING, databases_not_in_source, Colors.ENDC)
        if databases_not_in_target:
            print(f'Databases not in target:',
                  Colors.WARNING, databases_not_in_target, Colors.ENDC)

    for database_name in sorted(filtered_source_dbs):
        source_db = source_instance.get_database(database_name)
        target_db = target_instance.get_database(database_name)
        print(f'   Comparing database: {database_name}')

        source_collections = set(source_db.collections)
        target_collections = set(target_db.collections)
        collections_in_any = source_collections.union(target_collections)

        if len(specified_collections) == 0:
            # report these diffs only if there are no specified collections
            collections_not_in_both = source_collections.symmetric_difference(target_collections)
            if "system.views" in collections_not_in_both:
                collections_not_in_both.remove("system.views")
            collections_not_in_source = collections_not_in_both.intersection(target_collections)
            collections_not_in_target = collections_not_in_both.intersection(source_collections)
            if collections_not_in_source:
                print(f'           * Collections not in source:', Colors.WARNING, collections_not_in_source, Colors.ENDC)
            if collections_not_in_target:
                print(f'           * Collections not in target:', Colors.WARNING, collections_not_in_target, Colors.ENDC)

        collections_in_any = filter_specified_collections(specified_collections, collections_in_any)
        if "system.views" in collections_in_any:
            collections_in_any.remove("system.views")

        for collection_name in sorted(collections_in_any):
            source_collection = source_db.get_collection(collection_name)
            target_collection = target_db.get_collection(collection_name)

            compare_documents(collection_name, source_collection, target_collection)
            # compare_sizes(source_collection, target_collection, source_db, target_db)
            if collection_name in source_collections and collection_name in target_collections:
                compare_indexes(source_collection, target_collection)

    print('Done')

def filter_specified_databases(specified_databases, actual_dbs):
    if len(specified_databases) == 0:
        return set(actual_dbs)
    else:
        filtered_list = list()
        for actual_db in actual_dbs:
            if actual_db in specified_databases:
                filtered_list.append(actual_db)
        return set(filtered_list)

def filter_specified_collections(specified_collections, actual_collections):
    if len(specified_collections) == 0:
        return set(actual_collections)
    else:
        filtered_list = list()
        for actual_coll in actual_collections:
            if actual_coll in specified_collections:
                filtered_list.append(actual_coll)
        return set(filtered_list)

def compare_sizes(source_collection, target_collection, source_db, target_db):
    size1 = source_collection.get_size(source_db)
    size2 = target_collection.get_size(target_db)

    if size1 != size2:
        difference_in_bytes = abs(size1 - size2)
        print(f'           * Difference in bytes: {difference_in_bytes}')
        if size1 != 0:
            percent_difference = round((difference_in_bytes / size1) * 100, 2)
            print(f'           * Difference in percentage: {percent_difference}%')

def compare_documents(collection_name, source_collection, target_collection):
    num_documents_source = source_collection.get_num_documents()
    num_documents_target = target_collection.get_num_documents()

    diff_docs = num_documents_source - num_documents_target
    print(f'         - {collection_name}: {Colors.WARNING if diff_docs != 0 else Colors.OKGREEN}\
{num_documents_source} / {num_documents_target} [{diff_docs}]{Colors.ENDC}')

def compare_indexes(source_collection, target_collection):
    source_indexes = set(source_collection.get_indexes())
    target_indexes = set(target_collection.get_indexes())

    indexes_not_in_both = source_indexes.symmetric_difference(target_indexes)
    indexes_not_in_source = indexes_not_in_both.intersection(target_indexes)
    indexes_not_in_target = indexes_not_in_both.intersection(source_indexes)

    if indexes_not_in_source:
        print(f'           * Indexes not in source:', Colors.WARNING, indexes_not_in_source, Colors.ENDC)

    if indexes_not_in_target:
        print(f'           * Indexes not in target:', Colors.WARNING, indexes_not_in_target, Colors.ENDC)

def read_json_file(infile):
    with open(infile, 'rt') as f:
        return json.loads(f.read())

def example_config():
    example = dict()
    migration1 = dict()
    migration1['cluster'] = "1-US-DEV (SOMETHING)"
    migration1['source'] = "mongodb+srv://mongodb-source1..."
    migration1['target'] = "mongodb://cosmosdb-target1..."
    migration1['databases'] = []
    migration1['collections'] = []
    example['migration1'] = migration1
    migration2 = dict()
    migration1['cluster'] = "1-US-UAT (SOMETHING)"
    migration2['source'] = "mongodb+srv://mongodb-source2..."
    migration2['target'] = "mongodb://cosmosdb-target2..."
    migration2['databases'] = ['customers', 'sales']
    migration2['collections'] = []
    migration2['cluster'] = '99-DK-UAT (DANISH-STOUT)'
    example['migration2'] = migration2
    return example

if __name__ == '__main__':
    if len(sys.argv) > 1:
        config_key = sys.argv[1]
        print('config_key specified: {}'.format(config_key))
        # file verify.json is git-ignored; create your own based on verify-example.json
        # print(json.dumps(example_config(), sort_keys=False, indent=2))

        config_file = 'verify.json'
        if os.path.isfile(config_file):
            print('config file exists: {}'.format(config_file))
            config = read_json_file('verify.json')
            if config_key in config.keys():
                migration_obj = config[config_key]
                print(json.dumps(migration_obj, sort_keys=False, indent=2))
                source_connection_string = migration_obj['source']
                target_connection_string = migration_obj['target']
                source_instance = MongoDBInstance(source_connection_string)
                target_instance = MongoDBInstance(target_connection_string)
                specified_databases = migration_obj['databases']
                specified_collections = migration_obj['collections']
                print('specified_databases:   {}'.format(specified_databases))
                print('specified_collections: {}'.format(specified_collections))
                compare_instances(source_instance, target_instance, specified_databases, specified_collections)
            else:
                print('error, file {} does not contain key: {}'.format(config_file, config_key))
        else:
            print('error, file {} does not exist.  it should look like the following:'.format(config_file))
            print(json.dumps(example_config(), sort_keys=False, indent=2))
            print('')
            print('terminating')
    else:
        print('error, no command-line config-key specified.  terminating.')
        print('usage: python verify.py <key>    # where <key> is a key in the verify.json dict')
