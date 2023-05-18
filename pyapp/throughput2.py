"""
Usage:
  Command-Line Format:     
    python throughput.py <config-key> <action>
        <config-key> is a key in your verify.json file
        <action> is one of get_current_throughput, scale_down, etc. as shown below:
  Examples:
    python throughput.py xxx list_databases
    python throughput.py xxx list_databases_and_collections
    python throughput.py xxx get_current_throughput
    python throughput.py xxx scale_down
    python throughput.py xxx scale_down -v
  Notes:
    1)  see the instructions in verify.py regarding configuration of
        the Python virtual environment, and configuration file verify.json.
    2)  The default input file is 'current/psql/mma_collection_ru.csv',
        but this can be overridden with the --infile flag.
"""

# Enhancement List:
# 1) simpler lookup by cosmos host, sorted by db

# Developer Notes:
# https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/how-to-provision-throughput
# https://www.mongodb.com/docs/manual/reference/method/db.runCommand/
# https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/custom-commands

import json
import os.path
import sys
import traceback

from docopt import docopt

from pymongo import MongoClient
import certifi

from pysrc.env import Env
from pysrc.fs import FS

def read_json_file(infile):
    with open(infile, 'rt') as f:
        return json.loads(f.read())
def filter_collections(all_collections, migration_obj):
    mongo_conn_str  = migration_obj['source']
    cosmos_conn_str = migration_obj['target']
    mongo_host  = mongo_conn_str.split('@')[1]
    cosmos_host = cosmos_conn_str.split('@')[1].split(':')[0].split('.')[0]
    cluster     = migration_obj['cluster']
    databases   = migration_obj['databases']
    collections = migration_obj['collections']

    print('filter_collections, searching for cluster: "{}" in {} rows'.format(
        cluster, len(all_collections)))

    filtered = list()
    sibling_cluster = None

    for c in all_collections:
        if cluster == c.cluster:
            if c.cluster == c.mma_cluster:
                filtered.append(c)
            else:
                sibling_cluster = c.mma_cluster

    if len(filtered) < 1:
        if sibling_cluster != None:
            print('filter_collections, searching for sibling_cluster: {}'.format(sibling_cluster))
            for c in all_collections:
                if sibling_cluster == c.cluster:
                    filtered.append(c)

    if len(filtered) < 1:
        if len(all_collections) > 0:
            print(json.dumps(all_collections[0].as_dict(), sort_keys=False, indent=2))

    return filtered

def list_databases(client):
    dbnames = client.list_database_names()
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    for dbname in sorted(dbnames):
        print('database: {}'.format(dbname))

def list_databases_and_collections(client):
    dbnames = client.list_database_names()
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    for dbname in sorted(dbnames):
        db = client[dbname]
        collections = db.list_collection_names(filter={'type': 'collection'})
        for cname in sorted(collections):
            print('database: {}  collection: {}'.format(dbname, cname))

def get_current_throughput(client, migration_obj):
    # https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/custom-commands#get-collection
    dbnames = client.list_database_names()
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    for dbname in sorted(dbnames):
        if array_match(migration_obj['databases'], dbname):
            print("=== database: {}".format(dbname))
            db = client[dbname]
            db_throughput = get_database_throughput(db)
            if Env.verbose():
                print(json.dumps(db_throughput, sort_keys=False, indent=2))
            else:
                print(db_throughput['__summary__'])

            collections = db.list_collection_names(filter={'type': 'collection'})
            for cname in sorted(collections):
                if array_match(migration_obj['collections'], cname):
                    print("---collection {} in database: {}".format(cname, dbname))
                    coll_throughput = get_collection_throughput(db, cname)
                    if Env.verbose():
                        print(json.dumps(coll_throughput, sort_keys=False, indent=2))
                    else:
                        print(coll_throughput['__summary__'])
        else:
            print("found database {}, but it's not in the verify.json key specification".format(dbname))

def scale_throughput(client, migration_obj, filtered_collections, direction='down'):
    # https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/custom-commands#update-database

    if len(migration_obj['databases']) < 1:
        print('the array of databases in verify.json must not be empty for RU scaling.')
        print('run this program with the "list_databases" option to discover the database names.')
        return

    dbnames = client.list_database_names()

    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)

    for dbname in sorted(dbnames):
        if array_match(migration_obj['databases'], dbname):
            print("=== database: {}".format(dbname))
            db = client[dbname]
            db_throughput = get_database_throughput(db)
            if Env.verbose():
                print(json.dumps(db_throughput, sort_keys=False, indent=2))
            else:
                print(db_throughput['__summary__'])

            collections = db.list_collection_names(filter={'type': 'collection'})
            for cname in sorted(collections):
                db_coll_key = '{}|{}'.format(dbname, cname)
                if array_match(migration_obj['collections'], cname):
                    print("---collection {} in database: {}".format(cname, dbname))
                    coll_throughput = get_collection_throughput(db, cname)
                    if Env.verbose():
                        print(json.dumps(coll_throughput, sort_keys=False, indent=2))
                    else:
                        print(coll_throughput['__summary__'])

                    if False:
                        try:
                            command, max_autoscale = dict(), dict()
                            if direction.lower() == 'up':
                                max_autoscale['maxThroughput'] = int(c.est_migration_ru)
                            else:
                                max_autoscale['maxThroughput'] = int(c.est_post_migration_ru)
                            command['customAction'] = 'UpdateCollection'
                            command['collection'] = cname
                            command['autoScaleSettings'] = max_autoscale
                            result = db.command(command)
                            print(result)
                        except Exception as e:
                            print(traceback.format_exc())
        else:
            print("found cosmos database {}, but it's not in the MMA data".format(dbname))

def get_database_throughput(db):
    try:
        command = dict()
        command['customAction'] = 'GetDatabase'
        data = db.command(command)
        if 'autoScaleSettings' in data.keys():
            max = data['autoScaleSettings']['maxThroughput']
            data['__summary__'] = 'db_shared:{}'.format(max)
        else:
            data['__summary__'] = 'db_not_shared'
        return data
    except:
        return {}
    """
    GetDatabase response - not shared throuput
    {
      "database": "x",
      "ok": 1.0
    }
    
    GetDatabase response - with shared throuput
    {
      "database": "shared",
      "provisionedThroughput": 400,
      "autoScaleSettings": {
        "maxThroughput": 4000
      },
      "ok": 1.0
    }
    """

def get_collection_throughput(db, cname):
    try:
        command = dict()
        command['customAction'] = 'GetCollection'
        command['collection'] = cname
        data = db.command(command)
        if 'provisionedThroughput' in data.keys():
            if 'autoScaleSettings' in data.keys():
                max = data['autoScaleSettings']['maxThroughput']
                data['__summary__'] = 'container_autoscale:{}'.format(max)
            else:
                ru = data['provisionedThroughput']
                data['__summary__'] = 'container_manual:{}'.format(ru)
        else:
            data['__summary__'] = 'db_shared'
        return data
    except:
        return {}
    """ 
    GetCollection response - in a shared throughput database 
    {
      "database": "shared",
      "collection": "shared1",
      "shardKeyDefinition": {
        "pk": "hashed"
      },
      "maxContentLength": 2097152,
      "ok": 1.0
    }
    
    GetCollection response - container manual throughput
    {
      "database": "x",
      "collection": "manual500",
      "shardKeyDefinition": {
        "_id": "hashed"
      },
      "provisionedThroughput": 500,
      "maxContentLength": 2097152,
      "ok": 1.0
    }
    
    GetCollection response - container autoscale throughput
    {
      "database": "x",
      "collection": "autoscale4000",
      "shardKeyDefinition": {
        "_id": "hashed"
      },
      "provisionedThroughput": 400,
      "autoScaleSettings": {
        "maxThroughput": 4000
      },
      "maxContentLength": 2097152,
      "ok": 1.0
    }
    """

def array_match(array, item):
    if len(array) == 0:
        return True
    else:
        for name in array:
            if name == item:
                return True
    print('array_match, item: {} is not in array: {}'.format(item, array))
    return False

def print_options(msg):
    print(msg)
    # arguments = docopt(__doc__, version='0.1.0')
    # print(arguments)


if __name__ == "__main__":
    if len(sys.argv) > 2:
        config_key, action = sys.argv[1], sys.argv[2]
        print('command-line args:')
        print('  config_key:      {}'.format(config_key))
        print('  action:          {}'.format(action))

        config_file = 'verify.json'
        if os.path.isfile(config_file):
            print('config file exists: {}'.format(config_file))
            config = read_json_file('verify.json')
            if config_key in config.keys():
                migration_obj = config[config_key]
                print(migration_obj)
                mongo_conn_str = migration_obj['source']
                cosmos_conn_str = migration_obj['target']
                print('  cosmos_conn_str: {}'.format(cosmos_conn_str))
                print('  mongo_conn_str:  {}'.format(mongo_conn_str))

                client = MongoClient(cosmos_conn_str, tlsCAFile=certifi.where())

                if action == 'list_databases':
                    list_databases(client)
                elif action == 'list_databases_and_collections':
                    list_databases_and_collections(client)
                elif action == 'get_current_throughput':
                    get_current_throughput(client, migration_obj)
                elif action == 'scale_down':
                    scale_throughput(client, migration_obj, 'down')
                else:
                    print_options('error: undefined action specified on the command line - {}'.format(action))
            else:
                print_options('error: {} is not a key in JSON file {}'.format(config_key, config_file))
        else:
            print_options('error: file {} does not exist'.format(config_file))
    else:
        print_options('error: invalid command line')
