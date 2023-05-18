"""
Usage:
  Command-Line Format:     
    python throughput.py <config-key> <action> <optional-params>
        <config-key> is a key in your verify.json file
        <action> is one of list_databases, get_current_statem scale_down, etc. as shown below:
  Examples:
    python throughput.py xxx list_databases
    python throughput.py xxx list_databases_and_collections
    python throughput.py xxx get_current_state
    python throughput.py xxx scale_down -preview_only      <-- preview the changes only
    python throughput.py xxx scale_down -preview_only -v   <-- preview the changes, verbose
    python throughput.py xxx scale_down                    <-- actually scale down
"""

# Developer Notes:
# https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/how-to-provision-throughput
# https://www.mongodb.com/docs/manual/reference/method/db.runCommand/
# https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/custom-commands

import json
import math
import os.path
import sys
import traceback

from pymongo import MongoClient
import certifi

from pysrc.env import Env

def read_json_file(infile):
    with open(infile, 'rt') as f:
        return json.loads(f.read())

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

def get_current_state(client, migration_obj):
    # https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/custom-commands#get-collection
    dbnames = client.list_database_names()
    one_gb = 1024.0 * 1024.0 * 1024.0

    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    for dbname in sorted(dbnames):
        if array_match(migration_obj['databases'], dbname):
            print('')
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
                    coll_throughput = get_collection_throughput(db, cname)
                    stats = db.command("collStats", cname)
                    doc_count, size_bytes = stats['count'], stats['count']
                    gb = float(size_bytes) / one_gb
                    gbstr = "%.4f" % gb

                    print("---collection {} in database: {} docs: {} bytes: {} gb: {}".format(
                        cname, dbname, doc_count, size_bytes, gbstr))

                    if Env.verbose():
                        print(json.dumps(coll_throughput, sort_keys=False, indent=2))
                        print(json.dumps(stats, sort_keys=False, indent=2))
                    else:
                        print(coll_throughput['__summary__'])
        else:
            print("found database {}, but it's not in the verify.json key specification".format(dbname))

def scale_down(client, migration_obj):
    # https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/custom-commands#update-database

    if len(migration_obj['databases']) < 1:
        print('the array of databases in verify.json must not be empty for RU scaling.')
        print('run this program with the "list_databases" option to discover the database names.')
        return

    dbnames = client.list_database_names()
    one_gb = 1024.0 * 1024.0 * 1024.0

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
                    print("")
                    print("---processing collection {} in database: {}".format(cname, dbname))
                    coll_throughput = get_collection_throughput(db, cname)
                    summary_tokens = coll_throughput['__summary__'].split(':')  # "container_autoscale:10000"
                    provisioning_type = summary_tokens[0]
                    curr_ru_value = int(summary_tokens[1])
                    min_scale_down_ru_value = int(str(curr_ru_value)[:-1])  # 60000 -> 6000 or 1/10th

                    stats = db.command("collStats", cname)
                    doc_count, size_bytes = stats['count'], stats['count']
                    gb = float(size_bytes) / one_gb
                    gb_str = "%.4f" % gb

                    physical_partitions = int(math.ceil(gb / 50.0))
                    if physical_partitions < 1:
                        physical_partitions = 1
                    new_ru_value = int(physical_partitions * 1000)

                    if new_ru_value < min_scale_down_ru_value:
                        new_ru_value = min_scale_down_ru_value

                    print("---coll: {} in db: {} docs: {} gb: {} prov: {} pp: {} curr_ru: {} new_ru: {}".format(
                        cname, dbname, doc_count, gb_str, provisioning_type,
                        physical_partitions, curr_ru_value, new_ru_value))

                    if Env.verbose():
                        print(json.dumps(coll_throughput, sort_keys=False, indent=2))
                        print(json.dumps(stats, sort_keys=False, indent=2))
                    else:
                        print(coll_throughput['__summary__'])

                    if preview_only():
                        pass
                    else:
                        if provisioning_type != 'container_autoscale':
                            print('not updating throughput because container is not container_autoscale')
                        else:
                            if curr_ru_value == new_ru_value:
                                print('not updating throughput because container because RU value is correct')
                            else:
                                try:
                                    command, autoscaleObj = dict(), dict()
                                    autoscaleObj['maxThroughput'] = new_ru_value
                                    command['customAction'] = 'UpdateCollection'
                                    command['collection'] = cname
                                    command['autoScaleSettings'] = autoscaleObj
                                    result = db.command(command)
                                    print(result)
                                except Exception as e:
                                    print(traceback.format_exc())

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

def preview_only():
    for arg in sys.argv:
        if arg == '-preview_only':
            return True
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
                cosmos_conn_str = migration_obj['target']
                print('  cosmos_conn_str: {}'.format(cosmos_conn_str))
                client = MongoClient(cosmos_conn_str, tlsCAFile=certifi.where())

                if action == 'list_databases':
                    list_databases(client)
                elif action == 'list_databases_and_collections':
                    list_databases_and_collections(client)
                elif action == 'get_current_state':
                    get_current_state(client, migration_obj)
                elif action == 'scale_down':
                    scale_down(client, migration_obj)
                else:
                    print_options('error: undefined action specified on the command line - {}'.format(action))
            else:
                print_options('error: {} is not a key in JSON file {}'.format(config_key, config_file))
        else:
            print_options('error: file {} does not exist'.format(config_file))
    else:
        print_options('error: invalid command line')
