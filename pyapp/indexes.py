"""
Usage:
  Command-Line Format: 
    python indexes.py key dbname collection  
    python indexes.py key dbname collection --list-dbs-and-colls
  Examples:
    python indexes.py brown x x --list-dbs-and-colls
    python indexes.py brown NFA requesters
    python indexes.py brown Vehicle mongockLock
  Notes:
    1) <key> is a key in the verify.json dictionary
"""

# Enhancement List:
# 1) <key> <db> all   <-- report on all indexes on all containers
# 2) sort the index results data 
# 3) diff the structure of each index 

import json
import os.path
import sys
import traceback

from docopt import docopt

from pysrc.fs import FS
from pysrc.mongo import Mongo, MongoDBInstance, MongoDBDatabase, MongoDBCollection

def print_options(msg):
    print(msg)
    # arguments = docopt(__doc__, version='1')
    # print(arguments)

def read_json_file(infile):
    with open(infile, 'rt') as f:
        return json.loads(f.read())

def get_indexes(config_key, source_or_target, connection_string, dbname, cname):
    indexes = None
    print('get_indexes {} {} {} {} {}'.format(
        config_key, source_or_target, dbname, cname, connection_string))
    try:
        client_instance = MongoDBInstance(connection_string)
        db = client_instance.get_database(dbname)
        coll = db.get_collection(cname)
        indexes = coll.get_indexes()
        if indexes != None:
            print('---')
            print(source_or_target)
            print(json.dumps(indexes, sort_keys=False, indent=2))
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())
    return indexes


if __name__ == "__main__":
    if len(sys.argv) > 3:
        config_key, dbname, cname = sys.argv[1], sys.argv[2], sys.argv[3]
        print('command-line args:')
        print('  config_key:      {}'.format(config_key))
        print('  dbname:          {}'.format(dbname))
        print('  cname:           {}'.format(cname))

        config_file = 'verify.json'
        if os.path.isfile(config_file):
            print('config file exists: {}'.format(config_file))
            config = read_json_file('verify.json')
            if config_key in config.keys():
                migration_obj = config[config_key]
                source_connection_string = migration_obj['source']
                target_connection_string = migration_obj['target']
                print('  source conn_str: {}'.format(source_connection_string))
                print('  target conn_str: {}'.format(target_connection_string))

                source_indexes = get_indexes(
                    config_key, 'source', source_connection_string, dbname, cname)
                target_indexes = get_indexes(
                    config_key, 'target', target_connection_string, dbname, cname)

                if source_indexes != None:
                    outfile = 'tmp/indexes_py_indexes_{}_source_{}_{}.json'.format(
                        config_key, dbname, cname)
                    FS.write_json(source_indexes, outfile)

                if target_indexes != None:
                    outfile = 'tmp/indexes_py_indexes_{}_target_{}_{}.json'.format(
                        config_key, dbname, cname)
                    FS.write_json(target_indexes, outfile)
            else:
                print_options('error: {} is not a key in JSON file {}'.format(config_key, config_file))
        else:
            print_options('error: file {} does not exist'.format(config_file))
    else:
        print_options('Error: invalid command line')
