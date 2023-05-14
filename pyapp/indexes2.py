"""
NOTE: indexes2.py is a new/enhanced version of indexes.py, currently under development.

Usage:
  Command-Line Format: 
    python indexes2.py capture key dbname collection
    python indexes2.py compare capture-file dbname collection
  Examples:
    python indexes2.py brown all all
    python indexes2.py brown NFA all
    python indexes2.py brown NFA requesters
    python indexes2.py compare tmp/indexes_brown_all_all_20230514-1339.json NFA requesters
  Notes:
    1) <key> is a key in the verify.json dictionary
"""

# Enhancement List:
# 1) <key> <db> all   <-- report on all indexes on all containers
# 2) <key> all all    <-- report on all indexes on all containers in all databases
# 3) sort the index results data
# 4) diff the structure of each index

import json
import os.path
import sys
import traceback

import arrow
from docopt import docopt

from pymongo import MongoClient
import certifi

from pysrc.fs import FS
from pysrc.mongo import Mongo, MongoDBInstance, MongoDBDatabase, MongoDBCollection

def print_options(msg):
    print(msg)
    # TODO - docopt not working?
    # arguments = docopt(__doc__, version='1')
    # print(arguments)

def read_json_file(infile):
    with open(infile, 'rt') as f:
        return json.loads(f.read())

def capture():
    if len(sys.argv) > 3:
        config_key, dbname, cname = sys.argv[2], sys.argv[3], sys.argv[4]
        print('command-line args:')
        print('  config_key: {}'.format(config_key))
        print('  dbname:     {}'.format(dbname))
        print('  cname:      {}'.format(cname))

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

                combined_dict = dict()
                combined_dict['source'] = collect_indexes(
                    config_key, 'source', source_connection_string, dbname, cname)
                combined_dict['target'] = collect_indexes(
                    config_key, 'target', target_connection_string, dbname, cname)

                outfile = 'tmp/indexes_{}_{}_{}_{}.json'.format(
                    config_key, dbname, cname, curr_timestamp())
                FS.write_json(combined_dict, outfile)
            else:
                print_options('error: {} is not a key in JSON file {}'.format(config_key, config_file))
        else:
            print_options('error: file {} does not exist'.format(config_file))
    else:
        print_options('Error: invalid command line')


def compare():
    if len(sys.argv) > 3:
        infile, dbname, cname = sys.argv[2], sys.argv[3], sys.argv[4]
        print('command-line args:')
        print('  infile: {}'.format(infile))
        print('  dbname: {}'.format(dbname))
        print('  cname:  {}'.format(cname))
    else:
        print_options('Error: invalid command line')

def collect_indexes(config_key, source_or_target, connection_string, cli_dbname, cli_cname):
    raw_data_dict = dict()  # return object
    print('collect_indexes {} {} {} {} {}'.format(
        config_key, source_or_target, cli_dbname, cli_cname, connection_string))
    try:
        opts = dict()
        opts['conn_string'] = connection_string
        opts['verbose'] = False
        m = Mongo(opts)

        for dbname in m.list_databases():
            if matches(cli_dbname, dbname):
                db_dict = dict()
                raw_data_dict[dbname] = db_dict
                if verbose():
                    print('{} dbname match: {}'.format(source_or_target, dbname))
                m.set_db(dbname)
                for cname in m.list_collections():
                    if matches(cli_cname, cname):
                        print('{} dbname/coll match: {} {}'.format(source_or_target, dbname, cname))
                        db_dict[cname] = m.get_coll_indexes(cname)
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())
    return raw_data_dict

def compare_all_indexes(combined_dict):
    print('TODO - implement compare_all_indexes')


# coll = db.get_collection(cname)
# indexes = coll.get_indexes()
# if indexes != None:
#     print('---')
#     print(source_or_target)
#     print(json.dumps(indexes, sort_keys=False, indent=2))

def get_sorted_db_names(mongo_client):
    dbnames = mongo_client.list_database_names()
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    return sorted(dbnames)

def matches(cli_name, found_name):
    if cli_name.lower() == 'all':
        return True
    return cli_name == found_name

def curr_timestamp():
    return arrow.utcnow().format('YYYYMMDD-HHmm')

def verbose():
    for arg in sys.argv:
        if arg == '-v':
            return True
    return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_options('Error: no command-line args')
    else:
        func = sys.argv[1].lower()
        if func == 'capture':
            capture()
        elif func == 'compare':
            compare()
        else:
            print_options('Error: invalid command-line function: {}'.format(func))



