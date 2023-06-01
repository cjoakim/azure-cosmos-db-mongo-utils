"""
vcore.py is used to migrate the databases, collections, and indexes
from a source Mongo database to a Cosmos DB Mongo/vCore account.

Usage:
  Command-Line Format:
    python vcore.py extract_from_mongo <verify-json_key>
    python vcore.py create_in_vcore <verify-json_key>
  Examples:
    python vcore.py extract_from_mongo bw2vcore
"""

import json
import sys
import traceback

import arrow

from docopt import docopt

from pysrc.counter import Counter
from pysrc.fs import FS
from pysrc.mongo import Mongo


def print_options(msg: str):
    print(msg)
    arguments = docopt(__doc__, version='1')
    print(arguments)

def read_json_file(infile: str):
    with open(infile, 'rt') as f:
        return json.loads(f.read())

def extract_from_mongo(config_key: str, run_config: dict):
    try:
        print('beginning of extract_from_mongo()')
        print('config_key:  {}'.format(config_key))
        print('run_config:  {}'.format(run_config))
        conn_string = run_config['source']
        print('conn_string: {}'.format(conn_string))

        outfile = extract_filename(config_key)
        extract_data = dict()  # db|collection is key, indexes dict is the value

        opts = dict()
        opts['conn_string'] = conn_string
        opts['verbose'] = False
        m = Mongo(opts)

        for dbname in sorted(filter_dbnames(m.list_databases())):
            m.set_db(dbname)
            for cname in m.list_collections():
                print('extracting metadata for db: {} cname: {}'.format(dbname, cname))
                try:
                    db_coll_key = '{}|{}'.format(dbname, cname)
                    extract_data[db_coll_key] = {'pending': True}
                    extract_data[db_coll_key] = m.get_coll_indexes(cname)
                except Exception as e:
                    print(str(e))
                    print(traceback.format_exc())

    except KeyboardInterrupt:
        print('terminating per KeyboardInterrupt')
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())

    FS.write_json(extract_data, outfile)

def extract_filename(config_key: str):
    return 'tmp/vcore_extract_{}.json'.format(config_key)

def filter_dbnames(dbnames: list):
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    return dbnames

def create_in_vcore(config_key: str, run_config: dict):
    print('create_in_vcore() is not yet implemented')

def curr_timestamp():
    return arrow.utcnow().format('YYYYMMDD-HHmm')

def verbose():
    for arg in sys.argv:
        if arg == '-v':
            return True
    return False

def very_verbose():
    for arg in sys.argv:
        if arg == '-v':
            return True
        if arg == '-vv':
            return True
    return False


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print_options('Error: missing command-line args')
    else:
        func = sys.argv[1].lower()
        config_key = sys.argv[2].lower()
        config = read_json_file('verify.json')
        if config_key in config.keys():
            run_config = config[config_key]
            if func == 'extract_from_mongo':
                extract_from_mongo(config_key, run_config)
            elif func == 'create_in_vcore':
                create_in_vcore(config_key, run_config)
            else:
                print_options('Error: invalid command-line function: {}'.format(func))
        else:
            print("key '{}' is not in file verify.json".format(config_key))
