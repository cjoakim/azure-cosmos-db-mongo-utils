"""
vcore.py is used to migrate the databases, collections, and indexes
from a source Mongo database to a Cosmos DB Mongo/vCore account.

Usage:
  Command-Line Format:
    python vcore.py extract_from_mongo <verify-json_key>
    python vcore.py create_in_vcore <verify-json_key>
  Examples:
    python vcore.py extract_from_mongo bw2vcore
    python vcore.py create_in_vcore bw2vcore
"""

# https://docs.python.org/3/library/typing.html

import json
import sys
import time
import traceback

import arrow

from docopt import docopt

from pysrc.counter import Counter
from pysrc.fs import FS
from pysrc.mongo import Mongo


def print_options(msg: str) -> None:
    print(msg)
    arguments = docopt(__doc__, version='1')
    print(arguments)

def read_json_file(infile: str) -> dict:
    with open(infile, 'rt') as f:
        return json.loads(f.read())

def extract_from_mongo(config_key: str, run_config: dict) -> None:
    try:
        print('beginning of extract_from_mongo()')
        print('config_key: {}'.format(config_key))
        print('run_config: {}'.format(run_config))

        outfile = extract_filename(config_key)
        extract_data = dict()  # db|collection is key, indexes dict is the value

        opts = dict()
        opts['conn_string'] = run_config['source']
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

def extract_filename(config_key: str) -> str:
    return 'tmp/vcore_extract_{}.json'.format(config_key)

def filter_dbnames(dbnames: list) -> list:
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    return dbnames

def create_in_vcore(config_key: str, run_config: dict) -> None:
    try:
        print('beginning of create_in_vcore()')
        print('config_key: {}'.format(config_key))
        print('run_config: {}'.format(run_config))

        infile = extract_filename(config_key)
        extract_data = read_json_file(infile)
        source_dbnames = collect_extract_db_names(extract_data)

        opts = dict()
        opts['conn_string'] = run_config['target']
        opts['verbose'] = False
        m = Mongo(opts)

        # First, create the databases if they don't already exist
        for source_dbname in source_dbnames:
            if array_match(run_config['databases'], source_dbname):
                print('migrating db: {} per verify.json config'.format(source_dbname))

                vcore_dbnames =  sorted(filter_dbnames(m.list_databases()))
                if source_dbname in vcore_dbnames:
                    print('database {} already exists in target'.format(source_dbname))
                else:
                    print('database {} does not exist in target; creating...'.format(source_dbname))
                    result = create_database(m, source_dbname, 10)
                    if result == True:
                        print('Ok - database created: {}'.format(source_dbname))
                    else:
                        print('Error - database created: {}'.format(source_dbname))
            else:
                print('not migrating db: {} per verify.json config'.format(source_dbname))

    except KeyboardInterrupt:
        print('terminating per KeyboardInterrupt')
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())

def collect_extract_db_names(extract_data: dict) -> list:
    dbnames = dict()
    for key in extract_data.keys():
        dbname = key.split('|')[0]
        dbnames[dbname] = ''
    return sorted(dbnames.keys())

def array_match(array, item) -> bool:
    if len(array) == 0:
        return True
    else:
        for name in array:
            if name == item:
                return True
    return False

def create_database(m: Mongo, dbname: str, max_attempts: int) -> bool:
    for attempt_num in range(1, max_attempts + 1):
        try:
            dbnames = m.list_databases()
            if dbname in dbnames:
                print('create_database - db exists: {}'.format(dbname))
                return True
            else:
                db = m.create_database(dbname)
                print(db)
                time.sleep(3)
                dbnames = m.list_databases()
                if dbname in dbnames:
                    return True
                else:
                    raise ValueError('create_database - dbname: {} is not in target account'.format(dbname))
        except:
            if attempt_num < max_attempts:
                print('create_database - unsuccessful attempt {}, giving up'.format(attempt_num))
            else:
                sleep_secs = attempt_num * 2
                print('create_database - unsuccessful attempt {}, sleeping for {} seconds'.format(attempt_num, sleep_secs))
                time.sleep(sleep_secs)
    return False

def curr_timestamp() -> str:
    return arrow.utcnow().format('YYYYMMDD-HHmm')

def verbose() -> bool:
    for arg in sys.argv:
        if arg == '-v':
            return True
    return False

def very_verbose() -> bool:
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
