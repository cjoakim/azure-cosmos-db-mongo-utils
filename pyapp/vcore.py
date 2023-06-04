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
        print('=== beginning of extract_from_mongo()')
        outfile = extract_filename(config_key)
        extract_data = dict()  # db|collection is key, indexes dict is the value

        opts = dict()
        opts['conn_string'] = run_config['source']
        opts['verbose'] = False
        m = Mongo(opts)

        extract_coll_shard_info(m)

        # shards = m.get_shards()
        # for shard in shards:
        #     print('shard: {}'.format(shard))

        for dbname in sorted(filter_dbnames(m.list_databases())):
            m.set_db(dbname)
            for cname in m.list_collections():
                print('extracting metadata for db: {} cname: {}'.format(dbname, cname))
                try:
                    db_coll_key = '{}|{}'.format(dbname, cname)
                    coll_data = {}
                    extract_data[db_coll_key] = coll_data
                    collstats = m.command_coll_stats(cname)
                    #print('collstats: {}'.format(collstats))
                    coll_data['indexes'] = m.get_coll_indexes(cname)
                    coll_data['stats']   = str(collstats)
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
        print('=== beginning of create_in_vcore()')
        infile = extract_filename(config_key)
        extract_data = read_json_file(infile)
        source_dbnames = extract_db_names(extract_data)

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
                    create_database(m, source_dbname, extract_data)
            else:
                print('not migrating db: {} per verify.json config'.format(source_dbname))

    except KeyboardInterrupt:
        print('terminating per KeyboardInterrupt')
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())

def extract_db_names(extract_data: dict) -> list:
    dbnames = dict()
    for key in extract_data.keys():
        dbname = key.split('|')[0]
        dbnames[dbname] = ''
    return sorted(dbnames.keys())

def extract_collection_count_for_db(extract_data: dict, dbname: str) -> list:
    count = 0
    key_prefix = '{}|'.format(dbname)
    for key in extract_data.keys():
        if key.startswith(key_prefix):
            count = count + 1
    return count

def extract_coll_shard_info(m: Mongo) -> None:
    coll_data = list()
    m.set_db("config")
    m.set_coll("collections")
    colls = m.find({})
    for coll in colls:
        print(str(type(coll)))
        for key in 'lastmodEpoch,lastmod,uuid'.split(','):
            if key in coll.keys():
                del coll[key]
        coll_data.append(coll)
        print("coll: {}".format(coll))
    FS.write_json(coll_data, 'tmp/colls.json')

def array_match(array, item) -> bool:
    if len(array) == 0:
        return True
    else:
        for name in array:
            if name == item:
                return True
    return False

def create_database(m: Mongo, dbname: str, extract_data: dict) -> bool:
    print("--- create_database; dbname: {}".format(dbname))
    db_key_prefix = '{}|'.format(dbname)
    max_attempts = 10

    for attempt_num in range(1, max_attempts):
        try:
            dbnames = m.list_databases()
            if dbname in dbnames:
                print('create_database - db exists: {}'.format(dbname))
                return True
            else:
                db = m.create_database(dbname)
                m.set_db(dbname)

                commands_response = m.list_commands()
                print('commands_response: {}'.format(commands_response))
                for cmd_name in sorted(commands_response['commands'].keys()):
                    print('---')
                    print(cmd_name)
                    print(commands_response['commands'][cmd_name])

                extract_collection_count = extract_collection_count_for_db(extract_data, dbname)

                if extract_collection_count < 1:
                    # this new db in pymongo is created only after a write to it
                    db.scaffolding.insert_one({"time": time.time()})  # insert a dummy doc in a dummy container
                    time.sleep(1.0)
                    m.delete_container['scaffolding']  # delete the dummy container, the db will still exist
                    time.sleep(1.0)
                else:
                    for db_coll_key in extract_data.keys():
                        if db_coll_key.startswith(db_key_prefix):
                            cname = db_coll_key.split('|')[1]
                            extract_indexes = extract_data[db_coll_key]
                            c = create_collection(m, dbname, cname, extract_data[db_coll_key])




                dbnames = m.list_databases()
                print('create_database - dbnames after create_database: {}'.format(dbnames))
                if dbname in dbnames:
                    return True
                else:
                    raise ValueError('db not present')
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
            if attempt_num > max_attempts:
                print('create_database - unsuccessful attempt {}, giving up'.format(attempt_num))
            else:
                sleep_secs = attempt_num * 2
                print('create_database - unsuccessful attempt {}, sleeping for {} seconds'.format(attempt_num, sleep_secs))
                time.sleep(sleep_secs)
    return False

def create_collection(m: Mongo, dbname: str, cname: str, collection_data: dict):
    print('create_collection - dbname: {} cname: {}'.format(dbname, cname))
    m.set_db(dbname)
    return m.create_coll(cname)


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
            print('run_config: {}'.format(run_config))

            if func == 'extract_from_mongo':
                extract_from_mongo(config_key, run_config)
            elif func == 'create_in_vcore':
                create_in_vcore(config_key, run_config)
            else:
                print_options('Error: invalid command-line function: {}'.format(func))
        else:
            print("key '{}' is not in file verify.json".format(config_key))
