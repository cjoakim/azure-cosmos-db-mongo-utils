"""
vcore.py is used to migrate the databases, collections, and indexes
from a source Mongo database to a Cosmos DB Mongo/vCore account.

Usage:
  Command-Line Format:
    python vcore.py extract <verify-json_key>
    python vcore.py create <verify-json_key>
  Examples:
    python vcore.py extract bw2vcore
"""

import json
import sys
import traceback

import arrow

from docopt import docopt

from pysrc.counter import Counter
from pysrc.fs import FS
from pysrc.mongo import Mongo


def print_options(msg):
    print(msg)
    arguments = docopt(__doc__, version='1')
    print(arguments)

def read_json_file(infile):
    with open(infile, 'rt') as f:
        return json.loads(f.read())

def extract(config_key, run_config):
    try:
        print('beginning of extract()')
        print('config_key:  {}'.format(config_key))
        print('run_config:  {}'.format(run_config))
        conn_string = run_config['source']
        print('conn_string: {}'.format(conn_string))

        outfile = extract_filename(config_key)
        extract_data = dict()

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

def extract_filename(config_key):
    return 'tmp/vcore_extract_{}.json'.format(config_key)

def filter_dbnames(dbnames):
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    return dbnames

def create(config_key, run_config):
    print('create() is not yet implemented')

def analyze(config_key, run_config):
    print('analyze() is not yet implemented')
    # counter = Counter()
    # files = FS.walk('tmp/indexes')
    # unique_indexes = dict()
    # for file in files:
    #     infile = file['full']
    #     if infile.endswith('.json'):
    #         print('processing file: {}'.format(infile))
    #         cluster_data = FS.read_json(infile)
    #         cluster_indexes = cluster_data['indexes']
    #         db_cname_keys = sorted(cluster_indexes.keys())
    #         for db_cname_key in db_cname_keys:
    #             # db_cname_key is in 'dbname|cname' format
    #             coll_index_data = cluster_indexes[db_cname_key]
    #             coll_index_names = coll_index_data.keys()
    #             for coll_index_name in sorted(coll_index_names):
    #                 index = coll_index_data[coll_index_name]
    #                 index['v'] = '0'
    #                 index['ns'] = 'normalized'
    #                 del index['v']
    #                 del index['ns']
    #                 jstr = json.dumps(index, sort_keys=True)
    #                 print(jstr)
    #                 counter.increment(jstr)
    #
    # FS.write_json(counter.get_data(), 'tmp/unique_indexes.json')

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
            if func == 'extract':
                # python indexesx.py extract tmp/tokens.txt
                extract(config_key, run_config)
            elif func == 'analyze':
                analyze(config_key, run_config)
            elif func == 'create':
                create(config_key, run_config)
            else:
                print_options('Error: invalid command-line function: {}'.format(func))
        else:
            print("key '{}' is not in file verify.json".format(config_key))
