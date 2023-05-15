"""
NOTE: indexes2.py is a new/enhanced version of indexes.py, currently under development.

Usage:
  Command-Line Format: 
    python indexes2.py capture key dbname collection
    python indexes2.py compare capture-file dbname collection
  Examples:
    python indexes2.py capture brown all all
    python indexes2.py capture brown NFA all
    python indexes2.py capture brown NFA requesters
    python indexes2.py compare tmp/indexes_brown_all_all_20230515-1734.json NFA requesters
  Notes:
    1) <key> is a key in the verify.json dictionary
"""

# Enhancement List:
# 1) <key> <db> all   <-- report on all indexes on all containers
# 2) <key> all all    <-- report on all indexes on all containers in all databases
# 3) sort the index results data
# 4) diff the structure of each index

import copy
import json
import os.path
import sys
import traceback

import arrow
from docopt import docopt

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
        config_key, cli_dbname, cli_cname = sys.argv[2], sys.argv[3], sys.argv[4]
        print('command-line args:')
        print('  config_key: {}'.format(config_key))
        print('  cli_dbname: {}'.format(cli_dbname))
        print('  cli_cname:  {}'.format(cli_cname))

        config_file = 'verify.json'
        if os.path.isfile(config_file):
            print('config file exists: {}'.format(config_file))
            config = read_json_file('verify.json')
            if config_key in config.keys():
                migration_obj = config[config_key]
                source_conn_string = migration_obj['source']
                target_conn_string = migration_obj['target']
                print('  source conn_str: {}'.format(source_conn_string))
                print('  target conn_str: {}'.format(target_conn_string))

                combined_dict = dict()
                combined_dict['source'] = collect_indexes(
                    config_key, 'source', source_conn_string, cli_dbname, cli_cname)
                combined_dict['target'] = collect_indexes(
                    config_key, 'target', target_conn_string, cli_dbname, cli_cname)

                outfile = 'tmp/indexes_{}_{}_{}_{}.json'.format(
                    config_key, cli_dbname, cli_cname, curr_timestamp())
                FS.write_json(combined_dict, outfile)
            else:
                print_options('error: {} is not a key in JSON file {}'.format(config_key, config_file))
        else:
            print_options('error: file {} does not exist'.format(config_file))
    else:
        print_options('Error: invalid command line')

def compare():
    if len(sys.argv) > 3:
        try:
            infile, cli_dbname, cli_cname = sys.argv[2], sys.argv[3], sys.argv[4]
            print('command-line args; dbname: {}, cname: {}, infile: {}'.format(cli_dbname, cli_cname, infile))

            # Read the input file produced by the capture() process
            print('reading infile {}'.format(infile))
            combined_dict = FS.read_json(infile)
            print('infile source keys count: {}'.format(len(combined_dict['source'])))
            print('infile target keys count: {}'.format(len(combined_dict['target'])))

            print('filtering input to just the databases and containers specified on the command-line ...')
            filtered_db_coll_dict = dict()
            filtered_db_coll_dict['source'] = filtered_captured(combined_dict, 'source', cli_dbname, cli_cname)
            filtered_db_coll_dict['target'] = filtered_captured(combined_dict, 'target', cli_dbname, cli_cname)
            print('filtered_db_coll_dict source key count:  {}'.format(len(filtered_db_coll_dict['source'])))
            if very_verbose():
                print('filtered_db_coll_dict source key values: {}'.format(sorted(filtered_db_coll_dict['source'].keys())))
            print('filtered_db_coll_dict target key count:  {}'.format(len(filtered_db_coll_dict['target'])))
            if very_verbose():
                print('filtered_db_coll_dict target key values: {}'.format(sorted(filtered_db_coll_dict['target'].keys())))

            print('collecting the aggregated set of db/coll names ...')
            agg_db_coll_names = dict()
            for db_coll_key in filtered_db_coll_dict['source']:
                agg_db_coll_names[db_coll_key] = ''
            for db_coll_key in filtered_db_coll_dict['target']:
                agg_db_coll_names[db_coll_key] = ''
            sorted_agg_db_coll_names = sorted(agg_db_coll_names.keys())
            if very_verbose():
                print('sorted_agg_db_coll_names: {}'.format(sorted_agg_db_coll_names))

            for db_coll_key in sorted_agg_db_coll_names:
                compare_db_coll_indexes(filtered_db_coll_dict, db_coll_key)

            outfile = 'tmp/indexes_filtered_db_coll_dict.json'
            FS.write_json(filtered_db_coll_dict, outfile)
        except Exception as e2:
            print(str(e2))
            print(traceback.format_exc())
    else:
        print_options('Error: invalid command line')

def filtered_captured(combined_dict, source_or_target, cli_dbname, cli_cname):
    # Filter the input combined_dict to just the databases and containers specified on the command-line
    filtered_dict = dict()
    source_or_target_dict = combined_dict[source_or_target]
    dbnames = sorted(source_or_target_dict.keys())
    for dbname in dbnames:
        if names_match(cli_dbname, dbname):
            db_colls_dict = source_or_target_dict[dbname]
            for cname in sorted(db_colls_dict.keys()):
                if names_match(cli_cname, cname):
                    key2 = '{}|{}'.format(dbname, cname)
                    key3 = '{}|{}|{}'.format(source_or_target, dbname, cname)
                    coll_data = dict()
                    coll_data['source'] = source_or_target
                    coll_data['source_db_coll_key'] = key3
                    coll_data['db_coll_key'] = key2
                    coll_data['indexes'] = copy.deepcopy(db_colls_dict[cname])
                    filtered_dict[key2] = coll_data
    return filtered_dict

def compare_db_coll_indexes(filtered_db_coll_dict, db_coll_key):
    source_db_colls = filtered_db_coll_dict['source']
    target_db_colls = filtered_db_coll_dict['target']
    source_db_coll_indexes, target_db_coll_indexes = None, None

    if db_coll_key in source_db_colls.keys():
        source_coll_indexes = source_db_colls[db_coll_key]
    if db_coll_key in target_db_colls.keys():
        target_coll_indexes = target_db_colls[db_coll_key]

    if source_coll_indexes == None:
        if target_coll_indexes == None:
            print('db_coll_key {} is in neither source nor target'.format(db_coll_key))
            return

    if source_coll_indexes != None:
        if target_coll_indexes == None:
            print('db_coll_key {} is in the source but not the target'.format(db_coll_key))
            return

    if source_coll_indexes == None:
        if target_coll_indexes != None:
            print('db_coll_key {} is in the target but not the source'.format(db_coll_key))
            return

    # At this point the db/collection combo is in both source and target; compare their indexes.
    # Collect the aggregated set of their index names for the db/collection.

    agg_index_names = dict()
    source_coll_indexes = source_coll_indexes['indexes']
    target_coll_indexes = target_coll_indexes['indexes']
    for idx_name in source_coll_indexes.keys():
        agg_index_names[idx_name] = ''
    for idx_name in target_coll_indexes.keys():
        agg_index_names[idx_name] = ''
    sorted_index_names = sorted(agg_index_names.keys())

    if very_verbose():
        print('--- source indexes for {}'.format(db_coll_key))
        print(json.dumps(source_coll_indexes, sort_keys=False, indent=2))
        print('--- target indexes for {}'.format(db_coll_key))
        print(json.dumps(target_coll_indexes, sort_keys=False, indent=2))
        print('--- combined index names: {}'.format(sorted_index_names))

    for idx_name in sorted_index_names:
        source_idx, target_idx = None, None
        if idx_name in source_coll_indexes.keys():
            source_idx = source_coll_indexes[idx_name]
        if idx_name in target_coll_indexes.keys():
            target_idx = target_coll_indexes[idx_name]

        if source_idx == None:
            print('source index {} not present for db/coll {}'.format(idx_name, db_coll_key))
        elif target_idx == None:
            print('target index {} not present for db/coll {}'.format(idx_name, db_coll_key))
        else:
            source_json = json.dumps(target_coll_indexes, sort_keys=True)
            target_json = json.dumps(target_coll_indexes, sort_keys=True)
            print('source:  {}'.format(source_json))
            print('target:  {}'.format(target_json))
            print('matched: {}'.format(source_json == target_json))

def collect_index_names(source_target_indexes, target_coll_indexes):
    # return a sorted list of strings - the names of all source and target indexes
    agg_index_names = dict()
    for idx_name in source_target_indexes['indexes'].keys():
        agg_index_names[idx_name] = ''
    for idx_name in target_coll_indexes['indexes'].keys():
        agg_index_names[idx_name] = ''
    return sorted(agg_index_names.keys())

def names_match(cli_name, found_name):
    if cli_name.lower() == 'all':
        return True
    return cli_name == found_name

def collect_indexes(config_key, source_or_target, conn_string, cli_dbname, cli_cname):
    raw_data_dict = dict()  # return object
    print('collect_indexes {} {} {} {} {}'.format(
        config_key, source_or_target, cli_dbname, cli_cname, conn_string))
    try:
        opts = dict()
        opts['conn_string'] = conn_string
        opts['verbose'] = False
        m = Mongo(opts)

        for dbname in m.list_databases():
            if names_match(cli_dbname, dbname):
                db_dict = dict()
                raw_data_dict[dbname] = db_dict
                if verbose():
                    print('{} dbname match: {}'.format(source_or_target, dbname))
                m.set_db(dbname)
                for cname in m.list_collections():
                    if names_match(cli_cname, cname):
                        print('{} dbname/coll match: {} {}'.format(source_or_target, dbname, cname))
                        db_dict[cname] = m.get_coll_indexes(cname)
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())
    return raw_data_dict

def get_sorted_db_names(mongo_client):
    dbnames = mongo_client.list_database_names()
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    return sorted(dbnames)

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



