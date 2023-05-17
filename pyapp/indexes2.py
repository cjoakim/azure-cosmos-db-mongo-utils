"""
NOTE: indexes2.py is a new/enhanced version of indexes.py, currently under development.

Usage:
  Command-Line Format:
    python indexes2.py compare <key> <dbname> <collection>    <-- <key> is a key in your verify.json file
    python indexes2.py compare_captured <key> <dbname> <collection> --infile <previous-capture-file>

  Examples:
    python indexes2.py compare brown all all          <--- all databases, all collections
    python indexes2.py compare brown NFA all          <--- all collections in the NFA database
    python indexes2.py compare brown NFA requesters   <--- just the requesters collection in the NFA database

    python indexes2.py compare_captured brown NFA all --infile tmp/indexes_brown_all_all_20230517-1915.json

  Notes:
    1) <key> is a key in the verify.json dictionary
"""

# Enhancement List:
# 1) merge the capture() and compress() into one operation
# 2) omit the admin, config, and local databases
# 3) normalize expireAtSeconds to an int before json compares
# 4) colorized output

# Non-Enhancement List:
# 1) retail verify.json as it is easy to edit and keys are useful

import copy
import json
import os.path
import sys
import traceback

import arrow
from docopt import docopt

from pysrc.constants import Colors
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

def compare():
    combined_dict = capture_source_target_index_info()
    if combined_dict != None:
        compare_captured(combined_dict)

def capture_source_target_index_info():
    if len(sys.argv) > 3:
        config_key, cli_dbname, cli_cname = sys.argv[2], sys.argv[3], sys.argv[4]
        print('command-line args:')
        print('  config_key: {}'.format(config_key))
        print('  cli_dbname: {}'.format(cli_dbname))
        print('  cli_cname:  {}'.format(cli_cname))
        combined_dict = None  # the return object to this method

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
    return combined_dict

def compare_captured(combined_dict=None):
    try:
        if len(sys.argv) > 3:
            config_key, cli_dbname, cli_cname = sys.argv[2], sys.argv[3], sys.argv[4]
        for idx, arg in enumerate(sys.argv):
            if arg == '--infile':
                infile = sys.argv[idx + 1]
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
    source_coll_indexes, target_coll_indexes = None, None
    db_key = db_coll_key.split('|')[0]
    coll_key = db_coll_key.split('|')[1]

    if db_coll_key in source_db_colls.keys():
        source_coll_indexes = source_db_colls[db_coll_key]
    if db_coll_key in target_db_colls.keys():
        target_coll_indexes = target_db_colls[db_coll_key]

    print(f'{Colors.WHITE}=== comparing db: {db_key} coll: {coll_key} {Colors.ENDC}')

    if source_coll_indexes == None:
        if target_coll_indexes == None:
            #print('db_coll_key {} is in neither source nor target'.format(db_coll_key))
            print(f'{Colors.YELLOW}    db: {db_key} coll: {coll_key} - is in neither source nor target {Colors.ENDC}')
            return

    if source_coll_indexes != None:
        if target_coll_indexes == None:
            #print('db_coll_key {} is in the source but not the target'.format(db_coll_key))
            print(f'{Colors.YELLOW}    db: {db_key} coll: {coll_key} - is in the source but not the target {Colors.ENDC}')
            return

    if source_coll_indexes == None:
        if target_coll_indexes != None:
            #print('db_coll_key {} is in the target but not the source'.format(db_coll_key))
            print(f'{Colors.YELLOW}    db: {db_key} coll: {coll_key} - is in the target but not the source {Colors.ENDC}')
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
    print('aggregated index names in source and target coll: {}'.format(sorted_index_names))

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
            # normalize the v: attribute by removing it
            if 'v' in source_idx.keys():
                source_idx['v'] = 'normalized for compare'
            if 'v' in target_idx.keys():
                target_idx['v'] = 'normalized for compare'

            source_json = json.dumps(source_idx, sort_keys=True)
            target_json = json.dumps(target_idx, sort_keys=True)
            print('source:  {}'.format(source_json))
            print('target:  {}'.format(target_json))

            matched = (source_json == target_json)
            if matched:
                print(f'{Colors.OKGREEN}matched: {matched} {Colors.ENDC}')
            else:
                print(f'{Colors.FAIL}matched: {matched} {Colors.ENDC}')
            print('---')

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
    print('')
    print('collecting indexes for key: {} {} db: {} coll: {} {}'.format(
        config_key, source_or_target, cli_dbname, cli_cname, conn_string))
    try:
        opts = dict()
        opts['conn_string'] = conn_string
        opts['verbose'] = False
        m = Mongo(opts)

        for dbname in sorted(filter_dbnames(m.list_databases())):
            if names_match(cli_dbname, dbname):
                db_dict = dict()
                raw_data_dict[dbname] = db_dict
                if verbose():
                    print('{} dbname match: {}'.format(source_or_target, dbname))
                m.set_db(dbname)
                for cname in m.list_collections():
                    if names_match(cli_cname, cname):
                        db_dict[cname] = m.get_coll_indexes(cname)
                        if verbose():
                            print('{} dbname/coll match: {} {}'.format(source_or_target, dbname, cname))

    except Exception as e:
        print(str(e))
        print(traceback.format_exc())
    return raw_data_dict

def filter_dbnames(dbnames):
    for exclude_dbname in 'admin,local,config'.split(','):
        if exclude_dbname in dbnames:
            dbnames.remove(exclude_dbname)
    return dbnames

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
        for idx, arg in enumerate(sys.argv):
            if arg == '--infile':
                infile = sys.argv[idx + 1]
                combined_dict = FS.read_json(infile)

        if func == 'compare':
            compare()
        elif func == 'compare_captured':
            compare_captured()
        else:
            print_options('Error: invalid command-line function: {}'.format(func))



