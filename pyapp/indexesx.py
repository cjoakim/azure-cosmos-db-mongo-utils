"""
NOTE: indexesx.py is used to extract the indexes from given cluster urls

Usage:
  Command-Line Format:
    python indexesx.py extract <txt-infile-with-connection-strings>

  Examples:
    python indexesx.py extract tmp/conn_strings.txt > tmp/indexesx.log
"""

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

def extract(infile):
    lines, conn_strings = FS.read_lines(infile), list()
    for line in lines:
        stripped = line.strip()
        if len(stripped) > 20:
            conn_strings.append(stripped)
    for idx, conn_string in enumerate(sorted(conn_strings)):
        try:
            extract_indexes_in_cluster(idx, conn_string)
        except Exception as e:
            print('Exception on cluster {} {}'.format(idx, conn_string))
            print(str(e))
            print(traceback.format_exc())
    print('done')

def extract_indexes_in_cluster(idx, conn_string):
    if idx > 999999:
        return
    print('===')
    print('processing cluster idx: {} url: {}'.format(idx, conn_string))
    cluster_dict, cluster_indexes = dict(), dict()
    host = conn_string.split('@')[1] 
    cluster_dict['idx']  = idx 
    cluster_dict['host'] = host
    cluster_dict['indexes']  = cluster_indexes
    cluster_outfile = 'tmp/indexes/{}.json'.format(host.replace('.','-'))

    if True:
        opts = dict()
        opts['conn_string'] = conn_string
        opts['verbose'] = False
        m = Mongo(opts)

        for dbname in sorted(filter_dbnames(m.list_databases())):
            m.set_db(dbname)
            for cname in m.list_collections():
                print('db: {} cname: {} conn_string: {}'.format(dbname, cname, conn_string))
                try:
                    db_coll_key = '{}|{}'.format(dbname, cname)
                    cluster_indexes[db_coll_key] = {'pending': True}
                    cluster_indexes[db_coll_key] = m.get_coll_indexes(cname)
                except Exception as e:
                    print(str(e))
                    print(traceback.format_exc())

    FS.write_json(cluster_dict, cluster_outfile)

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

        if func == 'extract':
            # python indexesx.py extract tmp/tokens.txt
            extract(sys.argv[2])
        else:
            print_options('Error: invalid command-line function: {}'.format(func))
