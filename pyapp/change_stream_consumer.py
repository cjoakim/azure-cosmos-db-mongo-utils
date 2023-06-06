"""
Usage:
  python main.py <func>
  python change_stream_consumer.py consume <db-name> <collection-name> <connection-string> 
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# Chris Joakim, Microsoft

import json
import os
import sys

import pymongo
from pymongo import MongoClient

from bson.json_util import dumps
from docopt import docopt

def print_options(msg):
    print(msg)
    arguments = docopt(__doc__, version='1.0.0')
    print(arguments)

def consume(dbname, cname, conn_str):
    try:
        print('consuming change stream for db: {}, container: {} at {}'.format(
            dbname, cname, conn_str))

        # connect to the Cosmos DB Account, Database, and Container
        mongo_client = MongoClient(conn_str)
        print("-\n{}".format(mongo_client))
        db = mongo_client[dbname]
        print("-\n{}".format(db))
        coll = db[cname]
        print("-\n{}".format(coll))

        # define the pipeline of changes to receive
        pipeline = [
            { '$match': { "operationType": { '$in': ["insert", "update", "replace"] } } },
            { '$project': { "_id": 1, "fullDocument": 1, "ns": 1, "documentKey": 1 } }
        ]

        print('starting to watch the change stream ...')
        for change in db.watch(pipeline=pipeline, full_document='updateLookup'):
            print('---')
            print("change event:\n{}".format(change))
            print("ns:\n{}".format(change['ns']))
            print("documentKey:\n{}".format(change['documentKey']))
            print("fullDocument:\n{}".format(change['fullDocument']))
    except KeyboardInterrupt as ki:
        print('KeyboardInterrupt received; exiting')
        return


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cli_func = sys.argv[1].lower()
        if cli_func == 'consume':
            dbname   = sys.argv[2]
            cname    = sys.argv[3]
            conn_str = sys.argv[4]
            consume(dbname, cname, conn_str)
        else:
            print_options('Error: invalid command-line function: {}'.format(cli_func))
    else:
        print_options('Error: no command-line args provided') 
