import certifi
import json
import sys
import traceback

from pymongo import MongoClient
from bson.objectid import ObjectId

# This class is used to access a MongoDB database, including the CosmosDB Mongo API.
# Chris Joakim, Microsoft, 2023
#
# Importing:
# from pysrc.mongo import Mongo, MongoDBInstance, MongoDBDatabase, MongoDBCollection

class Mongo(object):

    def __init__(self, opts):
        self._opts = opts
        self._db = None
        self._coll = None
        if 'conn_string' in self._opts.keys():
            if 'cosmos.azure.com' in opts['conn_string']:
                self._env = 'cosmos'
            else:
                self._env = 'mongo'
            self._client = MongoClient(opts['conn_string'], tlsCAFile=certifi.where())
        else:
            if 'cosmos.azure.com' in opts['host']:
                self._env = 'cosmos'
            else:
                self._env = 'mongo'
            self._client = MongoClient(opts['host'], opts['port'], tlsCAFile=certifi.where())

        if self.is_verbose():
            print(json.dumps(self._opts, sort_keys=False, indent=2))

    def is_verbose(self):
        if 'verbose' in self._opts.keys():
            return self._opts['verbose']
        return False

    def list_databases(self):
        try:
            return sorted(self._client.list_database_names())
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())

    def list_collections(self):
        return self._db.list_collection_names(filter={'type': 'collection'})

    def set_db(self, dbname):
        self._db = self._client[dbname]
        return self._db

    def set_coll(self, collname):
        try:
            self._coll = self._db[collname]
            return self._coll
        except Exception as e:
            # observed: collection names must not start or end with '.'
            print('Exception - env: {} - {}'.format(self._env, str(e)))
            return None

    def get_coll_indexes(self, collname):
        try:
            self.set_coll(collname)
            return self._coll.index_information()
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
            error_data = dict()
            error_data['get_coll_indexes_error'] = True
            error_data['db'] = self._db.name
            error_data['coll'] = collname
            error_data['msg'] = str(e)

    # crud below, meta above

    def insert_doc(self, doc):
        return self._coll.insert_one(doc)

    def find_one(self, query_spec):
        return self._coll.find_one(query_spec)

    def find(self, query_spec):
        return self._coll.find(query_spec)

    def find_by_id(self, id):
        return self._coll.find_one({'_id': ObjectId(id)})

    def delete_by_id(self, id):
        return self._coll.delete_one({'_id': ObjectId(id)})

    def delete_one(self, query_spec):
        return self._coll.delete_one(query_spec)

    def delete_many(self, query_spec):
        return self._coll.delete_many(query_spec)

    def update_one(self, filter, update, upsert):
        # 'update only works with $ operators'
        return self._coll.update_one(filter, update, upsert)

    def update_many(self, filter, update, upsert):
        # 'update only works with $ operators'
        return self._coll.update_many(filter, update, upsert)

    def count_docs(self, query_spec):
        return self._coll.count_documents(query_spec)

    def last_request_stats(self):
        return self._db.command({'getLastRequestStatistics': 1})

    def last_request_request_charge(self):
        stats = self.last_request_stats()
        if stats == None:
            return -1
        else:
            return stats['RequestCharge']

    def client(self):
        return self._client


class MongoDBInstance:

    def __init__(self, uri: str):
        self.uri = uri
        if 'cosmos.azure.com' in self.uri:
            self._env = 'cosmos'
        else:
            self._env = 'mongo'
        
        self.client = MongoClient(uri, tlsCAFile=certifi.where())

        self.databases = self.client.list_database_names()
        if "admin" in self.databases:
            self.databases.remove("admin")
        if "local" in self.databases:
            self.databases.remove("local")
        if "config" in self.databases:
            self.databases.remove("config")
        self.collections = {}
        for database in self.databases:
            self.collections[database] = self.client[database].list_collection_names(filter={'type': 'collection'})

        # optionally display and capture the list of databases and their collections
        # if the --list-dbs-and-colls command-line arg is provided
        if self.list_databases_and_collections():
            for db_idx, db_name in enumerate(sorted(self.databases)):
                db = self.get_database(db_name)
                if db != None:
                    print('env: {} db: {}'.format(self._env, db_name))
                    for cname in sorted(db.collections):
                        print('env: {} db: {} coll: {}'.format(self._env, db_name, cname))
                        key = '{}|{}|{}'.format(self._env, db_name, cname)

    def list_databases_and_collections(self):
        for arg in sys.argv:
            if arg == '--list-dbs-and-colls':
                return True
        return False
        
    def get_database(self, database_name: str):
        return MongoDBDatabase(self.client[database_name])


class MongoDBDatabase:

    def __init__(self, database):
        self.database = database
        self.collections = self.database.list_collection_names(filter={'type': 'collection'})

    def get_collection(self, collection_name: str):
        return MongoDBCollection(self.database, collection_name)


class MongoDBCollection:

    def __init__(self, database, collection_name: str):
        self.collection = database[collection_name]

    def get_size(self, database):
        stats = database.database.command("collStats", self.collection.name)
        return stats['size']

    def get_num_documents(self):
        return self.collection.estimated_document_count()

    def get_indexes(self):
        return self.collection.index_information()
