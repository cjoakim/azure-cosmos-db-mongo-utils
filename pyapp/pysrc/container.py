import math

from pysrc.bytes import Bytes

# Instances of this class represent a Collection in MongoDB and the
# corresponding Container in Cosmos DB.  Instances are created from
# the data produced by the MMA tool - MongoMigrationAssessment.exe
#
# Chris Joakim, Microsoft, 2023

class Container(object):

    def __init__(self, cluster, dbname, cname):
        self.data = dict()
        self.data['key'] = '' # set below
        self.data['cluster'] = cluster 
        self.data['dbname'] = dbname 
        self.data['cname'] = cname 
        self.data['stats'] = dict() 
        self.data['sharded'] = False
        self.data['shard_keys'] = dict()
        self.data['pp'] = 1
        self.data['db_level_throughput'] = False
        self.data['migration_ru'] = 0
        self.data['post_migration_ru'] = 0
        self.data['provisioning_type'] = ''

        self.data['key'] = self.key()

    def set_data(self, data):
        self.data = data

    def get_data(self):
        return self.data

    def key(self):
        try:
            return '{}|{}|{}'.format(self.cluster(), self.dbname(), self.cname())
        except:
            return '||'

    def cluster_db_key(self):
        try:
            return '{}|{}'.format(self.cluster().strip(), self.dbname().strip())
        except:
            return '|'

    def cluster(self):
        try:
            return str(self.data['cluster']).strip()
        except:
            return '?'

    def dbname(self):
        try:
            return str(self.data['dbname']).strip()
        except:
            return '?'

    def cname(self):
        try:
            return str(self.data['cname']).strip()
        except:
            return '?'

    def sharded(self):
        try:
            return str(self.data['sharded']).lower()
        except:
            return '?'

    def shard_key(self):
        try:
            if self.sharded() == 'false':
                return ''
            else:
                return 'todo'
        except:
            return '?'

    def doc_count(self):
        try:
            return self.data['stats']['document_count']
        except:
            return -1

    def avg_doc_size(self):
        try:
            return self.data['stats']['avg_object_size_in_bytes']
        except:
            return -1

    def size_in_bytes(self):
        try:
            return int(self.data['stats']['size_in_bytes'])
        except:
            return -1

    def size_in_gb(self):
        try:
            return Bytes.as_gigabytes(self.size_in_bytes())
        except:
            return -1

    def set_stats(self, obj):
        if obj != None:
            self.data['stats'] = obj

    def set_options(self, obj):
        if obj != None:
            self.data['options'] = obj

    def get_collation(self):
        try:
            return str(self.data['options']['collation']).lower()
        except:
            return 'none'

    def is_capped(self):
        try:
            return str(self.data['options']['is_capped']).lower()
        except:
            return 'false'

    def get_features(self):
        words = list()
        col = self.get_collation()
        if col != 'none':
            words.append('collation: {}'.format(col).replace(',',' '))
        cap = self.is_capped()
        if cap != 'none':
            words.append('capped')

        result = '. '.join(words)
        if result != '':
            print('# features: {} --> {}'.format(self.key(), result))  # greppable
        return result

    def set_sharded(self, value):
        if value != None:
            self.data['sharded'] = value

    def set_shard_keys(self, obj):
        if obj != None:
            self.data['shard_keys'] = obj

    def get_pp(self):
        return self.data['pp']

    def set_pp(self, i):
        self.data['pp'] = int(i)

    def get_db_level_throughput(self):
        return self.data['db_level_throughput']

    def set_db_level_throughput(self, bool):
        self.data['db_level_throughput'] = bool

    def get_mpp_flag(self):
        if self.get_pp() > 1:
            return 'yes'
        else:
            return ''

    def get_migration_ru(self):
        return self.data['migration_ru']
    def set_migration_ru(self, ru):
        self.data['migration_ru'] = ru

    def get_post_migration_ru(self):
        return self.data['post_migration_ru']
    def set_post_migration_ru(self, ru):
        self.data['post_migration_ru'] = ru

    def get_provisioning_type(self):
        return self.data['provisioning_type']
    def set_provisioning_type(self, s):
        self.data['provisioning_type'] = s

    def get_sharding_note(self):
        if self.size_in_gb() < 5.0:
            return ''
        else:
            return 'should be sharded due to > 5GB compressed storage'

    def get_report_note(self):
        return '{}.  {}'.format(self.data['provisioning_type'], self.get_sharding_note())

    def calculate(self):
        gb = self.size_in_gb()
        gb_uncompressed = gb * 4.0
        pp = int(math.ceil(gb_uncompressed / 50.0))
        if pp < 1:
            pp = 1
        self.set_pp(pp)

        # do the same calculation regardless of db-level-throughput

        # if gb < 1:
        #     self.set_migration_ru(10000)
        #     self.set_post_migration_ru(400)
        #     self.set_provisioning_type('manual scale')
        # else:
        #     self.set_migration_ru(int(pp * 10000))
        #     self.set_post_migration_ru(int(pp * 4000))
        #     self.set_provisioning_type('auto scale')

        # As of 2023/05/02 use container-level autoscale throughput in all cases; corresponds with migration tool.
        self.set_provisioning_type('auto scale')
        self.set_migration_ru(int(pp * 10000))
        if pp < 2:
            self.set_post_migration_ru(int(pp * 1000))
        else:
            self.set_post_migration_ru(int(pp * 10000))

        if False:
            print('Container_calculate() {} gb: {}, pp: {}, mpp: {}, mru: {}, pmru: {}, ptype: {}'.format(
                self.key(),
                gb,
                pp,
                self.get_mpp_flag(),
                self.get_migration_ru(),
                self.get_post_migration_ru(),
                self.get_provisioning_type()))
