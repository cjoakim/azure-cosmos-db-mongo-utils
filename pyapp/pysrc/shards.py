import json

from pysrc.counter import Counter
from pysrc.datasets import Datasets
from pysrc.env import Env

# This class is used to extact the MongoDB sharding information from the
# MMA outputs.
#
# Chris Joakim, Microsoft, 2023

class Shards(object):

    def __init__(self, aggregated_mma_outputs):
        self.aggregated_mma_outputs = aggregated_mma_outputs
        self.aggregated_shard_keys = dict()
        self.aggregated_shard_advice = dict()

    def extract_keys_and_advice(self):
        print('extract_keys_and_advice...')
        ctr = Counter()
        #filename_pattern = 'db0da162dfcef309aab0c847ea46d9b982308f85e2e7c0535b655c1d8c4bf890'
        filename_pattern = 'AppData'  # AppData matches all MMA output files

        for mma_obj in self.aggregated_mma_outputs:
            ctr.increment(mma_obj['_cluster'])
            filename = mma_obj['_file_name'].strip()
            if filename_pattern in filename:
                if filename.endswith('shard_keys.json'):
                    self.parse_shard_keys_data(mma_obj)
                elif filename.endswith('shard_key_advisor_report.json'):
                    self.parse_shard_key_advisor_report(mma_obj)

        Datasets.write_cluster_file_counts(ctr.get_data())
        Datasets.write_aggregated_shard_keys_file(self.aggregated_shard_keys)
        Datasets.write_aggregated_shard_advice_file(self.aggregated_shard_advice)

    def parse_shard_keys_data(self, mma_obj):
        cluster = mma_obj['_cluster'] 
        try:
            if Env.very_verbose():
                if cluster != '?':
                    print(json.dumps(mma_obj, sort_keys=False, indent=2))
            shard_keys = mma_obj['data']['shard_keys']
            for shard_key in shard_keys:
                shard_key['_file_name'] = mma_obj['_file_name']
                ns = shard_key['namespace'].replace('.','|')
                skey = str(shard_key['key'])
                uniq = shard_key['is_unique']
                if shard_key['is_dropped'] == False:
                    cdc_key = '{}|{}'.format(cluster.strip(), ns.strip())
                    self.aggregated_shard_keys[cdc_key] = shard_key
        except:
            print('exception on file: {}'.format(mma_obj['_file_name']))

    def parse_shard_key_advisor_report(self, mma_obj):
        try:
            cluster = mma_obj['_cluster']
            data = mma_obj['data']
            dbdict = data['Databases']
            for dbname in dbdict.keys():
                db = dbdict[dbname]
                colls = db['Collections']
                for cname in colls.keys():
                    coll = colls[cname]
                    # print(json.dumps(coll, sort_keys=False, indent=2))
                    for ckey in coll.keys():
                        if 'ssessments' in ckey:
                            assessment_list = coll[ckey]
                            for assessment in assessment_list:
                                obj = dict()
                                key = '{}|{}|{}'.format(cluster.strip(), dbname.strip(), cname.strip())
                                obj['sev']  = assessment['AssessmentSeverity']
                                obj['name'] = assessment['AssessmentName']
                                obj['msg']  = assessment['Message']
                                if key in self.aggregated_shard_advice.keys():
                                    self.aggregated_shard_advice[key].append(obj)
                                else:
                                    self.aggregated_shard_advice[key] = list()
                                    self.aggregated_shard_advice[key].append(obj)
        except Exception as e:
            print(e)
