import json

from pysrc.counter import Counter
from pysrc.datasets import Datasets
from pysrc.env import Env

# This class is used to extact the MongoDB index information from the
# MMA outputs.
#
# Chris Joakim, Microsoft, 2023

class Indices(object):

    def __init__(self, aggregated_mma_outputs):
        self.aggregated_mma_outputs = aggregated_mma_outputs
        self.aggregated_index_info = dict()
        self.aggregated_index_advice = dict()
        self.index_issue_counter = Counter()

    def extract_info_and_advice(self):
        identifier = 'AppData'
        for mma_obj in self.aggregated_mma_outputs:
            filename = mma_obj['_file_name'].strip()
            if 'index' in filename:
                if Env.verbose():
                    print(filename)
            if identifier in filename:
                if 'index_metadata' in filename:
                    if filename.endswith('.json'):
                        self.parse_index_metadata(mma_obj)
                elif filename.endswith('index_advisor_report.json'):
                    self.parse_index_advisor_report(mma_obj)

        Datasets.write_aggregated_index_info_file(self.aggregated_index_info)
        Datasets.write_aggregated_index_advice_file(self.aggregated_index_advice)
        Datasets.write_aggregated_index_advice_unique_file(self.index_issue_counter.get_data())
        Datasets.write_aggregated_index_advice_unique_csv_file(self.index_issue_counter.get_data())

    def parse_index_metadata(self, mma_obj):
        cluster = mma_obj['_cluster'] 
        try:
            data = mma_obj['data']
            dbname = data['db_name']
            cname = data['collection_name']
            key = '{}|{}|{}'.format(cluster.strip(), dbname.strip(), cname.strip())
            for index in data['indexes']:
                index['_file_name'] = mma_obj['_file_name']
                #print(json.dumps(index, sort_keys=False, indent=2))
                if key in self.aggregated_index_info.keys():
                    self.aggregated_index_info[key].append(index)
                else:
                    self.aggregated_index_info[key] = list()
                    self.aggregated_index_info[key].append(index)
        except:
            print('exception on file: {}'.format(mma_obj['_file_name']))

    def parse_index_advisor_report(self, mma_obj):
        try:
            cluster = mma_obj['_cluster']
            data = mma_obj['data']
            dbdict = data['Databases']
            for dbname in dbdict.keys():
                db = dbdict[dbname]
                #print(json.dumps(db, sort_keys=False, indent=2))
                colls = db['Collections']
                for cname in colls.keys():
                    coll = colls[cname]
                    #print(json.dumps(coll, sort_keys=False, indent=2))
                    for ckey in coll.keys():
                        if 'ssessments' in ckey:
                            assessment_list = coll[ckey]
                            for assessment in assessment_list:
                                #print(json.dumps(assessment, sort_keys=False, indent=2))
                                obj = dict()
                                key = '{}|{}|{}'.format(cluster.strip(), dbname.strip(), cname.strip())
                                obj['sev']  = assessment['AssessmentSeverity']
                                obj['name'] = assessment['AssessmentName']
                                obj['msg']  = assessment['Message']

                                counter_key = '{}|{}|{}'.format(obj['sev'], obj['name'], obj['msg'])
                                self.index_issue_counter.increment(counter_key)

                                if key in self.aggregated_index_advice.keys():
                                    self.aggregated_index_advice[key].append(obj)
                                else:
                                    self.aggregated_index_advice[key] = list()
                                    self.aggregated_index_advice[key].append(obj)
        except Exception as e:
            print(e)

    def unique_advice(self):
        data = dict()
        for cdc_key in self.aggregated_index_advice.keys():
            cdc_list = self.aggregated_index_advice[cdc_key]  # cdc = cluster-db-container
            for advice in cdc_list:
                concat_key = '{}|{}|{}'.format(advice['sev'], advice['name'], advice['msg'])
                data[concat_key] = ''
        return data
