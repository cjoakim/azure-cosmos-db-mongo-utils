from pysrc.env import Env
from pysrc.fs import FS

# Instances of this class represent the set of expected MMA output files
# for a cluster evaluation, and their actual presence or absence.
#
# Chris Joakim, Microsoft, 2023

class MmaFiles(object):

    def __init__(self, parent_dir):
        self.dir = parent_dir
        self.expected = dict()
        self.expected['mongo_migration_assessment_report.html'] = False
        self.expected['collection_options_advisor_report.json'] = False
        self.expected['features_advisor_report.json'] = False
        self.expected['index_advisor_report.json'] = False
        self.expected['instance_summary_advisor_report.json'] = False
        self.expected['limits_quotas_advisor_report.json'] = False
        self.expected['shard_keys.json'] = False
        self.expected['shard_key_advisor_report.json'] = False
        self.expected['feature_supported.json'] = False
        self.expected['instance_detail.json'] = False
        self.expected['shard_keys.json'] = False
        self.expected_basenames = sorted(self.expected.keys())
        self.collection_metadata = list()
        self.index_metadata = list()

    def get_data(self):
        data = dict()
        data['dir'] = self.dir
        data['expected'] = self.expected
        data['collection_metadata_count'] = len(self.collection_metadata)
        data['index_metadata_count'] = len(self.index_metadata)
        return data

    def add(self, file_obj):
        try:
            basename = file_obj['base']
            abspath  = file_obj['abspath']
            for expected_basename in self.expected_basenames:
                if expected_basename == basename:
                    self.expected[expected_basename] = True
            if 'collection_metadata' in abspath:
                if abspath.endswith('.json'):
                    self.collection_metadata.append(abspath)
            if 'index_metadata' in abspath:
                if abspath.endswith('.json'):
                    self.index_metadata.append(abspath)
        except:
            print("exception in ExpectedFilesSet#add")

    def all_present(self):
        for basename, value in self.expected.items():
            if value == False:
                return False
        return True
