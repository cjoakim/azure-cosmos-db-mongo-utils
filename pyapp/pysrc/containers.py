# Instances of this class represent set of Collection in MongoDB and the
# corresponding Container in Cosmos DB.  Instances are created from
# the data produced by the MMA tool - MongoMigrationAssessment.exe
#
# Chris Joakim, Microsoft, 2023

class Containers(object):
    def __init__(self):
        self.container_list = list()
        self.container_dict = dict()

    def get_containers_list(self):
        return self.container_list

    def get_containers_dict(self):
        return self.container_dict

    def count(self):
        return len(self.container_list)

    def add(self, container):
        key = container.key()
        if key in self.container_dict.keys():
            pass
        else:
            self.container_list.append(container)
            self.container_dict[key] = container

    def has_key(self, key):
        return key in self.container_dict.keys()

    def get_data_list(self):
        # Create an optimized data structure for downstream processing
        # such as report and artifact generation.
        data = dict()
        data['cluster_names'] = dict()
        data['container_keys'] = dict()
        data['container_list'] = list()
        for c in self.container_list:
            cluster, key = c.cluster(), c.key()
            data['cluster_names'][cluster] = ''
            data['container_keys'][key] = ''
            data['container_list'].append(c.get_data())
        return data
