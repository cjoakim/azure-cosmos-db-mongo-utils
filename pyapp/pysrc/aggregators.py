import json

from pysrc.env import Env

from pysrc.container import Container
from pysrc.containers import Containers

# This class is used aggregate the MMA outputs into an instance of 
# class Containers which has instances of class Container.
# This class implements some filtering and grouping of these objects.
#
# Chris Joakim, Microsoft, 2023

class ContainersAggregator(object):
    def __init__(self, aggregated_mma_outputs):
        self.aggregated_mma_outputs = aggregated_mma_outputs
        self.containers_obj = Containers()

        for mma_obj in aggregated_mma_outputs:
            filename = mma_obj['_file_name'].strip()
            if 'Collector' in filename:
                if 'collection_metadata' in filename:
                    if filename.endswith('.json'):
                        if Env.verbose():
                            print('processing filename: {}'.format(filename))
                        if Env.very_verbose():
                            print(json.dumps(mma_obj, sort_keys=False, indent=2))
                        try:
                            cluster, dbname, cname = None, None, None
                            if '_cluster' in mma_obj.keys():
                                cluster = mma_obj['_cluster']
                            if 'data' in mma_obj.keys():
                                data = mma_obj['data']
                                if 'db_name' in data.keys():
                                    dbname = data['db_name']
                                    if 'collection_metadata' in data.keys():
                                        cm = data['collection_metadata']
                                        if 'name' in cm.keys():
                                            cname = cm['name']
                                            c = Container(cluster, dbname, cname)
                                            if 'stats' in cm.keys():
                                                c.set_stats(cm['stats'])
                                            if 'options' in cm.keys():
                                                c.set_options(cm['options'])
                                            c.calculate()
                                            self.containers_obj.add(c)
                        except Exception as e:
                            print(e)

    def get_containers_obj(self):
        return self.containers_obj

    def get_containers_in_cluster_names(self, cluster_names):
        containers_list = list()
        for c in self.containers_obj.get_containers_list():
            if c.cluster() in cluster_names:
                containers_list.append(c)
        return containers_list

    def group_by_cluster_db_keys(self, container_obj_list):
        grouped_dict = dict()

        # first create dict with an empty list for each cluster_db key
        for c in container_obj_list:
            key = c.cluster_db_key()
            grouped_dict[key] = list()

        # next populate the dict
        for c in container_obj_list:
            key = c.cluster_db_key()
            grouped_dict[key].append(c)

        return grouped_dict

    def collect_overrides_data(self, containers_data_list):
        data = dict()
        # create the major sections in the desired sequence in data
        data['defaults'] = dict()
        data['cluster_mappings'] = dict()
        data['container_mappings'] = list()

        obj = dict()
        obj['subscription'] = '<your-azure-subscription-id>'
        obj['resource_group'] = 'devops_test'
        obj['regions'] = ['eastus']
        obj['ru_provisioning_options'] = 'one of: db_autoscale, container_autoscale, container_manual'
        obj['ru_provisioning'] = 'db_autoscale'
        obj['ru'] = 10000
        obj['partition_key'] = '_id'
        data['defaults'] = obj

        for cluster_name in sorted(containers_data_list['cluster_names'].keys()):
            obj = dict()
            obj['source_cluster'] = cluster_name 
            obj['cosmos_acct'] = cluster_name 
            obj['subscription'] = '' 
            obj['resource_group'] = '' 
            obj['regions'] = []
            obj['ru_provisioning'] = 'db_autoscale'
            data['cluster_mappings'][cluster_name] = obj

        for c in containers_data_list['container_list']:
            obj = dict()
            obj['key'] = c['key']
            obj['cluster'] = c['cluster']
            obj['dbname'] = c['dbname'] 
            obj['cname'] = c['cname']
            obj['sharded'] = c['sharded']
            obj['shard_keys'] = c['shard_keys']
            obj['partition_key'] = ''
            obj['ru_provisioning'] = 'db_autoscale'
            obj['ru'] = self.estimated_ru_for_container(c)
            data['container_mappings'].append(obj)

        return data

    def estimated_ru_for_container(self, c):
        # TODO - implement
        return 0
