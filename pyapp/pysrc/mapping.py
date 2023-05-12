# This class is a wrapper for mappings.json, provides access methods
# to its data.
#
# THIS CLASS IS NOT FULLY IMPLEMENTED AT THIS TIME.  USED FOR ARTIFACT GENERATION.
#
# Chris Joakim, Microsoft, 2023

class Mapping(object):

    def __init__(self, mappings):
        self.mappings = mappings  # this is the parsed mappings.json data

    def cluster_subscription(self, cluster_name):
        cluster_sub, default_sub = '', ''
        try:
            default_sub = self.mappings['defaults']['subscription'].strip()
            cluster_sub = self.mappings['cluster_mappings'][cluster_name]['subscription'].strip()
            if len(cluster_sub) > 0:
                return cluster_sub
        except:
            pass
        return default_sub

    def resource_group(self, cluster_name):
        cluster_rg, default_rg = '', ''
        try:
            default_rg = self.mappings['defaults']['resource_group'].strip()
            cluster_rg = self.mappings['cluster_mappings'][cluster_name]['resource_group'].strip()
            if len(cluster_rg) > 0:
                return cluster_rg
        except:
            pass
        return default_rg

    def region(self, cluster_name):
        cluster_region, default_region = '', ''
        try:
            default_region = self.mappings['defaults']['regions'][0].strip()
            cluster_region = self.mappings['cluster_mappings'][cluster_name]['regions'][0].strip()
            if len(cluster_region) > 0:
                return cluster_region
        except:
            pass
        return default_region
