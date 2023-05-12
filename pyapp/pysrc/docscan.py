
# This module is used to read, proces, and report on the output of
# the "large document scanner" at https://github.com/cjoakim/mongodb-docscan
#
# Chris Joakim, Microsoft, 2023

from pysrc.bytes import Bytes

class DocscanClusterResult(object):

    def __init__(self, data):
        self.raw_data = data
        self.collections = list()
        self.exception = None
        self.cluster_name = None

    def parse(self):
        try:
            self.cluster_name = self.raw_data['clusterName']
            for container_key in self.raw_data['containerInfo'].keys():
                container_obj = self.raw_data['containerInfo'][container_key]
                dc = DocscanContainer(self.cluster_name, container_obj)
                self.collections.append(dc)
        except Exception as e:
            self.exception = e

    def successful(self):
        return self.exception == None

class DocscanCluster(object):

    def __init__(self, cluster_name):
        self.name = cluster_name
        self.largest_doc_size = 0
        self.largest_doc = None

    def add_container(self, dc):
        # dc is an instance of class DocscanContainer
        if dc is not None:
            if dc.largest_doc_size > self.largest_doc_size:
                self.largest_doc = dc
                self.largest_doc_size = dc.largest_doc_size

class DocscanContainer(object):

    def __init__(self, cluster_name, data):
        self.key = '?'
        self.cluster_name = cluster_name
        self.data = data
        self.doc_count = -1
        self.largest_doc_size = -1
        self.largest_doc_prefix = ''
        self.exception = None

        try:
            self.doc_count = data['iteratedDocumentCount']
            self.dbname = data['dbName']
            self.cname  = data['cName']
            self.largest_doc_size  = data['largestSize']
            self.largest_doc_prefix = data['largestDocJsonPrefix'].replace(',',' ')
            self.key = '{}|{}|{}'.format(self.cluster_name, self.dbname, self.cname)
        except Exception as e:
            self.exception = e

    def get_data(self):
        return self.data

    def as_csv_header(self):
        return 'cluster,database,collection,doc_count,largest_size,over_2mb,id'

    def as_csv(self):
        two_mb = Bytes.megabyte() * 2.0
        over_2mb = 'no'
        if self.largest_doc_size > two_mb:
            over_2mb = 'yes'

        values = list()
        values.append(self.cluster_name)
        values.append(self.dbname)
        values.append(self.cname)
        values.append(str(self.doc_count))
        values.append(str(self.largest_doc_size))
        values.append(over_2mb)
        values.append(str(self.parse_id()))
        return ','.join(values)

    def parse_id(self):
        if self.largest_doc_prefix is not None:
            try:
                s = self.largest_doc_prefix.replace("\"",'')
                s = s.replace(':','')
                s = s.replace('{','')
                s = s.replace('}','')
                tokens = s.split(' ')
                if len(tokens) > 1:
                    if len(tokens) > 2:
                        if tokens[1] == '$oid':
                            return tokens[2]
                    else:
                        return tokens[1]
                else:
                    return ''
            except:
                return ''
        else:
            return ''
