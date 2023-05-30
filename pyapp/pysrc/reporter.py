import math

from pysrc.aggregators import ContainersAggregator
from pysrc.datasets import Datasets
from pysrc.env import Env
from pysrc.fs import FS

# This class is used to produce CSV/Excel reports based on the MMA data
# and other data.  For example, the "wave" and "all clusters" report.
#
# Chris Joakim, Microsoft, 2023

class Reporter(object):  

    def __init__(self):
        self.cluster_uuid_mappings = Datasets.read_cluster_uuid_mappings_file()
        self.aggregated_mma_outputs = Datasets.read_aggregated_mma_output_file()
        self.agg = ContainersAggregator(self.aggregated_mma_outputs)
        self.agg_index_advice = Datasets.read_aggregated_index_advice_file()
        self.agg_shard_keys   = Datasets.read_aggregated_shard_keys_file()
        self.agg_shard_advice = Datasets.read_aggregated_shard_advice_file()

        self.excel_clusters = FS.read_json('current/excel_clusters.json')
        self.unique_cluster_urls = FS.read_json('current/excel_unique_cluster_urls.json')
        self.docscan_collections = FS.read_json('current/docscan_merged_collections_dict.json')

        self.clusters_status = self.merge_clusters_status()

    def merge_clusters_status(self):
        # Merge the cluster definition names in the Customer Master Excel file
        # with the actual set of cluster names from actual MMA outputs.
        merged_clusters, mma_clusters = dict(), dict()

        for cluster_key in self.excel_clusters.keys():
            merged_clusters[cluster_key] = 'defined_in_excel'

        for file_obj in self.aggregated_mma_outputs:
            try:
                mma_cluster_key = file_obj['_cluster']
                mma_clusters[mma_cluster_key] = ''
                merged_clusters[mma_cluster_key] = 'has_explicit_mma_output'

                for conn_str in self.unique_cluster_urls.keys():
                    associated_clusters_list = self.unique_cluster_urls[conn_str]
                    if mma_cluster_key in associated_clusters_list:
                        for assoc_cluster_key in associated_clusters_list:
                            if assoc_cluster_key != mma_cluster_key:
                                assoc_value = merged_clusters[assoc_cluster_key]
                                if assoc_value != 'has_explicit_mma_output':
                                    new_value = 'has_associated_mma_output_from|{}'.format(mma_cluster_key)
                                    merged_clusters[assoc_cluster_key] = new_value
            except:
                pass

        FS.write_json(merged_clusters, 'current/merged_clusters_status.json')
        return merged_clusters

    def lookup_uuid_value(self, cluster_key):
        for uuid_key, value in self.cluster_uuid_mappings.items():
            if value == cluster_key:
                return uuid_key
        return 'none'

    def migration_wave_report(self):
        print('migration_wave_report')

        # first, display the cluster_uuid_mappings and clusters_status
        for idx, key in enumerate(sorted(self.cluster_uuid_mappings.keys())):
            name = self.cluster_uuid_mappings[key]
            print('mapped_cluster: {} uuid: {} name: {}'.format(idx, key, name))
        for idx, cluster_key in enumerate(sorted(self.clusters_status.keys())):
            status = self.clusters_status[cluster_key]
            uuid_value = self.lookup_uuid_value(cluster_key)
            print('cluster_status: {} name: {} status: {} uuid: {}'.format(idx, cluster_key, status, uuid_value))

        report_outfile = 'current/migration_wave_report.csv'

        # initialize the list of csv_lines, produce the CSV Header line, display columns
        csv_lines = list()
        csv_lines.append(self.csv_header_line())
        csv_cols = self.csv_header_line().split(',')
        print('csv_cols_count: {}'.format(len(csv_cols)))
        if Env.verbose():
            for col_idx, col_name in enumerate(csv_cols):
                print('# report csv_col {} {} '.format(col_idx, col_name))
                #print('    {}             VARCHAR(255),'.format(col_name))

        # the list of wave_clusters drives the report creation
        self.wave_clusters = sorted(self.clusters_status.keys())
        FS.write_json(self.wave_clusters, 'current/migration_wave_clusters.json')

        # collect all of the containers that pertain to this wave
        self.containers_for_wave = self.agg.get_containers_in_cluster_names(self.wave_clusters)
        containers_data = list()
        for c in self.containers_for_wave:
            containers_data.append(c.get_data())
        FS.write_json(containers_data, 'current/containers_for_wave.json')
        print('containers_for_wave count: {}'.format(len(self.containers_for_wave)))

        self.grouped_by_cluster_db_dict = self.agg.group_by_cluster_db_keys(self.containers_for_wave)
        print('grouped_by_cluster_db_dict len: {}'.format(len(self.grouped_by_cluster_db_dict)))
        for key in self.grouped_by_cluster_db_dict.keys():
            print('grouped_by_cluster_db_dict key: {}'.format(key))
            # grouped_by_cluster_db_dict key: 9-DTC-PROD---VIENNA|Order

        prev_cluster_db_key = ''
        for wave_cluster_idx, wave_cluster_key in enumerate(sorted(self.wave_clusters)):
            status = self.clusters_status[wave_cluster_key]
            print('wave_cluster_key: {} status: {}'.format(wave_cluster_key, status))  # 0-DTC-PROD---BAMBERG

            if status == 'has_explicit_mma_output':
                pass
            elif status.startswith('has_associated_mma_output'):
                assoc_cluster = status.split('|')[1]
                csv_lines.append('')
                cluster = self.cluster_as_trello(wave_cluster_key)
                notes   = 'see sibling cluster {}'.format(assoc_cluster)
                source_host = self.lookup_source_host(wave_cluster_key)
                cosmos_acct = self.lookup_cosmos_acct(wave_cluster_key)
                csv_line = self.csv_line_template().format(
                    self.cluster_as_trello(cluster),
                    self.cluster_as_trello(assoc_cluster),
                    '',
                    '',
                    '0',
                    '',
                    '',
                    '0',
                    '',
                    '',
                    '0',
                    '0',
                    '0',
                    '',
                    '0',
                    '0',
                    '',
                    notes,
                    '',
                    '',
                    '.',
                    '.',
                    '.',
                    source_host,
                    cosmos_acct)
                csv_lines.append(csv_line)
                continue
            elif status == 'defined_in_excel':
                csv_lines.append('')
                cluster = self.cluster_as_trello(wave_cluster_key)
                source_host = self.lookup_source_host(wave_cluster_key)
                cosmos_acct = self.lookup_cosmos_acct(wave_cluster_key)
                notes =('{} is in the Customer Excel file but has no MMA output'.format(
                    self.cluster_as_trello(wave_cluster_key)))
                csv_line = self.csv_line_template().format(
                    self.cluster_as_trello(cluster),
                    '',
                    '',
                    '',
                    '0',
                    '',
                    '',
                    '0',
                    '',
                    '',
                    '0',
                    '0',
                    '0',
                    '',
                    '0',
                    '0',
                    '',
                    notes,
                    '',
                    '',
                    '.',
                    '.',
                    '.',
                    source_host,
                    cosmos_acct)
                csv_lines.append(csv_line)
                continue

            cluster_db_keys = self.collect_cluster_db_keys(wave_cluster_key)
            for cluster_db_key in sorted(cluster_db_keys):
                print('wave_cluster_key: {} cluster_db_key: {}'.format(wave_cluster_key, cluster_db_key))
                if cluster_db_key != prev_cluster_db_key:
                    prev_cluster_db_key = cluster_db_key
                    csv_lines.append('')
                tokens  = cluster_db_key.split('|')
                cluster = tokens[0]
                dbname  = tokens[1]
                print('cluster: {}  dbname: {}'.format(cluster, dbname))
                cluster_db_containers = self.grouped_by_cluster_db_dict[cluster_db_key]
                db_totals = self.calculate_db_totals(cluster_db_containers)
                bytes = db_totals['bytes']
                gb = format(db_totals['gb'], ".6f")
                pp = db_totals['pp']
                db_level_migration_ru = db_totals['db_level_migration_ru']
                db_level_post_migration_ru = db_totals['db_level_post_migration_ru']
                notes = ''
                if db_level_migration_ru > 0:
                    notes = 'use database-level autoscale'
                else:
                    notes = 'use container-level scaling'
                index_advice = ''

                # Display the database level totals first
                largest = ''
                source_host = self.lookup_source_host(cluster)
                cosmos_acct = self.lookup_cosmos_acct(cluster)
                csv_line = self.csv_line_template().format(
                    self.cluster_as_trello(cluster),
                    self.cluster_as_trello(cluster),
                    dbname,
                    'Database Total',
                    len(cluster_db_containers),
                    '',
                    '',
                    bytes,
                    '',
                    '',
                    str(largest),
                    gb,
                    pp,
                    '',
                    db_level_migration_ru,
                    db_level_post_migration_ru,
                    '',
                    notes,
                    '',
                    index_advice,
                    'w',
                    'c',
                    's',
                    source_host,
                    cosmos_acct)
                csv_lines.append(csv_line)

                # Display the container level details next
                for c in cluster_db_containers:
                    ckey = c.key()
                    if key in self.docscan_collections.keys():
                        print('container key is in docscan: {}'.format(ckey))

                    notes = ''
                    largest = '-1'
                    index_advice = self.lookup_index_advice(c)
                    shard_key = self.lookup_shard_key_info(cluster_db_key, c.cname())
                    gb = format(c.size_in_gb(), ".6f")
                    if db_totals['use_db_level_throughput'] == True:
                        csv_line = self.csv_line_template().format(
                            self.cluster_as_trello(c.cluster()),
                            self.cluster_as_trello(c.cluster()),
                            c.dbname(),
                            c.cname(),
                            '1',
                            len(shard_key) > 0,
                            shard_key,
                            c.size_in_bytes(),
                            c.doc_count(),
                            c.avg_doc_size(),
                            str(largest),
                            gb,
                            c.get_pp(),
                            c.get_mpp_flag(),
                            '',
                            '',
                            '',
                            c.get_sharding_note(),
                            c.get_features(),
                            index_advice,
                            '',
                            '',
                            '',
                            source_host,
                            cosmos_acct)
                        csv_lines.append(csv_line)
                    else:
                        csv_line = self.csv_line_template().format(
                            self.cluster_as_trello(c.cluster()),
                            self.cluster_as_trello(c.cluster()),
                            c.dbname(),
                            c.cname(),
                            '1',
                            len(shard_key) > 0, #c.sharded(),
                            shard_key,
                            c.size_in_bytes(),
                            c.doc_count(),
                            c.avg_doc_size(),
                            str(largest),
                            gb,
                            c.get_pp(),
                            c.get_mpp_flag(),
                            c.get_migration_ru(),
                            c.get_post_migration_ru(),
                            '',
                            c.get_report_note(),
                            c.get_features(),
                            index_advice,
                            '0',
                            '0',
                            '0',
                            source_host,
                            cosmos_acct)
                        csv_lines.append(csv_line)

        FS.write_lines(csv_lines, report_outfile)
        self.write_postgresql_files(csv_lines)

    def collect_cluster_db_keys(self, wave_cluster_key):
        cluster_db_keys = list()
        for key in self.grouped_by_cluster_db_dict.keys():
            if key.startswith(wave_cluster_key):
                cluster_db_keys.append(key)
        return sorted(cluster_db_keys)

    def lookup_cosmos_acct(self, cluster_key):
        acct = ''
        if cluster_key in self.excel_clusters.keys():
            acct = self.excel_clusters[cluster_key]['target']
        print('lookup_cosmos_acct: {} --> {}'.format(cluster_key, acct))
        return acct

    def lookup_source_host(self, cluster_key):
        host = ''
        try:
            if cluster_key in self.excel_clusters.keys():
                host = self.excel_clusters[cluster_key]['source'].split('@')[1]
        except:
            pass
        print('lookup_source_host: {} --> {}'.format(cluster_key, host))
        return host

    def csv_line_template(self):
        lines = list()
        for field in self.csv_header_line().split(','):
            lines.append('{}')
        return ','.join(lines)

    def csv_header_line(self):
        return 'cluster,mma_sibling_cluster,database,container,container_count,sharded,shard_key,size_in_bytes,doc_count,avg_doc_size,largest,size_in_gb,pp_equiv,mpp,est_migration_ru,est_post_migration_ru,.,notes,features,index_advice,idx_warning,idx_critical,idx_score,source_host,cosmos_acct'

    def write_postgresql_files(self, csv_lines):
        FS.write_lines(self.pg_lines(csv_lines), 'current/psql/mma_report.csv')
        FS.write_lines(self.ru_lines(csv_lines), 'current/psql/mma_collection_ru.csv')

        lines = list()
        lines.append("uuid,cname,tname")
        for idx, uuid_key in enumerate(sorted(self.cluster_uuid_mappings.keys())):
            cname = self.cluster_uuid_mappings[uuid_key]
            tname = self.cluster_as_trello(cname)
            lines.append('{},{},{}'.format(uuid_key,cname,tname))
        FS.write_lines(lines, 'current/psql/mma_uuid_to_cluster.csv')

    def pg_lines(self, csv_lines):
        lines = list()
        for line in csv_lines:
            include = True
            if len(line) < 20:
                include = False
            if ' Excel ' in line:
                include = False
            # if ' sibling ' in line:
            #     include = False
            if include == True:
                lines.append(line)
        return lines

    def ru_lines(self, csv_lines):
        lines, fields = list(), self.csv_header_line().split(',')
        # dynamically determine the field indices based on the current csv header
        cluster_idx, mma_cluster_idx, db_idx, c_idx, ru1_idx, ru2_idx, source_idx, cosmos_idx = 0, 0, 0, 0, 0, 0, 0, 0
        for field_idx, field_name in enumerate(fields):
            if field_name == 'cluster':
                cluster_idx = field_idx
            if field_name == 'mma_sibling_cluster':
                mma_cluster_idx = field_idx
            if field_name == 'database':
                db_idx = field_idx
            if field_name == 'container':
                c_idx = field_idx
            if field_name == 'est_migration_ru':
                ru1_idx = field_idx
            if field_name == 'est_post_migration_ru':
                ru2_idx = field_idx
            if field_name == 'source_host':
                source_idx = field_idx
            if field_name == 'cosmos_acct':
                cosmos_idx = field_idx

        for line in csv_lines:
            tokens = line.split(',')
            if len(tokens) == len(fields):
                cname = tokens[c_idx]
                if 'Database Total' in cname:
                    pass
                else:
                    ru_line = '{},{},{},{},{},{},{},{}'.format(
                        tokens[cluster_idx],
                        tokens[mma_cluster_idx],
                        tokens[db_idx],
                        tokens[c_idx],
                        tokens[ru1_idx],
                        tokens[ru2_idx],
                        tokens[source_idx],
                        tokens[cosmos_idx])
                    lines.append(ru_line)
        return lines

    def cluster_as_trello(self, c):
        tokens = c.split('---')
        if len(tokens) == 2:
            return '{} ({})'.format(tokens[0], tokens[1])
        else:
            return c

    def display_csv_line_fields(self, csv_cols, csv_line):
        print('csv_line:')
        names  = csv_cols.split(',')
        values = csv_line.split(',')
        if len(names) == len(values):
            for col_idx, col_name in enumerate(names):
                print('  {}: {} --> {}'.format(col_idx, col_name, values[col_idx]))
        else:
            print('  unmatched csv_cols ({}) vs csv_line ({})'.format(len(names), len(values)))

    def calculate_db_totals(self, cluster_db_containers):
        #print('calculate_db_totals begin')
        totals = dict()
        totals['bytes'] = 0
        totals['gb'] = 0
        totals['db_level_migration_ru'] = 0
        totals['db_level_post_migration_ru'] = 0
        totals['container_count'] = len(cluster_db_containers)

        use_db_level_throughput = False
        # As of 2023/05/02 use container-level throughput in all cases; corresponds with migration tool.
        # if len(cluster_db_containers) < 25:
        #     use_db_level_throughput = True
        totals['use_db_level_throughput'] = use_db_level_throughput
        totals['db_level_post_migration_ru'] = 0
        totals['db_level_migration_ru'] = 0
        totals['gb'] = 0.0
        totals['pp'] = 0

        for c in cluster_db_containers:
            c.set_db_level_throughput(use_db_level_throughput)
            c.calculate()
            #print('calculate_db_totals; c: {}'.format(c.key()))
            totals['bytes'] = totals['bytes'] + c.size_in_bytes()
            totals['gb'] = totals['gb'] + c.size_in_gb()
            totals['pp'] = totals['pp'] + c.get_pp()

            if use_db_level_throughput == True:
                totals['db_level_post_migration_ru'] = \
                    totals['db_level_post_migration_ru'] + c.get_post_migration_ru()
                totals['db_level_migration_ru'] = \
                    totals['db_level_migration_ru'] + c.get_migration_ru()

        if use_db_level_throughput == True:
            if totals['gb'] < 10:
                totals['db_level_post_migration_ru'] = 4000
                totals['db_level_migration_ru'] = 10000
            else:
                pp = self.pp_for_gb(totals['gb'])
                totals['db_level_post_migration_ru'] = int(pp * 10000)
                totals['db_level_migration_ru'] = int(pp * 4000)

            if totals['db_level_migration_ru'] < totals['db_level_post_migration_ru']:
                totals['db_level_migration_ru'] = totals['db_level_post_migration_ru']

        return totals

    def lookup_shard_key_info(self, cluster_db_key, cname):
        result = ''  # dummy default result dict
        cluster_db_coll_key = '{}|{}'.format(cluster_db_key, cname)
        try:
            key_dict = self.agg_shard_keys[cluster_db_coll_key]['key']
            fields = list()
            for k, v in key_dict.items():
                fields.append('{}:{}'.format(k,v))
            result = ' '.join(fields)
            #print('lookup_shard_key_info hit on: {} -> {}'.format(cluster_db_coll_key, result))
        except:
            pass
        return result

    def lookup_index_advice(self, container):
        key = container.key()
        if key in self.agg_index_advice.keys():
            advice_list = self.agg_index_advice[key]
            advice_lines = list()
            for advice_obj in advice_list:
                advice_lines.append('{} - {}.'.format(advice_obj['sev'], advice_obj['name']))
            if len(advice_lines) > 0:
                advice_lines.append('See MMA output.')
            return '  '.join(advice_lines)
        else:
            return ''

    def shard_index_advice(self, container):
        key = container.key()
        if key in self.agg_shard_advice.keys():
            advice_list = self.agg_shard_advice[key]
            advice_lines = list()
            for advice_obj in advice_list:
                advice_lines.append('{} - {}.'.format(advice_obj['sev'], advice_obj['name']))
            return '  '.join(advice_lines)
        else:
            return ''

    def pp_for_gb(self, gb):
        gb_uncompressed = gb * 4.0
        pp = int(math.ceil(gb_uncompressed / 50.0))
        if pp < 1:
            pp = 1
        return pp
