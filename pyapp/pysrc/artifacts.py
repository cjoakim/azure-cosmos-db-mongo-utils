import math
import os

import arrow
import jinja2

from pysrc.env import Env
from pysrc.fs import FS
from pysrc.mapping import Mapping
from pysrc.datasets import Datasets

# Class ArtifactGenerator is used to generate DevOps artifacts
# such as Bicep templates, Bicep parameters, and PowerShell scripts
# which execute the Bicep templates.
# This class uses Jinja2 templates; see https://palletsprojects.com/p/jinja/
# Other DevOps artifact types, such as Terraform, may be added in the future.
#
# THIS CLASS IS NOT FULLY IMPLEMENTED AT THIS TIME.
#
# Chris Joakim, Microsoft, 2023

class ArtifactGenerator(object):  

    def __init__(self):
        # These are the two inputs to the artifact generator:
        # 1) aggregated_mma_outputs is the MMA info
        # 2) mappings is a user-edited file, but was generated from MMA info

        self.aggregated_mma_outputs = Datasets.read_aggregated_mma_output_file()
        self.mappings = Datasets.read_user_mappings_file()
        self.mapping = Mapping(self.mappings)

    def generate_bicep_and_powershell_artifacts(self):
        print('generate_bicep_and_powershell_artifacts - this is a work-in-progress')

        # jinja2 template names, see the templates/ directory
        ps1_deploy_template         = 'ps1_deploy_template.jinga2'
        cosmos_mongo_bicep_template = 'cosmos_mongo_db_bicep.jinga2'
        bicep_params_template       = 'bicep_params.jinja2'

        # Produce Bicep, Bicep Parms, and a PowerShell script for each cluster.
        # Assumptions are:
        # 1) there is a 1:1 mapping from source cluster to a corresponding cosmos acct
        # 2) a 1:1 mapping from source database names to target cosmos database names
        # 3) a 1:1 mapping from source collection names to cosmos containers
        # 4) the user has edited the generated mappings.json file appropriately
        #    prior to artifacts being generated here in this class.

        for cluster_name in sorted(self.mappings['cluster_mappings']):
            cluster_mapping = self.mappings['cluster_mappings'][cluster_name]
            cosmos_acct = cluster_mapping['cosmos_acct']
            self.apply_cluster_defaults(cluster_mapping)

            dbnames = self.collect_dbnames_for_cluster(cluster_name)
            for dbname in dbnames:
                print('cluster: {}, dbname: {}'.format(cluster_name, dbname))
                db_containers = self.collect_containers_for_cluster_and_db(cluster_name, dbname)
                for c in db_containers:
                    print('cluster: {}, dbname: {}, cname: {}'.format(cluster_name, dbname, c['cname']))
                for c in db_containers:
                    self.apply_container_defaults(c)

                # build the output filenames
                deploy_name = '{}-{}-deploy'.format(cosmos_acct, dbname)
                ps1_file    = 'artifacts/bicep/{}.ps1'.format(deploy_name)

                bicep_base  = '{}.bicep'.format(deploy_name)
                bicep_file  = 'artifacts/bicep/{}'.format(bicep_base)
                params_base = '{}-params.json'.format(deploy_name)
                params_file = 'artifacts/bicep/{}'.format(params_base)

                # build one template_data object to pass to jinga2 for three templates
                template_data = dict()
                template_data['gen_timestamp'] = self.timestamp()
                template_data['source_cluster_name'] = cluster_name
                template_data['cosmos_acct_name'] = cosmos_acct
                template_data['dbname'] = dbname
                template_data['subscription'] = self.mapping.cluster_subscription(cluster_name)
                template_data['resource_group'] = self.mapping.resource_group(cluster_name)
                template_data['region'] = self.mapping.region(cluster_name)
                template_data['bicep_base']  = bicep_base
                template_data['params_base'] = params_base
                template_data['deployment_name'] = deploy_name
                template_data['redirect'] = "> tmp/{}.log".format(deploy_name)
                containers = self.collect_containers_for_cluster_and_db(cluster_name, dbname)
                self.set_consistent_throughputs(cluster_name, dbname, containers)

                template_data['db_level_throughput'] = self.calculate_db_level_throughput(dbname, containers)
                template_data['containers'] = containers

                debug_file = 'tmp/template-data-{}-{}.json'.format(cosmos_acct, dbname)
                FS.write_json(template_data, debug_file)

                self.render_template(ps1_deploy_template, template_data, ps1_file)
                self.render_template(cosmos_mongo_bicep_template, template_data, bicep_file)
                self.render_template(bicep_params_template, template_data, params_file)

            # see https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/parameter-files

    def apply_cluster_defaults(self, cluster_mapping):
        if len(cluster_mapping['subscription']) < 1:
            cluster_mapping['subscription'] = self.mappings['defaults']['subscription']

        if len(cluster_mapping['resource_group'].strip()) < 1:
            default_rg = self.mappings['defaults']['resource_group']
            if len(default_rg.strip()) < 1:
                cluster_mapping['resource_group'] = cluster_mapping['cosmos_acct']
            else:
                cluster_mapping['resource_group'] = default_rg
        
        if len(cluster_mapping['regions']) < 1:
          cluster_mapping['region'] = self.mappings['defaults']['regions'][0]
        else:
          cluster_mapping['region'] = cluster_mapping['regions'][0]

        cluster_mapping['what_if_clause'] = ''  # --what-if 

    def collect_dbnames_for_cluster(self, cluster_name):
        dbnames = dict()
        for c in self.mappings['container_mappings']:
            if c['cluster'] == cluster_name:
                dbname = c['dbname']
                dbnames[dbname] = ''
        return sorted(dbnames.keys())

    def collect_containers_for_cluster_and_db(self, cluster_name, dbname):
        containers = list()
        for c in self.mappings['container_mappings']:
            if c['cluster'] == cluster_name:
                if c['dbname'] == dbname:     
                    containers.append(c)
        return containers

    def set_consistent_throughputs(self, cluster_name, dbname, containers):
        # db_autoscale, container_autoscale, container_manual
        pass

    def calculate_db_level_throughput(self, dbname, containers):
        # TODO - remove this if statement; for initial dev only
        if dbname == 'dev':
            total_ru = 0;
            for c in containers:
                total_ru = total_ru + c['ru']
            return self.round_to_100(total_ru / 2.0)  # TODO: refine this formula

        if len(containers) > 25:
            return 0
        else:
            total_ru = 0;
            for c in containers:
                total_ru = total_ru + c['ru']
            return self.round_to_100(total_ru / 2.0)  # TODO: refine this formula

    def round_to_100(self, n):
        return int(math.ceil(n / 100.0)) * 100

    def apply_container_defaults(self, c):
        if c['ru'] < 400:
            c['ru'] = int(self.mappings['defaults']['ru'])

        if c['ru_provisioning'] == 'manual':
            pass  # ok, valid value 
        elif c['ru_provisioning'] == 'autoscale':
            pass  # ok, valid value
        else:
            c['ru_provisioning'] = self.mappings['defaults']['ru']

        if c['partition_key'] == '':
            c['partition_key'] = self.mappings['defaults']['partition_key']

    def azure_subscription(self):
        value = self.mappings['default_subscription']
        if value.startswith('Env:'):
            tokens = value.split(':')
            return Env.var(tokens[1])
        else:
            return value

    def timestamp(self):
        return arrow.utcnow().format('YYYY-MM-DD HH:mm:ss UTC')

    def render_template(self, template_name, template_data, outfile):
        t = self.get_template(os.getcwd(), template_name)
        s = t.render(template_data)
        self.write(outfile, s)

    def get_template(self, root_dir, name):
        filename = 'templates/{}'.format(name)
        print('get_template: {}'.format(filename))
        return self.get_jinja2_env(root_dir).get_template(filename)

    def get_jinja2_env(self, root_dir):
        # See https://jinja.palletsprojects.com/en/2.10.x/api/?highlight=trim_blocks
        return jinja2.Environment(
            loader = jinja2.FileSystemLoader(root_dir),
            trim_blocks = True,
            lstrip_blocks = False,
            autoescape = False)

    def write(self, outfile, s, verbose=True):
        with open(outfile, 'w', encoding='utf-8') as f:
            f.write(s)
            if verbose:
                print('file written: {}'.format(outfile))
