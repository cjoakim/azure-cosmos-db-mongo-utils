from pysrc.env import Env
from pysrc.fs import FS

# Centralized location in this app to read and write the many
# application-specific files.
#
# Chris Joakim, Microsoft, 2023

class Datasets(object):

    input_files = list()
    output_files = list()
    config = None

    def __init__(self):
        pass

    @classmethod
    def username(cls):
        return Env.username()
    def pwd(cls):
        return cls.as_unix_filename(FS.pwd())

    @classmethod
    def as_unix_filename(cls, filename):
        if filename.upper().startswith("C:"):
            return filename[2:].replace("\\", "/")
        else:
            return filename

    @classmethod
    def customer_data_dir(cls):
        return cls.as_unix_filename(Env.customer_data_dir())

    @classmethod
    def mma_output_dir(cls):
        return '/Users/{}/AppData/Local/Temp/MongoMigrationAssessment'.format(cls.username())

    @classmethod
    def mma_output_dir_walk_list_file(cls):
        return 'current/mma_output_dir_walk_list.json'

    @classmethod
    def mma_output_dir_walk_filenames_file(cls):
        return 'current/mma_output_dir_walk_filenames.json'

    @classmethod
    def aggregated_mma_output_file(cls):
        return 'current/aggregated_mma_outputs.json'

    @classmethod
    def mma_logs_dir(cls):
        return '/Users/{}/AppData/Local/Temp/.dmamongo/logs'.format(cls.username())

    @classmethod
    def cluster_uuid_mapping_file(cls):
        return 'current/cluster_uuid_mapping.json'

    @classmethod
    def cluster_id_verification_file(cls):
        return 'current/cluster_id_verification.json'

    @classmethod
    def cluster_file_counts(cls):
        return 'current/cluster_file_counts.json'

    @classmethod
    def swift_config_filename(cls):
        return 'current/swift-config.json'

    @classmethod
    def swift_config_sample_filename(cls):
        return 'current/swift-config-sample.json'

    @classmethod
    def mappings_filename(cls):
        return 'current/mappings.json'

    @classmethod
    def mappings_generated_config_filename(cls):
        return 'current/mappings-generated.json'

    @classmethod
    def mma_execution_script_filename(cls, mma_exe_dir):
        return '{}/swift-mma-execute.ps1'.format(mma_exe_dir)

    @classmethod
    def mma_execution_script_tmp_filename(cls):
        return 'current/swift-mma-execute.ps1'

    @classmethod
    def swift_aggregated_mma_info_filename(cls):
        return 'current/aggregated-mma-info.json'

    @classmethod
    def swift_aggregated_containers_info_filename(cls):
        return 'current/aggregated-containers-info.json'

    @classmethod
    def swift_aggregated_shard_keys_filename(cls):
        return 'current/aggregated-shard-keys.json'

    @classmethod
    def swift_aggregated_shard_advice_filename(cls):
        return 'current/aggregated-shard-advice.json'

    @classmethod
    def swift_aggregated_index_info_filename(cls):
        return 'current/aggregated-index-info.json'

    @classmethod
    def swift_aggregated_index_advice_filename(cls):
        return 'current/aggregated-index-advice.json'

    @classmethod
    def swift_aggregated_index_advice_unique_filename(cls):
        return 'current/aggregated-index-advice-unique.json'

    @classmethod
    def swift_aggregated_index_advice_unique_csv_filename(cls):
        return 'current/aggregated-index-advice-unique.csv'

    @classmethod
    def bicep_artifacts_dir(cls):
        return 'artifacts/bicep/'

    # ==========

    @classmethod
    def set_config(cls, c):
        config = c

    @classmethod
    def write_swift_config_sample_file(cls, config):
        filename = cls.swift_config_sample_filename()
        cls.add_output_file(filename)
        FS.write_json(config, filename)

    @classmethod
    def read_swift_config_file(cls):
        filename = cls.swift_config_filename()
        cls.add_input_file(filename)
        c = FS.read_json(filename)
        cls.config = c
        return c

    @classmethod
    def write_swift_config_file(cls, config):
        filename = cls.swift_config_filename()
        cls.add_output_file(filename)
        FS.write_json(config, filename)

    @classmethod
    def write_mma_execution_script_file(cls, lines):
        mma_exe_dir = cls.config['mma_exe_dir']
        filename = cls.mma_execution_script_filename(mma_exe_dir)
        cls.add_output_file(filename)
        FS.write_lines(lines, filename)

    @classmethod
    def write_mma_execution_script_tmp_file(cls, lines):
        filename = cls.mma_execution_script_tmp_filename()
        cls.add_output_file(filename)
        FS.write_lines(lines, filename)

    @classmethod
    def read_aggregated_mma_output_file(cls):
        filename = cls.aggregated_mma_output_file()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_aggregated_mma_output_file(cls, aggregated_json_files):
        filename = cls.aggregated_mma_output_file()
        cls.add_output_file(filename)
        FS.write_json(aggregated_json_files, filename)

    @classmethod
    def write_mma_output_dir_walk_list(cls, file_info_list):
        filename = cls.mma_output_dir_walk_list_file()
        cls.add_output_file(filename)
        FS.write_json(file_info_list, filename)

    @classmethod
    def write_mma_output_dir_walk_filenames(cls, file_info_list):
        filenames = list()
        for file_obj in file_info_list:
            filenames.append(file_obj['abspath'])
        filename = cls.mma_output_dir_walk_filenames_file()
        cls.add_output_file(filename)
        FS.write_json(filenames, filename)

    @classmethod
    def read_cluster_uuid_mappings_file(cls):
        filename = cls.cluster_uuid_mapping_file()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_cluster_uuid_mappings_file(cls, data):
        filename = cls.cluster_uuid_mapping_file()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def read_cluster_mappings_verification_file(cls):
        filename = cls.cluster_id_verification_file()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_cluster_mappings_verification_file(cls, data):
        filename = cls.cluster_id_verification_file()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def read_user_mappings_file(cls):
        filename = cls.mappings_filename()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_user_mappings_file(cls, data):
        filename = cls.mappings_filename()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def read_aggregated_containers_info_file(cls):
        filename = cls.swift_aggregated_containers_info_filename()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_aggregated_containers_info_file(cls, data):
        filename = cls.swift_aggregated_containers_info_filename()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def read_mappings_generated_config_file(cls):
        filename = cls.mappings_filename()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_mappings_generated_config_file(cls, data):
        filename = cls.mappings_filename()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def read_aggregated_shard_keys_file(cls):
        filename = cls.swift_aggregated_shard_keys_filename()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_aggregated_shard_keys_file(cls, data):
        filename = cls.swift_aggregated_shard_keys_filename()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def read_aggregated_shard_advice_file(cls):
        filename = cls.swift_aggregated_shard_advice_filename()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_aggregated_shard_advice_file(cls, data):
        filename = cls.swift_aggregated_shard_advice_filename()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def read_aggregated_index_info_file(cls):
        filename = cls.swift_aggregated_index_info_filename()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_aggregated_index_info_file(cls, data):
        filename = cls.swift_aggregated_index_info_filename()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def read_aggregated_index_advice_file(cls):
        filename = cls.swift_aggregated_index_advice_filename()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_aggregated_index_advice_file(cls, data):
        filename = cls.swift_aggregated_index_advice_filename()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def write_aggregated_index_advice_unique_file(cls, data):
        filename = cls.swift_aggregated_index_advice_unique_filename()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def write_aggregated_index_advice_unique_csv_file(cls, data):
        filename = cls.swift_aggregated_index_advice_unique_csv_filename()
        cls.add_output_file(filename)
        lines = list()
        lines.append('Count,Severity,Name,Description')
        for key in data:
            count = data[key]
            tokens = key.split('|')
            lines.append('{},{},{},{}'.format(count, tokens[0], tokens[1], tokens[2]))
        FS.write_lines(lines, filename)

    @classmethod
    def read_cluster_file_counts(cls):
        filename = cls.cluster_file_counts()
        cls.add_input_file(filename)
        return FS.read_json(filename)

    @classmethod
    def write_cluster_file_counts(cls, data):
        filename = cls.cluster_file_counts()
        cls.add_output_file(filename)
        FS.write_json(data, filename)

    @classmethod
    def reset(cls):
        input_files = list()
        output_files = list()

    @classmethod
    def add_input_file(cls, filename):
        if cls.input_files == None:
            cls.input_files = list()
        cls.input_files.append(filename)

    @classmethod
    def add_output_file(cls, filename):
        if cls.output_files == None:
            cls.output_files = list()
        cls.output_files.append(filename)

    @classmethod
    def display(cls):
        for filename in cls.input_files:
            print('Datasets.input_file:  {}'.format(filename))
        for filename in cls.output_files:
            print('Datasets.output_file: {}'.format(filename))
