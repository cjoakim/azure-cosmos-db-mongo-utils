"""
Usage:
  python main.py <func> <args...>
  python main.py check_env
  python main.py read_parse_clusters_info_excel_file
  python main.py docscan_doc_capture
  python main.py docscan_results_report
  python main.py gen_mma_execution_scripts
  python main.py gen_pymongo_connect_script
  python main.py pymongo_connect <cluster-name> <conn-str>
  python main.py aggregate_mma_execution_output
  python main.py gather_cluster_db_collection_info
  python main.py gather_shard_info
  python main.py gather_index_info
  python main.py capture_mma_logs
  python main.py clusters_report
  python main.py scan_mmaout_results
  python main.py scan_mmaout_results_report
  python main.py despace_file tmp/Results.csv tmp/despaced.csv
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# This is the entry-point for this Python application. The main method
# is passed a "function" as sys.argv[1], and other function-specific
# command-line args.  Most of the logic is delegated to class Tasks.
#
# Chris Joakim, Microsoft, 2023

import sys

from docopt import docopt

from pysrc.datasets import Datasets
from pysrc.fs import FS
from pysrc.task import Tasks


def print_options(msg):
    print(msg)
    arguments = docopt(__doc__, version='0.1.0')
    print(arguments)

def check_env():
    Datasets.reset()
    tr = Tasks.check_env()
    tr.display()

def read_parse_clusters_info_excel_file():
    Datasets.reset()
    tr = Tasks.read_parse_clusters_info_excel_file()
    tr.display()
    return tr.get_resp_obj()

def docscan_doc_capture():
    Datasets.reset()
    tr = Tasks.docscan_doc_capture()
    tr.display()

def docscan_results_report():
    Datasets.reset()
    tr = Tasks.docscan_results_report()
    tr.display()

def gen_mma_execution_scripts():
    clusters = read_parse_clusters_info_excel_file()
    Datasets.reset()
    tr = Tasks.gen_mma_execution_scripts(clusters)
    tr.display()

def gen_pymongo_connect_script():
    clusters = read_parse_clusters_info_excel_file()
    Datasets.reset()
    tr = Tasks.gen_pymongo_connect_script(clusters)
    tr.display()

def pymongo_connect(cluster_name, conn_str):
    Datasets.reset()
    tr = Tasks.pymongo_connect(cluster_name, conn_str)
    tr.display()

def aggregate_mma_execution_output():
    Datasets.reset()
    tr = Tasks.aggregate_mma_execution_output()
    tr.display()
    Datasets.display()

def gather_cluster_db_collection_info():
    Datasets.reset()
    tr = Tasks.gather_cluster_db_collection_info()
    tr.display()
    Datasets.display()

def gather_shard_info():
    Datasets.reset()
    tr = Tasks.gather_shard_info()
    tr.display()
    Datasets.display()

def gather_index_info():
    Datasets.reset()
    tr = Tasks.gather_index_info()
    tr.display()
    Datasets.display()

def capture_mma_logs():
    Datasets.reset()
    tr = Tasks.capture_mma_logs()
    tr.display()
    Datasets.display()

def migration_wave_report():
    Datasets.reset()
    tr = Tasks.migration_wave_report()
    tr.display()
    Datasets.display()

def scan_mmaout_results():
    Datasets.reset()
    tr = Tasks.scan_mmaout_results()
    tr.display()

def scan_mmaout_results_report():
    Datasets.reset()
    tr = Tasks.scan_mmaout_results_report()
    tr.display()
    return tr.get_resp_obj()

def despace_file(infile, outfile):
    Datasets.reset()
    tr = Tasks.despace_file(infile, outfile)
    tr.display()

def adhoc():
    in_lines = FS.read_lines('tmp/mma_1000.ps1')
    out_lines = list()
    for line in in_lines:
        if '.exe' in line:
            tokens = line.strip().split()
            token  = tokens[2].replace('"','')
            print(token)
            out_lines.append(token)
    FS.write_lines(out_lines, 'tmp/tokens.txt')

def find_file_in_list(filenames, cluster_name, suffix):
    for filename in filenames:
        if cluster_name in filename:
            if suffix in filename:
                return filename
    return None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_options('Error: no command-line args')
    else:
        func = sys.argv[1].lower()
        if func == 'check_env':
            check_env()
        elif func == 'read_parse_clusters_info_excel_file':
            read_parse_clusters_info_excel_file()
        elif func == 'docscan_doc_capture':
            docscan_doc_capture()
        elif func == 'docscan_results_report':
            docscan_results_report()
        elif func == 'gen_mma_execution_scripts':
            gen_mma_execution_scripts()
        elif func == 'gen_pymongo_connect_script':
            gen_pymongo_connect_script()
        elif func == 'pymongo_connect':
            cluster_name, conn_str = sys.argv[2], sys.argv[3]
            pymongo_connect(cluster_name, conn_str)
        elif func == 'aggregate_mma_execution_output':
            aggregate_mma_execution_output()
        elif func == 'gather_cluster_db_collection_info':
            gather_cluster_db_collection_info()
        elif func == 'gather_shard_info':
            gather_shard_info()
        elif func == 'gather_index_info':
            gather_index_info()
        elif func == 'capture_mma_logs':
            capture_mma_logs()
        elif func == 'migration_wave_report':
            migration_wave_report()
        elif func == 'scan_mmaout_results':
            scan_mmaout_results()
        elif func == 'scan_mmaout_results_report':
            scan_mmaout_results_report()
        elif func == 'despace_file':
            infile, outfile = sys.argv[2], sys.argv[3]
            despace_file(infile, outfile)
        elif func == 'adhoc':
            adhoc()
        else:
            print_options('Error: invalid function: {}'.format(func))
