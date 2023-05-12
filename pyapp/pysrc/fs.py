import csv
import json
import os

# This class is used to do IO operations vs the local File System.
#
# Chris Joakim, Microsoft, 2023

class FS(object):

    @classmethod
    def as_unix_filename(cls, filename):
        if filename.upper().startswith("C:"):
            return filename[2:].replace("\\", "/")
        else:
            return filename

    @classmethod
    def pwd(cls):
        return cls.as_unix_filename(os.getcwd())

    @classmethod
    def read(cls, infile):
        with open(infile, 'rt') as f:
            return f.read()

    @classmethod
    def readt(cls, infile):
        with open(infile, 'r') as f:
            return f.read()

    @classmethod
    def read_binary(cls, infile):
        with open(infile, 'rb') as f:
            return f.read()

    @classmethod
    def read_lines(cls, infile):
        lines = list()
        with open(infile, 'rt') as f:
            for line in f:
                lines.append(line)
        return lines

    @classmethod
    def read_single_line(cls, infile):
        return cls.read_lines(infile)[0].strip()

    @classmethod
    def read_encoded_lines(cls, infile, encoding='cp1252'):
        lines = list()
        with open(infile, 'rt', encoding=encoding) as f:
            for line in f:
                lines.append(line)
        return lines

    @classmethod
    def read_win_cp1252(cls, infile, encoding='cp1252'):
        with open(os.path.join(infile), 'r', encoding='cp1252') as f:
            return f.read()

    @classmethod
    def read_csv(cls, infile, reader='default', delim=',', dialect='excel', skip=0):
        rows = list()
        if reader == 'dict':
            with open(infile, 'rt') as csvfile:
                rdr = csv.DictReader(csvfile, dialect=dialect, delimiter=delim)
                for row in rdr:
                    rows.append(row)
        else:
            with open(infile) as csvfile:
                rdr = csv.reader(csvfile, delimiter=delim)
                for idx, row in enumerate(rdr):
                    if idx >= skip:
                        rows.append(row)
        return rows

    @classmethod
    def read_json(cls, infile):
        with open(infile, 'rt') as f:
            return json.loads(f.read())

    @classmethod
    def read_json_utf8(cls, infile):
        with open(infile, 'r', encoding="utf-8") as f:
            return json.loads(f.read())

    @classmethod
    def write_json(cls, obj, outfile, pretty=True, verbose=True):
        jstr = None
        if pretty == True:
            jstr = json.dumps(obj, sort_keys=False, indent=2)
        else:
            jstr = json.dumps(obj)

        with open(outfile, 'w') as f:
            f.write(jstr)
            if verbose == True:
                print('file written: {}'.format(outfile))

    @classmethod
    def write_lines(cls, lines, outfile, verbose=True):
        with open(outfile, 'w', encoding="utf-8") as f:
            #f.writelines(lines)
            for line in lines:
                f.write(line + "\n") # os.linesep)  # \n works on Windows
            if verbose == True:
                print('file written: {}'.format(outfile))

    @classmethod
    def text_file_iterator(cls, infile):
        # return a line generator that can be iterated with iterate()
        with open(infile, 'rt') as f:
            for line in f:
                yield line.strip()

    @classmethod
    def write(cls, outfile, s, verbose=True):
        with open(outfile, 'w') as f:
            f.write(s)
            if verbose == True:
                print('file written: {}'.format(outfile))

    @classmethod
    def list_directories_in_dir(cls, rootdir):
        files = list()
        for file in os.listdir(rootdir):
            d = os.path.join(rootdir, file)
            if os.path.isdir(d):
                files.append(file)
        return files

    @classmethod
    def list_files_in_dir(cls, rootdir):
        files = list()
        for file in os.listdir(rootdir):
            d = os.path.join(rootdir, file)
            if os.path.isdir(d):
                pass
            else:
                files.append(file)
        return files

    @classmethod
    def walk(cls, directory):
        files = list()
        for dir_name, subdirs, base_names in os.walk(directory):
            for base_name in base_names:
                full_name = "{}/{}".format(dir_name, base_name)
                entry = dict()
                entry['base'] = base_name
                entry['dir'] = dir_name
                entry['full'] = full_name
                entry['abspath'] = os.path.abspath(full_name)
                files.append(entry)
        return files

    @classmethod
    def read_csvfile_into_rows(cls, infile, delim=','):
        rows = list()  # return a list of csv rows
        with open(infile, 'rt') as csvfile:
            reader = csv.reader(csvfile, delimiter=delim)
            for row in reader:
                rows.append(row)
        return rows

    @classmethod
    def read_csvfile_into_objects(cls, infile, delim=','):
        objects = list()  # return a list of dicts
        with open(infile) as csvfile:
            reader = csv.reader(csvfile, delimiter=delim)
            headers = None
            for idx, row in enumerate(reader):
                if idx == 0:
                    headers = row
                else:
                    if len(row) == len(headers):
                        obj = dict()
                        for field_idx, field_name in enumerate(headers):
                            key = field_name.strip().lower()
                            obj[key] = row[field_idx].strip()
                        objects.append(obj)
        return objects
