import os
import sys

import arrow

from pysrc.constants import Constants

# This class is used to read the host environment, such as environment variables.
# It also has methods for command-line argument processing.
#
# Chris Joakim, Microsoft, 2023

class Env(object):

    @classmethod
    def var(cls, name, default=None):
        if name in os.environ:
            return os.environ[name]
        else:
            return default

    @classmethod
    def username(cls):
        u = cls.var('USERNAME')  # Windows
        if u == None:
            u = cls.var('USER')  # macOS, Linux
        return u

    @classmethod
    def customer_data_dir(cls):
        return cls.var('AZURE_MONGO_UTILS_DATA_DIR')

    @classmethod
    def epoch(cls):
        return arrow.utcnow().timestamp

    @classmethod
    def verbose(cls):
        flags = [ Constants.FLAG_ARG_VERBOSE, Constants.FLAG_ARG_VERY_VERBOSE ]
        for arg in sys.argv:
            for flag in flags:
                if arg == flag:
                    return True
        return False
        
    @classmethod
    def very_verbose(cls):
        flags = [ Constants.FLAG_ARG_VERY_VERBOSE ]
        for arg in sys.argv:
            for flag in flags:
                if arg == flag:
                    return True
        return False

    @classmethod
    def boolean_arg(cls, flag):
        for arg in sys.argv:
            if arg == flag:
                return True
        return False
