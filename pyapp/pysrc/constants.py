# This class defines pseudo-constant values similar to a Java interface.
# The other classes in this project should use these magic/constant values.
#
# Chris Joakim, Microsoft, 2023
# Usage:
# from pysrc.constants import Constants, Colors

class Constants(object):

    FLAG_ARG_VERBOSE          = '-v'
    FLAG_ARG_VERY_VERBOSE     = '-vv'
    REQUEST_CHARGE_HEADER     = 'x-ms-request-charge'
    ACTIVITY_ID_HEADER        = 'x-ms-activity-id'

class Colors:
    HEADER    = '\033[95m'
    OKBLUE    = '\033[94m'
    OKCYAN    = '\033[96m'
    OKGREEN   = '\033[92m'
    WARNING   = '\033[93m'
    FAIL      = '\033[91m'

    YELLOW    = '\033[93m'
    RED       = '\033[91m'
    WHITE     = '\033[97m'

    ENDC      = '\033[0m'
    BOLD      = '\033[1m'
    UNDERLINE = '\033[4m'

    # COLOR = {
    #     'blue': '\033[94m',
    #     'default': '\033[99m',
    #     'grey': '\033[90m',
    #     'yellow': '\033[93m',
    #     'black': '\033[90m',
    #     'cyan': '\033[96m',
    #     'green': '\033[92m',
    #     'magenta': '\033[95m',
    #     'white': '\033[97m',
    #     'red': '\033[91m'
    # }