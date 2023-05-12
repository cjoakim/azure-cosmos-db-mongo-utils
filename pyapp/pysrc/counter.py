from pysrc.bytes import Bytes

# This class implements a simple counter with an underlying dict object.
#
# Chris Joakim, Microsoft, 2023

class Counter(object):

    def __init__(self):
        self.data = dict()

    def increment(self, key):
        if key in self.data.keys():
            self.data[key] = self.data[key] + 1
        else:
            self.data[key] = 1

    def decrement(self, key):
        if key in self.data.keys():
            self.data[key] = self.data[key] - 1
        else:
            self.data[key] = -1

    def get_data(self):
        return self.data
