#!/usr/bin/env python

import sys
from itertools import groupby
from operator import itemgetter

SEP = '\t'

class Reducer(object):
    def __init__(self, stream, sep=SEP):
        self.stream = stream
        self.sep = sep
        
    def emit(self, key, value):
        sys.stdout.write('{}{}{}\n'.format(key, self.sep, round(value,2)))
        
    def reduce(self):
        for current, group in groupby(self, itemgetter(0)):
            total = 0
            count = 0
            
            for item in group:
                total += item[1]
                count += 1
            
            #self.emit(current, float(total) / float(count))
            self.emit(current, float(total))
            
    def __iter__(self):
        for line in self.stream:
            try:
                parts = line.split(self.sep)
                yield parts[0], float(parts[1])
            except:
                continue
            
if __name__ == '__main__':
    reducer = Reducer(sys.stdin)
    reducer.reduce()