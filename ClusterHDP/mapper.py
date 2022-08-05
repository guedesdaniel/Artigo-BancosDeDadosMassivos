#!/usr/bin/env python

import sys
import csv

SEP = '\t'

class Mapper(object):
    def __init__(self, stream, sep=SEP):
        self.stream = stream
        self.sep = sep
        
    def emit(self, key, value):
        sys.stdout.write("{}{}{}\n".format(key, self.sep, round(value,2)))
    
    def map(self):
        for row in self:
            #print(row)
            #print(row[0], row[9])
            self.emit(row[3], float(row[9]))
            
    def __iter__(self):
        reader = csv.reader(self.stream, delimiter=';')
        for row in reader:
            if row[9] != 'valor':
                yield row
            
if __name__ == '__main__':
    mapper = Mapper(sys.stdin)
    mapper.map()