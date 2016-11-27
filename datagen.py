#! /usr/bin/env python
# Generate Synthetic Query Dataset

import math
import random
import bisect

class uniformGenerator(object):
    pass


class biasUniformGenerator(object):
    pass


class zipfGenerator(object):
    def __init__(self, N, s):
        pdf = [1. / math.pow(float(i), s) for i in xrange(1, N+1)]
        cdf = reduce(lambda sums, x: sums + [sums[-1] + x], pdf, [0])
        self.distMap = [x / cdf[-1] for x in cdf]

    def get(self):
        return bisect.bisect(self.distMap, random.random())

    def get_indx(self):
        return self.get() - 1


class reverseZipfGenerator(zipfGenerator):
    def __init__(self, N, s):
        zipfGenerator.__init__(self, N, s)
        self.N = N

    def get(self):
        v = bisect.bisect(self.distMap, random.random())
        return self.N + 1 - v 

    def get_indx(self):
        return self.get() - 1


class dataGen(object):
    
    def __init__(self, query_size, role_size, table_size, column_size, params):
        self.query_size = query_size
        self.role_size = 4 # In current impl, ROLE_SIZE is fixed with 4
        self.table_size = table_size
        self.column_size = column_size
        self.s = params['s']
        self.role_list = ['ROLE' + str(k+1) for k in xrange(self.role_size)]
        self.table_list = ['TABLE' + str(k+1) for k in xrange(self.table_size)]
        self.column_list = ['COLUMN' + str(k+1) for k in xrange(self.column_size)]
    
    def _assemble(self, query_command, query_tables, table_columns):
        return 'SELECT * FROM hackday;'

    def _gen_first_role_data(self):
        query_command = 'SELECT'
        query_tables = set()
        table_columns = {}
        tgen = zipfGenerator(self.table_size, self.s)
        for k in xrange(self.table_size):
            query_tables.add(self.table_list[tgen.get_indx()]) 
        for table in query_tables:
            columns = set()
            cgen = zipfGenerator(self.column_size, self.s)
            for k in xrange(self.column_size):
                columns.add(self.column_list[cgen.get_indx()])
            table_columns[table] = columns
        query = self._assemble(query_command, query_tables, table_columns)
        return (query, table_columns)

    def _gen_second_role_data(self):
        query_command = 'SELECT'
        query_tables = set()
        table_columns = {}
        tgen = reverseZipfGenerator(self.table_size, self.s)
        for k in xrange(self.table_size):
            query_tables.add(self.table_list[tgen.get_indx()]) 
        for table in query_tables:
            columns = set()
            cgen = reverseZipfGenerator(self.column_size, self.s)
            for k in xrange(self.column_size):
                columns.add(self.column_list[cgen.get_indx()])
            table_columns[table] = columns
        query = self._assemble(query_command, query_tables, table_columns)
        return (query, table_columns)

    def _gen_third_role_data(self):
        return ('SELECT * FROM hackday;', set()) 

    def _gen_forth_role_data(self):
        return ('SELECT * FROM hackday;', set()) 
    
    def generate(self, output, mode = 'query'):
        f = file(output, 'w')
        rgen = zipfGenerator(self.role_size, self.s)
        for k in xrange(self.query_size):
            role_indx = rgen.get_indx()
            role = self.role_list[role_indx]
            if role_indx == 0:
                (query, tpl) = self._gen_first_role_data()
            elif role_indx == 1:
                (query, tpl) = self._gen_second_role_data()
            elif role_indx == 2:
                (query, tpl) = self._gen_third_role_data()
            else: # role_indx == 3
                (query, tpl) = self._gen_forth_role_data()
            if mode == 'query':
                f.write('%s|%s\n' % (role, query))
            elif mode == 'tuple':
                f.write('%s|%s\n' % (role, tpl))
            elif mode == 'all':
                f.write('%s|%s|%s\n' % (role, query, tpl))
        f.close()


if __name__ == '__main__':
    #ZIPF_10_5 = zipfGenerator(10, 5.)
    #for i in xrange(1000):
    #    ZIPF_10_5.get()
    data = dataGen(5000, 4, 100, 20, {'s': 3.})
    data.generate('train', 'all')
    #RZIPF_10_5 = reverseZipfGenerator(10, 5.)
    #print RZIPF_10_5.get_indx()
