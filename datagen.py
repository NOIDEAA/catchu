#! /usr/bin/env python
# Generate Synthetic Query Dataset

import math
import random
import bisect

class biasUniformGenerator(object):
    def __init__(self, N, pdf):
        self.distMap = reduce(lambda sums, x: sums + [sums[-1] + x], pdf, [0])
    
    def get(self):
        return bisect.bisect(self.distMap, random.random())

    def get_indx(self):
        return self.get() - 1


class uniformGenerator(biasUniformGenerator):
    def __init__(self, N):
        pdf = [1. / N for k in xrange(N)]
        biasUniformGenerator.__init__(self, N, pdf)
    
    def get(self):
        return biasUniformGenerator.get(self)

    def get_indx(self):
        return biasUniformGenerator.get_indx(self)


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
    
    def _assemble(self, query_command, query_tables, table_columns, condition_table_columns):
        return 'SELECT * FROM hackday;'

    def _gen_first_role_data(self):
        query_command = 'SELECT'
        query_tables = set()
        table_columns = {}
        condition_table_columns = {}
        tgen = zipfGenerator(self.table_size, self.s)
        for k in xrange(self.table_size):
            query_tables.add(self.table_list[tgen.get_indx()]) 
        for table in query_tables:
            columns = set()
            cgen = zipfGenerator(self.column_size, self.s)
            for k in xrange(self.column_size):
                columns.add(self.column_list[cgen.get_indx()])
            table_columns[table] = columns
        for table in query_tables:
            columns = set()
            cgen = zipfGenerator(self.column_size, self.s)
            for k in xrange(self.column_size):
                columns.add(self.column_list[cgen.get_indx()])
            condition_table_columns[table] = columns
        query = self._assemble(query_command, query_tables, table_columns, condition_table_columns)
        return (query, 'SELECT', table_columns, condition_table_columns)

    def _gen_second_role_data(self):
        query_command = 'SELECT'
        query_tables = set()
        table_columns = {}
        condition_table_columns = {}
        tgen = reverseZipfGenerator(self.table_size, self.s)
        for k in xrange(self.table_size):
            query_tables.add(self.table_list[tgen.get_indx()]) 
        for table in query_tables:
            columns = set()
            cgen = reverseZipfGenerator(self.column_size, self.s)
            for k in xrange(self.column_size):
                columns.add(self.column_list[cgen.get_indx()])
            table_columns[table] = columns
        for table in query_tables:
            columns = set()
            cgen = reverseZipfGenerator(self.column_size, self.s)
            for k in xrange(self.column_size):
                columns.add(self.column_list[cgen.get_indx()])
            condition_table_columns[table] = columns
        query = self._assemble(query_command, query_tables, table_columns, condition_table_columns)
        return (query, 'SELECT', table_columns, condition_table_columns)

    def _gen_third_role_data(self):
        query_commands = ['SELECT', 'INSERT', 'DELETE', 'UPDATE']
        qcgen = biasUniformGenerator(len(query_commands), [0.1, 0.1, 0.1, 0.7])
        qc = query_commands[qcgen.get_indx()]
        query_tables = set()
        table_columns = {}
        condition_table_columns = {}
        tgen1 = uniformGenerator(self.table_size)
        tgen2 = zipfGenerator(self.table_size, self.s)
        cgen1 = uniformGenerator(self.column_size)
        cgen2 = zipfGenerator(self.column_size, self.s)
        if qc == 'SELECT':
            for k in xrange(self.table_size):
                query_tables.add(self.table_list[tgen1.get_indx()])
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen1.get_indx()])
                table_columns[table] = columns
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen1.get_indx()])
                condition_table_columns[table] = columns
        elif qc == 'INSERT':
            query_tables.add(self.table_list[tgen1.get_indx()])
            for table in query_tables:
                table_columns[table] = set()
        elif qc == 'DELETE':
            query_tables.add(self.table_list[tgen1.get_indx()])
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen1.get_indx()])
                condition_table_columns[table] = columns
        elif qc == 'UPDATE':
            query_tables.add(self.table_list[tgen2.get_indx()])
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen2.get_indx()])
                table_columns[table] = columns
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen2.get_indx()])
                condition_table_columns[table] = columns
        else:
            # TODO
            pass
        query = self._assemble(qc, query_tables, table_columns, condition_table_columns)
        return (query, qc, table_columns, condition_table_columns)

    def _gen_forth_role_data(self):
        query_commands = ['SELECT', 'INSERT', 'DELETE', 'UPDATE']
        qcgen = biasUniformGenerator(len(query_commands), [0.1, 0.1, 0.1, 0.7])
        qc = query_commands[qcgen.get_indx()]
        query_tables = set()
        table_columns = {}
        condition_table_columns = {}
        tgen1 = uniformGenerator(self.table_size)
        tgen2 = reverseZipfGenerator(self.table_size, self.s)
        cgen1 = uniformGenerator(self.column_size)
        cgen2 = reverseZipfGenerator(self.column_size, self.s)
        if qc == 'SELECT':
            for k in xrange(self.table_size):
                query_tables.add(self.table_list[tgen1.get_indx()])
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen1.get_indx()])
                table_columns[table] = columns
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen1.get_indx()])
                condition_table_columns[table] = columns
        elif qc == 'INSERT':
            query_tables.add(self.table_list[tgen1.get_indx()])
            for table in query_tables:
                table_columns[table] = set()
        elif qc == 'DELETE':
            query_tables.add(self.table_list[tgen1.get_indx()])
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen1.get_indx()])
                condition_table_columns[table] = columns
        elif qc == 'UPDATE':
            query_tables.add(self.table_list[tgen2.get_indx()])
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen2.get_indx()])
                table_columns[table] = columns
            for table in query_tables:
                columns = set()
                for k in xrange(self.column_size):
                    columns.add(self.column_list[cgen2.get_indx()])
                condition_table_columns[table] = columns
        else:
            # TODO
            pass
        query = self._assemble(qc, query_tables, table_columns, condition_table_columns)
        return (query, qc, table_columns, condition_table_columns)

    # 'T1#A1,A3,A2*T3#A9,A8,A7*T2#A5,A4,A6'
    def _pickle(self, f, role, command, table_columns, condition_table_columns):
        def pickle_dict_str_set(D):
            S = ''
            for k, v in D.items():
                S += str(k) + '#'
                for vv in v:
                    S += str(vv) + ','
                S = S.strip(',')
                S += '*'
            S = S.strip('*')
            return S

        table_columns_str, condition_table_columns_str = pickle_dict_str_set(table_columns), pickle_dict_str_set(condition_table_columns)
        f.write('%s|%s|%s|%s\n' % (role, command, table_columns_str, condition_table_columns_str))
    
    def generate(self, output, mode = 'query'):
        f = file(output, 'w')
        rgen = zipfGenerator(self.role_size, self.s)
        for k in xrange(self.query_size):
            role_indx = rgen.get_indx()
            role = self.role_list[role_indx]
            if role_indx == 0:
                (query, cmd, tpl, condition) = self._gen_first_role_data()
            elif role_indx == 1:
                (query, cmd, tpl, condition) = self._gen_second_role_data()
            elif role_indx == 2:
                (query, cmd, tpl, condition) = self._gen_third_role_data()
            else: # role_indx == 3
                (query, cmd, tpl, condition) = self._gen_forth_role_data()
            
            if mode == 'train':
                self._pickle(f, role, cmd, tpl, condition)
            elif mode == 'query':
                f.write('%s|%s|%s\n' % (role, query, condition))
            elif mode == 'tuple':
                f.write('%s|%s|%s\n' % (role, tpl, condition))
            elif mode == 'all':
                f.write('%s|%s|%s|%s\n' % (role, query, tpl, condition))
        f.close()


def usage():
    ZIPF_10_5 = zipfGenerator(10, 5.)
    for i in xrange(100):
        print ZIPF_10_5.get()
    RZIPF_10_5 = reverseZipfGenerator(10, 5.)
    print RZIPF_10_5.get_indx()
    

if __name__ == '__main__':
    #usage()
    data = dataGen(5000, 4, 100, 20, {'s': 3.})
    data.generate('train', 'train')
