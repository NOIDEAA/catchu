#! /usr/bin/env python

import sys

class record(object):
    def __init__(self, role, cmd, table_columns):
        self.role = role
        self.cmd = cmd
        self.table_columns = table_columns

    def check_print(self):
        print self.role
        print self.cmd
        print self.table_columns

class sql_nbc(object):
    def __init__(self, filename):
        self.filename = filename
        self._load()
        self.cntMap = {}

    def _load(self):
        f = file(self.filename)
        self.records = []
        for line in f:
            lst = line.split('|')
            if len(lst) <= 3:
                print 'Error Record!'
                sys.exit(1)
            role, cmd = lst[0], lst[1]
            table_columns = {}
            if cmd == 'SELECT':
                tables = lst[2].split('*')
                for table_column_str in tables:
                    tmp_lst = table_column_str.split('#')
                    table = tmp_lst[0]
                    column_lst = tmp_lst[1].split(',')
                    table_columns[table] = set(tmp_lst[1].split(',')) 
            elif cmd == 'INSERT':
                table_columns[lst[2][:-1]] = set()
            elif cmd == 'DELETE':
                tmp = lst[2].split('#')
                table_columns[tmp[0]] = set()
            else:
                # UPDATE
                tmp = lst[2].split('#')
                table_columns[tmp[0]] = set(tmp[1].split(','))
            self.records.append(record(role, cmd, table_columns))
        f.close()

    def check_print(self):
        for record in self.records:
            print '###################'
            record.check_print()

    def train(self):
        def setMap(key):
            if self.cntMap.get(key) == None:
                self.cntMap[key] = 1
            else:
                self.cntMap[key] += 1
        for record in self.records:
            key1 = record.role
            setMap(key1)
            key2 = '%s|%s' % (record.role, record.cmd)
            setMap(key2)
            table_columns = record.table_columns
            for table, columns in table_columns.items():
                column_str = ','.join(columns)
                table_column_str = '%s#%s' % (table, column_str)
                key3 = '%s|%s' % (record.role, table_column_str)
                setMap(key3)
        for k, v in self.cntMap.items():
            tmp = k.split('|')
            if tmp[-1] in {'SELECT', 'INSERT', 'DELETE', 'UPDATE'}:
                self.cntMap[k] = float(v) / float(self.cntMap[tmp[0]])
            elif len(tmp) != 1:
                self.cntMap[k] = float(v) / float(self.cntMap[tmp[0]])
        for k, v in self.cntMap.items():
            if len(k.split('|')) == 1:
                self.cntMap[k] = float(v) / float(len(self.records))
        print self.cntMap

    def dump_result(self):
        f1 = file('model0', 'w')
        f2 = file('model1', 'w')
        f3 = file('model2', 'w')
        f4 = file('model3', 'w')
        for k, v in self.cntMap.items():
            tmp = k.split('|')
            if len(tmp) == 1:
                f1.write('%s\t%s\n' % (k, v * len(self.records)))
                f2.write('%s\t%s\n' % (k, v))
            elif tmp[-1] in {'SELECT', 'INSERT', 'DELETE', 'UPDATE'}:
                f3.write('%s\t%s\n' % (k, v))
            else:
                f4.write('%s\t%s\n' % (k, v))
        f1.close()
        f2.close()
        f3.close()
        f4.close()


if __name__ == '__main__':
    obj = sql_nbc('/Users/wuhong/Desktop/2016-Pivotal/hackday/catchu/train')
    #obj.check_print()
    obj.train()
    obj.dump_result()
