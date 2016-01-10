#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_pddb
----------------------------------

Tests for all the database engine methods from `pddb` module.
"""

import os
import re
import sys
import time
import json
from shutil import rmtree
import unittest2
from pddb import PandasDatabase

# TODO: delete performance
# TODO: find non-existent column in dynamic schema
# TODO: find should not create table using auto_load -> Won't fix, will be handled by save
# TODO: rename row generator -> record generator
# TODO: add where_not tests
# TODO: add read_csv test
# TODO: double load
# TODO: save() saves  only dataframes that have been changed
# TODO: DeprecationWarning Using a non-integer number will result in an error in the future
# TODO: insert 2 items, delete 1 item by id, delete 1 item by id

class TestPandasDatabaseMethods(unittest2.TestCase):
# pylint: disable=invalid-name,too-many-public-methods,protected-access

    @classmethod
    def setUpClass(cls):
        cls.pddb = None
        cls.cols = ['A', 'B', 'C']
        cls.tname = 'table_name'

    @classmethod
    def tearDownClass(cls):
        if cls.pddb is not None and cls.pddb.persistent:
            cls.pddb.drop_all()

    def test_create_database(self):
        test_name = self.id()
        self.pddb = PandasDatabase(self.id(), dynamic_schema=False, auto_load=False,
                                   auto_save=False, persistent=True, debug=False)
        self.assertTrue(os.path.exists(test_name.lower()))
        rmtree(test_name.lower())

        self.pddb.drop_all()
        self.pddb = None

    def test_find_one(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=True)

        record = {'col_name': 'A_1'}
        record_from_insert = self.pddb.insert(
            self.tname, record, columns='col_name', astype='dict')
        record_from_findone = self.pddb.find_one(
            self.tname, where=record, columns='col_name', astype='dict')
        json_from_record = json.dumps(record)
        json_from_findone = json.dumps(record_from_findone)
        json_from_insert = json.dumps(record_from_insert)

        self.assertEqual(json_from_record, json_from_insert)
        self.assertEqual(json_from_record, json_from_findone)

        self.pddb.drop_all()
        self.pddb = None

    def test_find_one_none(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=True)

        record = self.pddb.find_one(self.tname, where={'my_cond': None}, astype='json')

        self.assertEqual(record, json.dumps(dict()))

        self.pddb.drop_all()
        self.pddb = None

    def test_create_table_with_fixed_schema(self):
        test_name = self.id()
        schema = {self.tname: self.cols}
        self.pddb = PandasDatabase(test_name, dynamic_schema=False, astype=list, auto_load=False,
                                   auto_save=False, persistent=False, debug=False)
        self.pddb.load(table_schemas=schema)

        db_cols = set(self.pddb._db[self.tname].columns)
        cols_with_id = set(['__id__'] + [c for c in self.cols])

        self.assertEqual(db_cols, cols_with_id)

        self.pddb.drop_all()
        self.pddb = None

    def test_single_insert_with_fixed_schema(self):
        test_name = self.id()
        schema = {self.tname: self.cols}
        self.pddb = PandasDatabase(test_name, dynamic_schema=False, astype='dict', auto_load=False,
                                   auto_save=False, persistent=False, debug=False)
        self.pddb.load(table_schemas=schema)

        record = {c: '%s_%d' % (c, i) for (i, c) in enumerate(self.cols)}
        record = self.pddb.insert(self.tname, record)
        record_id = record['__id__']

        record_db = self.pddb.find(self.tname, where={'__id__': record_id})[0]
        self.assertEqual(record, record_db)

        self.pddb.drop_all()
        self.pddb = None

    def test_many_insert_with_fixed_schema(self):
        test_name = self.id()
        schema = {self.tname: self.cols}
        self.pddb = PandasDatabase(test_name, dynamic_schema=False, auto_load=False,
                                   auto_save=False, persistent=False, debug=False)
        self.pddb.load(table_schemas=schema)

        test_record_store = []
        for i in range(10):
            record = {c: '%s_%d' % (c, i) for c in self.cols}
            record_db = self.pddb.insert(self.tname, record)
            record_id = record_db.loc['__id__']
            record['__id__'] = record_id
            test_record_store.append(record)

        rows = self.pddb.find(self.tname, astype='dict')
        self.assertEqual(rows, test_record_store)

        self.pddb.drop_all()
        self.pddb = None

    def test_single_upsert_with_fixed_schema(self):
        test_name = self.id()
        schema = {self.tname: self.cols}
        self.pddb = PandasDatabase(test_name, dynamic_schema=False, astype='dict', auto_load=False,
                                   auto_save=False, persistent=False, debug=False)
        self.pddb.load(table_schemas=schema)

        # Test insert first
        record = {c: '%s_%d' % (c, i) for (i, c) in enumerate(self.cols)}
        record = self.pddb.upsert(self.tname, record=record)[0]
        record_id = record['__id__']

        record_db = self.pddb.find_one(self.tname, where={'__id__': record_id})
        self.assertEqual(record, record_db)

        # Test update second
        record_new = {c: '%s_%d' % (c, -i) for (i, c) in enumerate(self.cols)}
        record_new = self.pddb.upsert(self.tname, record=record_new, where={'__id__': record_id})[0]

        record_db = self.pddb.find(self.tname, where={'__id__': record_id})[0]
        self.assertNotEqual(record, record_new)
        self.assertEqual(record_db, record_new)

        self.pddb.drop_all()
        self.pddb = None

    def test_many_upsert_with_fixed_schema(self):
        test_name = self.id()
        schema = {self.tname: self.cols}
        self.pddb = PandasDatabase(test_name, dynamic_schema=False, auto_load=False,
                                   auto_save=False, persistent=False, debug=False)
        self.pddb.load(table_schemas=schema)

        test_record_store = []
        for i in range(10):
            record = {c: '%s_%d' % (c, i) for c in self.cols}
            record_new = self.pddb.upsert(self.tname, record=record, astype='dict')[0]

            for c in self.cols:
                record_new[c] = '%s_%d' % (c, -i)

            self.pddb.upsert(self.tname, record=record_new, where={'__id__': record_new['__id__']})
            test_record_store.append(record_new)

        rows = self.pddb.find(self.tname, astype='dict')
        self.assertEqual(rows, test_record_store)

        self.pddb.drop_all()
        self.pddb = None

    def test_create_table_with_dynamic_schema(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=False)

        for i, c in enumerate(self.cols):
            record = {c:i}
            self.pddb.insert(self.tname, record)

        PandasDatabase_cols = set(self.pddb._db[self.tname].columns)
        cols_with_id = set(['__id__'] + [c for c in self.cols])

        self.assertEqual(PandasDatabase_cols, cols_with_id)

        self.pddb.drop_all()
        self.pddb = None

    def test_astype(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=False)

        for i in range(10):
            self.pddb.insert(self.tname, {c: '%s_%d' % (c, i) for c in self.cols})

        self.assertEqual(str(self.pddb.find(self.tname)),
                         str(self.pddb.find(self.tname, astype='dataframe')))
        self.assertEqual(str(self.pddb.find(self.tname, astype=dict)),
                         str(self.pddb.find(self.tname, astype='dict')))
        self.assertEqual(str(self.pddb.find(self.tname, astype=str)),
                         str(self.pddb.find(self.tname, astype='json')))
        self.assertRaisesRegex(RuntimeError, '.*',
                               lambda: self.pddb.find_one(self.tname, astype='dataframe'))

        self.pddb.drop_all()
        self.pddb = None

    def test_illegal_column_name(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, astype=list, auto_load=True,
                                   auto_save=False, persistent=False, debug=False)

        record = {'col*name': 'A_1'}
        insert_function = lambda: self.pddb.insert(self.tname, record)
        self.assertRaisesRegex(ValueError, 'Column names must match the following regex: ".+"',
                               insert_function)

        self.pddb.drop_all()
        self.pddb = None

    def test_find_regex(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=False)

        for i in range(10):
            self.pddb.insert(self.tname, record={'col_name': i})

        rows = self.pddb.find(self.tname, where={'col_name': re.compile(r'[1-5]')})
        self.assertEqual(len(rows), 5)

        self.pddb.drop_all()
        self.pddb = None

    def test_upsert_to_insert_with_where(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=True)

        record = self.pddb.upsert(self.tname, record={'col_name_1': '1'},
                                  where={'col_name_2': '2'}, columns=['col_name_1', 'col_name_2'],
                                  astype='dict')
        self.assertEqual(record[0], {'col_name_1': '1', 'col_name_2': '2'})

        self.pddb.drop_all()
        self.pddb = None

    def test_upsert_to_insert_with_conflict(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=True)

        newrower = lambda: {'col_name_2': '2'}
        self.pddb.load(table_rowgens={self.tname: newrower})

        insert_function = lambda: self.pddb.upsert(self.tname, record={'col_name_1': '1'},
                                                   where_not={'col_name_2': '2'})
        self.assertRaisesRegex(ValueError, 'Cannot insert new record because default '
                               'values conflict with conditions provided: {.+}', insert_function)

        self.pddb.drop_all()
        self.pddb = None

    def test_create_table_with_upper_case(self):
        test_name = self.id()
        tname = self.tname.upper()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=False,
                                   auto_save=False, persistent=False, debug=False)

        newrower = lambda: {c: '%s_%d' % (c, 0) for c in self.cols}
        self.pddb.load(table_rowgens={tname: newrower})

        row = newrower()
        record = self.pddb.insert(tname, columns=self.cols, astype='dict')

        self.assertEqual(row, record)

        self.pddb.drop_all()
        self.pddb = None

    def test_find_using_columns(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=False,
                                   auto_save=False, persistent=False, debug=False)
        self.pddb.load(table_names=self.tname)

        self.pddb.insert(self.tname, {c: '%s_%d' % (c, i) for (i, c) in enumerate(self.cols)})
        record_A = self.pddb.find_one(self.tname, columns=['A'])
        record_B = self.pddb.find_one(self.tname, columns='B')
        record_ABC = self.pddb.find(self.tname, columns=['A', 'B', 'C'], astype='dict')[0]

        cols_A = sorted(list(record_A.keys()))
        cols_B = sorted(list(record_B.keys()))
        cols_ABC = sorted(list(record_ABC.keys()))

        self.assertEqual(cols_A, ['A'])
        self.assertEqual(cols_B, ['B'])
        self.assertEqual(cols_ABC, ['A', 'B', 'C'])

        self.pddb.drop_all()
        self.pddb = None

    def test_find_where_in(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=False)

        record_store = []
        for i in range(10):
            record = {c: '%s_%d' % (c, i) for c in self.cols}
            self.pddb.insert(self.tname, record)
            record_store.append(record)

        search_for = {self.cols[0]: ['%s_%d' % (self.cols[0], i) for i in range(5)]}
        results = self.pddb.find(self.tname, where=search_for, columns=self.cols, astype='dict')

        self.assertEqual(record_store[:5], results)

        self.pddb.drop_all()
        self.pddb = None

    def test_single_delete_record(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=False,
                                   auto_save=False, persistent=False, debug=False)
        self.pddb.load(table_names=self.tname)

        for i in range(10):
            self.pddb.insert(self.tname, {c: '%s_%d' % (c, i) for c in self.cols})

        c = self.cols[0]
        firstrecord = {c: '%s_%d' % (c, 0)}
        delrows = self.pddb.delete(self.tname, where=firstrecord)
        allrows = self.pddb.find(self.tname)
        allvalA = allrows['A'].values

        self.assertEqual(1, len(delrows))
        self.assertEqual(9, len(allrows))
        self.assertFalse('A_0' in allvalA)

        self.pddb.drop_all()
        self.pddb = None

    def test_single_rowgen(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=False,
                                   auto_save=False, persistent=False, debug=False)

        newrower = lambda: {c: '%s_%d' % (c, 0) for c in self.cols}
        self.pddb.load(table_rowgens={self.tname: newrower})

        row = newrower()
        record = self.pddb.insert(self.tname, columns=self.cols, astype='dict')

        self.assertEqual(row, record)

        self.pddb.drop_all()

    def test_fixed_schema_fail_column(self):
        test_name = self.id()
        schema = {self.tname: self.cols}
        self.pddb = PandasDatabase(test_name, dynamic_schema=False, auto_load=False,
                                   auto_save=False, persistent=False, debug=False)
        self.pddb.load(table_schemas=schema)

        insert_function = lambda: self.pddb.insert(self.tname, record={'D':0})
        self.assertRaisesRegex(ValueError, 'Column "D" does not exist in schema for table "%s"' %
                               self.tname, insert_function)

        self.pddb.drop_all()
        self.pddb = None

    def test_drop_table(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=False)

        all_tables = []
        for i in range(10):
            tname = '%s_%d' % (self.tname, i)
            all_tables.append(tname)
            self.pddb.insert(tname, record={c: '%s_%d' % (c, i) for c in self.cols})

        tnames = self.pddb.get_table_names()
        self.assertEqual(sorted(tnames), sorted(all_tables))

        for i in range(5):
            tname = '%s_%d' % (self.tname, i)
            all_tables.remove(tname)
            self.pddb.drop(tname)

        tnames = self.pddb.get_table_names()
        self.assertEqual(sorted(tnames), sorted(all_tables))

        self.pddb.drop(all_tables)
        self.assertEqual(len(self.pddb.get_table_names()), 0)

        self.pddb.drop_all()
        self.pddb = None

    def test_save_then_drop_all(self):
        test_name = self.id()
        schema = {self.tname: self.cols}
        self.pddb = PandasDatabase(test_name, dynamic_schema=False, auto_load=False,
                                   auto_save=False, persistent=True, debug=False)
        self.pddb.load(table_schemas=schema)
        self.pddb.insert(self.tname, {c: '%s_%d' % (c, i) for (i, c) in enumerate(self.cols)})
        self.pddb.save()

        header_expected = ','.join(self.cols) + ',%s\n' % PandasDatabase._id_colname
        record_expected_regex = header_expected + \
            ','.join(['%s_%d' % (c, i) for (i, c) in enumerate(self.cols)]) + ',.+?\n'
        with open(os.path.join(test_name, self.tname + '.csv'), 'r') as f:
            record_csv = f.read()

        self.assertTrue(os.path.exists(test_name.lower()))
        self.pddb.drop_all()
        self.assertRegex(record_csv, record_expected_regex)
        self.assertFalse(os.path.exists(test_name.lower()))

        self.pddb.drop_all()
        self.pddb = None

    def test_defer_save_queue_max(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, astype='dict', auto_load=False,
                                   auto_save=True, deferred_save=True, persistent=True,
                                   debug=False)
        self.pddb.load(table_names=self.tname)

        i = 0
        for i in range(self.pddb.save_queue_max):
            self.pddb.insert(self.tname, {c: '%s_%d' % (c, i) for c in self.cols})

        self.assertFalse(os.path.exists(os.path.join(test_name.lower(), self.tname + '.csv')))
        self.pddb.insert(self.tname, {c: '%s_%d' % (c, i) for c in self.cols})
        time.sleep(1) # Give an extra second to actually save the file to disk
        self.assertTrue(os.path.exists(os.path.join(test_name.lower(), self.tname + '.csv')))

        self.pddb.drop_all()

    def test_defer_save_wait(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, astype='dict', auto_load=False,
                                   auto_save=True, deferred_save=True, persistent=True,
                                   debug=False)
        self.pddb.load(table_names=self.tname)

        for i in range(self.pddb.save_queue_max):
            self.pddb.insert(self.tname, {c: '%s_%d' % (c, i) for c in self.cols})

        self.assertFalse(os.path.exists(os.path.join(test_name.lower(), self.tname + '.csv')))
        time.sleep(self.pddb.save_wait + 1) # Give an extra second to actually save the file to disk
        self.assertTrue(os.path.exists(os.path.join(test_name.lower(), self.tname + '.csv')))

        self.pddb.drop_all()
        self.pddb = None

    '''
    def test_join(self):
        test_name = self.id()
        self.pddb = PandasDatabase(test_name, dynamic_schema=True, auto_load=True,
                                   auto_save=False, persistent=False, debug=True)

        employees = [{'id': 1, 'name': 'john', 'salary': 1, 'dept_id': 1},
                     {'id': 2, 'name': 'mary', 'salary': 7, 'dept_id': 1},
                     {'id': 3, 'name': 'alex', 'salary': 3, 'dept_id': 2}]

        departments = [{'id': 1, 'name': 'accounting'},
                       {'id': 2, 'name': 'marketing'},
                       {'id': 3, 'name': 'human resources'}]

        for emp in employees:
            self.pddb.insert('employees', record=emp)
        for dept in departments:
            self.pddb.insert('departments', record=dept)

        on_function = lambda emp, dept: emp['dept_id'] == dept['id']
        merged = self.pddb.join('employees', 'departments', how='inner', on=on_function)

        record_from_insert = self.pddb.insert(self.tname, record, columns='col_name', astype='dict')
        record_from_findone = self.pddb.find_one(self.tname, where=record, columns='col_name', astype='dict')
        json_from_record = json.dumps(record)
        json_from_findone = json.dumps(record_from_findone)
        json_from_insert = json.dumps(record_from_insert)

        self.assertEqual(json_from_record, json_from_insert)
        self.assertEqual(json_from_record, json_from_findone)

        self.pddb.drop_all()
        self.pddb = None
    '''

if __name__ == '__main__':
    sys.exit(unittest2.main())
