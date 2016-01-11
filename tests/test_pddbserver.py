#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_pddbserver
----------------------------------

Tests for all the bottle server-related methods from the `pddb` module.
"""

import os
import sys
import json
import unittest2
from pddb import PandasDatabase

class TestPandasDatabaseServerMethods(unittest2.TestCase):
# pylint: disable=invalid-name,protected-access

    @classmethod
    def setUpClass(cls):
        cls.pddb = PandasDatabase('TestPandasDatabaseServerMethods', debug=True)
        cls.pddb_app = cls.pddb.bind_bottle_routes(default_permissions='w')
        cls.record = {'A': '1', 'B': '2', 'C': '3'}

    @classmethod
    def tearDownClass(cls):
        cls.pddb.drop_all()

    def test_pddb_start(self):
        tname = self.id().lower()

        # Find on empty table should return empty list
        res = self.pddb._action_callbacks['find'](tname)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.body, '[]')

    def test_pddb_insert(self):
        tname = self.id().lower()

        # Insert new record using insert
        res = self.pddb._action_callbacks['insert'](tname, request_fallback=self.record)
        self.assertEqual(res.status_code, 200)

        # Find all records
        res = self.pddb._action_callbacks['find'](tname)
        self.assertEqual(res.status_code, 200)

        # Confirm there's a single record
        db_records = json.loads(res.body)
        self.assertEqual(len(db_records), 1)

        # Compared found record with what should have been inserted
        record = dict(db_records[0])
        del record[PandasDatabase._id_colname]
        self.assertEqual(record, self.record)

        self.pddb.drop(tname)

    def test_pddb_upsert(self):
        tname = self.id().lower()

        # Insert new record using upsert
        res = self.pddb._action_callbacks['upsert'](tname, request_fallback=self.record)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(
            self.record, {k:v for k, v in json.loads(res.body)[0].items() if k != '__id__'})

        # Find all records
        res = self.pddb._action_callbacks['find'](tname)
        self.assertEqual(res.status_code, 200)

        # Confirm there's a single record
        db_records = json.loads(res.body)
        self.assertEqual(len(db_records), 1)

        # Compared found record with what should have been inserted
        id_key = PandasDatabase._id_colname
        record = dict(db_records[0])
        del record[id_key]
        self.assertEqual(record, self.record)

        # Update the inserted record adding a new columns
        record_id = str(db_records[0][id_key])
        req = {'where': {id_key: record_id}, 'record': {'D': '4'}}
        res = self.pddb._action_callbacks['upsert'](tname, request_fallback=req)
        self.assertEqual(res.status_code, 200)
        record_inserted = json.loads(res.body)[0]

        # Find only the inserted record
        req = {id_key: record_id}
        res = self.pddb._action_callbacks['find'](tname, request_fallback=req)
        self.assertEqual(res.status_code, 200)

        # Confirm there's a single record
        db_records = json.loads(res.body)
        self.assertEqual(len(db_records), 1)

        # Compared updated record with what should have been inserted
        record = dict(db_records[0])
        record_new = dict(self.record)
        record_new['D'] = '4'
        self.assertEqual({k:v for k, v in record.items() if k != '__id__'}, record_new)
        self.assertEqual({k:v for k, v in record_inserted.items() if k != '__id__'}, record_new)

        self.pddb.drop(tname)

if __name__ == '__main__':
    sys.exit(unittest2.main())
