# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import unittest
import os
import io
import json
from decimal import Decimal

import savReaderWriter

from tableschema_spss import Storage

import logging
log = logging.getLogger(__name__)


class TestBasePath(unittest.TestCase):

    def test_base_path_exists(self):
        '''Storage with existing base_path doesn't raise exception.'''
        try:
            Storage(base_path='data')
        except Exception as e:
            self.fail("Storage() raised Exception")

    def test_base_path_not_exists(self):
        '''base_path arg must exist.'''
        with self.assertRaises(RuntimeError):
            Storage(base_path='not_here')

    def test_base_path_must_not_be_file(self):
        '''base_path arg must be a directory.'''
        with self.assertRaises(RuntimeError):
            Storage(base_path='data/simple.json')


class TestStorageCreate(unittest.TestCase):

    TEST_BASE_PATH = 'data'
    TEST_FILE_NAME = 'simple.sav'
    TEST_FILE_PATH = os.path.join(TEST_BASE_PATH, TEST_FILE_NAME)

    def tearDown(self):
        if os.path.exists(self.TEST_FILE_PATH):
            os.remove(self.TEST_FILE_PATH)

    def test_storage_create_creates_file(self):
        '''Storage .sav file created by create()'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        storage.create(self.TEST_FILE_NAME, simple_descriptor)
        assert os.path.exists(self.TEST_FILE_PATH)

    def test_storage_create_protect_existing(self):
        '''create() called twice raises exception preventing overwrite of existing
        file.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        storage.create(self.TEST_FILE_NAME, simple_descriptor)
        with self.assertRaises(RuntimeError):
            storage.create(self.TEST_FILE_NAME, simple_descriptor)

    def test_storage_create_force_overwrite(self):
        '''create() called twice with force=True allows file overwrite.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        storage.create(self.TEST_FILE_NAME, simple_descriptor)
        try:
            storage.create(self.TEST_FILE_NAME, simple_descriptor, force=True)
        except RuntimeError:
            self.fail("create() raised RuntimeError")
        assert os.path.exists(self.TEST_FILE_PATH)

    def test_storage_create_metadata_headers(self):
        '''created .sav file has expected metadata headers'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        storage.create(self.TEST_FILE_NAME, simple_descriptor)

        with savReaderWriter.SavHeaderReader(self.TEST_FILE_PATH) as header:
            metadata = header.all()
            # self.assertEqual(metadata.formats, {'name': 'A12', 'person_id': 'F8.2'})
            # self.assertEqual(metadata.varNames, ['person_id', 'name'])
            # self.assertEqual(metadata.varTypes, {'name': 12, 'person_id': 0})


class TestStorageDescribe(unittest.TestCase):

    TEST_BASE_PATH = 'data'

    def test_describe(self):
        '''Return the expected schema descriptor.'''
        expected_schema = json.load(io.open('data/Employee_expected_descriptor.json',
                                            encoding='utf-8'))
        storage = Storage(base_path=self.TEST_BASE_PATH)
        schema = storage.describe('Employee data.sav')
        self.assertEqual(expected_schema, schema)


class TestStorageIter(unittest.TestCase):

    TEST_BASE_PATH = 'data'
    EXPECTED_DATA = [
        [1, 'm', datetime.date(1952, 2, 3), 15, 3, Decimal('57000'), Decimal('27000'),
         98, 144, 0],
        [2, 'm', datetime.date(1958, 5, 23), 16, 1, Decimal('40200'), Decimal('18750'),
         98, 36, 0],
        [3, 'f', datetime.date(1929, 7, 26), 12, 1, Decimal('21450'), Decimal('12000'),
         98, 381, 0],
        [4, 'f', datetime.date(1947, 4, 15), 8, 1, Decimal('21900'), Decimal('13200'),
         98, 190, 0],
        [5, 'm', datetime.date(1955, 2, 9), 15, 1, Decimal('45000'), Decimal('21000'),
         98, 138, 0],
        [6, 'm', datetime.date(1958, 8, 22), 15, 1, Decimal('32100'), Decimal('13500'),
         98, 67, 0],
        [7, 'm', datetime.date(1956, 4, 26), 15, 1, Decimal('36000'), Decimal('18750'),
         98, 114, 0],
        [8, 'f', datetime.date(1966, 5, 6), 12, 1, Decimal('21900'), Decimal('9750'),
         98, 0, 0],
        [9, 'f', datetime.date(1946, 1, 23), 15, 1, Decimal('27900'), Decimal('12750'),
         98, 115, 0],
        [10, 'f', datetime.date(1946, 2, 13), 12, 1, Decimal('24000'), Decimal('13500'),
         98, 244, 0]
    ]

    def test_iter(self):
        storage = Storage(base_path=self.TEST_BASE_PATH)
        for i, row in enumerate(storage.iter('Employee data.sav')):
            # Test the first 10 rows against the expected data.
            self.assertEqual(self.EXPECTED_DATA[i], row)
            if i == 9:
                break
