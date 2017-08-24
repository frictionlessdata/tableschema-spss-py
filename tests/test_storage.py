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
import six

from tableschema_spss import Storage

import logging
log = logging.getLogger(__name__)


def test_storage_repr():

    assert repr(Storage(base_path='data')) == 'Storage <data>'


class BaseTestClass(unittest.TestCase):

    @classmethod
    def get_base_path(cls):
        '''Making this a readonly property prevents it being overridden, and subsequently
        deleted!'''
        return 'data/delete_tests'

    def tearDown(self):
        # Remove all files under test path
        files = os.listdir(self.get_base_path())
        for f in files:
            os.remove(os.path.join(self.get_base_path(), f))

    @classmethod
    def setUpClass(cls):
        # Create directory for writing test files
        if not os.path.exists(cls.get_base_path()):
            os.mkdir(cls.get_base_path())

    @classmethod
    def tearDownClass(cls):
        # Remove test directory
        if os.path.exists(cls.get_base_path()):
            os.rmdir(cls.get_base_path())


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

    def test_base_path_no_path_defined(self):
        '''No base_path defined shouldn't raise exception'''
        try:
            Storage()
        except Exception as e:
            print(e)
            self.fail("Storage() raised Exception")


class TestStorageCreate(BaseTestClass):

    TEST_BASE_PATH = BaseTestClass.get_base_path()
    TEST_FILE_NAME = 'test_simple.sav'
    TEST_FILE_PATH = os.path.join(TEST_BASE_PATH, TEST_FILE_NAME)

    def test_storage_create_creates_file(self):
        '''Storage .sav file created by create()'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        storage.create(self.TEST_FILE_NAME, simple_descriptor)
        assert os.path.exists(self.TEST_FILE_PATH)
        self.assertEqual(storage.buckets, ['test_simple.sav'])

    def test_storage_create_creates_file_no_base_path(self):
        '''Storage .sav file created by create() with no base_path'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage()
        storage.create(self.TEST_FILE_PATH, simple_descriptor)
        assert os.path.exists(self.TEST_FILE_PATH)
        # storage.buckets not maintained for storage with no base_path
        self.assertEqual(storage.buckets, None)

    def test_storage_create_protect_existing(self):
        '''create() called twice raises exception preventing overwrite of existing
        file.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        storage.create(self.TEST_FILE_NAME, simple_descriptor)
        with self.assertRaises(RuntimeError):
            storage.create(self.TEST_FILE_NAME, simple_descriptor)

    def test_storage_create_protect_existing_no_base_path(self):
        '''create() called twice raises exception preventing overwrite of existing
        file.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage()

        storage.create(self.TEST_FILE_PATH, simple_descriptor)
        with self.assertRaises(RuntimeError):
            storage.create(self.TEST_FILE_PATH, simple_descriptor)

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

    def test_storage_create_force_overwrite_no_base_path(self):
        '''create() called twice with force=True allows file overwrite.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage()

        storage.create(self.TEST_FILE_PATH, simple_descriptor)
        try:
            storage.create(self.TEST_FILE_PATH, simple_descriptor, force=True)
        except RuntimeError:
            self.fail("create() raised RuntimeError")
        assert os.path.exists(self.TEST_FILE_PATH)

    def test_storage_create_metadata_headers(self):
        '''created .sav file has expected metadata headers'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        storage.create(self.TEST_FILE_NAME, simple_descriptor)

        with savReaderWriter.SavHeaderReader(self.TEST_FILE_PATH, ioUtf8=True) as header:
            metadata = header.all()
            self.assertEqual(metadata.varNames, ['person_id', 'name', 'salary', 'bdate',
                                                 'var_datetime', 'var_time'])
            self.assertEqual(metadata.formats, {'name': 'A10', 'person_id': 'F8',
                                                'salary': 'DOLLAR8', 'bdate': 'ADATE10',
                                                'var_datetime': 'DATETIME19',
                                                'var_time': 'TIME10'})
            self.assertEqual(metadata.varTypes, {'name': 10, 'person_id': 0, 'bdate': 0,
                                                 'salary': 0, 'var_datetime': 0,
                                                 'var_time': 0})


class TestStorageWrite(BaseTestClass):

    TEST_BASE_PATH = BaseTestClass.get_base_path()
    TEST_FILE_NAME = 'test_simple.sav'
    TEST_FILE_PATH = os.path.join(TEST_BASE_PATH, TEST_FILE_NAME)

    def test_write_file_exists(self):
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        # create file with no rows
        storage.create(self.TEST_FILE_NAME, simple_descriptor)
        self.assertEqual(storage.read(self.TEST_FILE_NAME), [])

        rows = [[1, 'fred', Decimal('57000'), datetime.date(1952, 2, 3),
                 datetime.datetime(2010, 8, 11, 0, 0, 0), datetime.time(0, 0)]]
        storage.write(self.TEST_FILE_NAME, rows)

        self.assertEqual(storage.read(self.TEST_FILE_NAME), rows)

    def test_write_file_exists_no_base_path(self):
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage()

        # create file with no rows
        storage.create(self.TEST_FILE_PATH, simple_descriptor)
        self.assertEqual(storage.read(self.TEST_FILE_PATH), [])

        rows = [[1, 'fred', Decimal('57000'), datetime.date(1952, 2, 3),
                 datetime.datetime(2010, 8, 11, 0, 0, 0), datetime.time(0, 0)]]
        storage.write(self.TEST_FILE_PATH, rows)

        self.assertEqual(storage.read(self.TEST_FILE_PATH), rows)

    def test_write_file_doesnot_exist(self):
        '''Trying to write to a file that does not exist raises exception.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage(base_path=self.TEST_BASE_PATH)

        # create file with no rows
        storage.create(self.TEST_FILE_NAME, simple_descriptor)
        self.assertEqual(storage.read(self.TEST_FILE_NAME), [])

        # remove the file
        os.remove(self.TEST_FILE_PATH)

        rows = [[1, 'fred', Decimal('57000'), datetime.date(1952, 2, 3),
                 datetime.datetime(2010, 8, 11, 0, 0, 0), datetime.time(0, 0)]]
        # try to write to it
        with self.assertRaises(RuntimeError):
            storage.write(self.TEST_FILE_NAME, rows)

    def test_write_file_doesnot_exist_no_base_path(self):
        '''Trying to write to a file that does not exist raises exception.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))

        storage = Storage()

        # create file with no rows
        storage.create(self.TEST_FILE_PATH, simple_descriptor)
        self.assertEqual(storage.read(self.TEST_FILE_PATH), [])

        # remove the file
        os.remove(self.TEST_FILE_PATH)

        rows = [[1, 'fred', Decimal('57000'), datetime.date(1952, 2, 3),
                 datetime.datetime(2010, 8, 11, 0, 0, 0), datetime.time(0, 0)]]
        # try to write to it
        with self.assertRaises(RuntimeError):
            storage.write(self.TEST_FILE_PATH, rows)


class TestStorageDescribe(BaseTestClass):

    def test_describe(self):
        '''Return the expected schema descriptor.'''
        expected_schema = json.load(io.open('data/Employee_expected_descriptor.json',
                                            encoding='utf-8'))
        storage = Storage(base_path='data')
        schema = storage.describe('Employee data.sav')
        self.assertEqual(expected_schema, schema)

    def test_describe_no_base_path(self):
        '''Return the expected schema descriptor, with storage with no base_path.'''
        expected_schema = json.load(io.open('data/Employee_expected_descriptor.json',
                                            encoding='utf-8'))
        storage = Storage()
        # pass file path
        schema = storage.describe('data/Employee data.sav')
        self.assertEqual(expected_schema, schema)

    def test_describe_no_base_path_invalid(self):
        '''Attempting to describe an invalid file raises exception.'''
        storage = Storage()
        # pass file path
        with self.assertRaises(RuntimeError):
            storage.describe('data/no-file-here.sav')


class TestStorageIter_Read(BaseTestClass):

    READ_TEST_BASE_PATH = 'data'

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

    def _assert_rows(self, rows):
        for i, row in enumerate(rows):
            # Test the first 10 rows against the expected data.
            self.assertEqual(self.EXPECTED_DATA[i], row)
            if i == 9:
                break

    def test_iter(self):
        storage = Storage(base_path=self.READ_TEST_BASE_PATH)
        self._assert_rows(storage.iter('Employee data.sav'))

    def test_iter_no_base_path(self):
        storage = Storage()
        self._assert_rows(storage.iter('data/Employee data.sav'))

    def test_read(self):
        storage = Storage(base_path=self.READ_TEST_BASE_PATH)
        self._assert_rows(storage.read('Employee data.sav'))

    def test_read_no_base_path(self):
        storage = Storage()
        self._assert_rows(storage.read('data/Employee data.sav'))

    def test_read_no_base_path_invalid(self):
        storage = Storage()
        with self.assertRaises(RuntimeError):
            storage.read('data/no-file-here.sav')


class TestStorageRead_Dates(BaseTestClass):

    READ_TEST_BASE_PATH = 'data'

    EXPECTED_FIRST_ROW = \
        [datetime.datetime(2010, 8, 11, 0, 0), u'32 WK 2010', datetime.date(2010, 8, 11),
         u'3 Q 2010', datetime.date(2010, 8, 11), datetime.date(2010, 8, 11),
         u'156260 00:00:00', datetime.date(2010, 8, 11), u'August', u'August 2010',
         datetime.time(0, 0), datetime.date(2010, 8, 11), u'Wednesday']

    def test_read_date_file(self):
        '''Test various date formated fields from test file'''
        storage = Storage(base_path=self.READ_TEST_BASE_PATH)
        row = six.next(storage.iter('test_dates.sav'))
        self.assertEqual(row, self.EXPECTED_FIRST_ROW)

    def test_read_time_with_no_decimal(self):
        '''Test file containing time field with no decimals.'''
        storage = Storage(base_path=self.READ_TEST_BASE_PATH)
        expected_time = datetime.time(16, 0)
        row = six.next(storage.iter('test_time_no_decimal.sav'))
        self.assertEqual(row[2], expected_time)


class TestStorageDelete(BaseTestClass):

    TEST_BASE_PATH = BaseTestClass.get_base_path()
    TEST_FILE_PATH = os.path.join(TEST_BASE_PATH, 'delme.sav')
    SIMPLE_DESCRIPTOR = json.load(io.open('data/simple.json', encoding='utf-8'))

    def test_delete_file(self):
        storage = Storage(base_path=self.TEST_BASE_PATH)
        storage.create('delme.sav', self.SIMPLE_DESCRIPTOR)

        # File was created
        self.assertTrue(os.path.exists(self.TEST_FILE_PATH))
        self.assertEqual(storage.buckets, ['delme.sav'])

        storage.delete('delme.sav')

        # File was deleted
        self.assertFalse(os.path.exists(self.TEST_FILE_PATH))
        self.assertEqual(storage.buckets, [])

    def test_delete_file_no_base_path(self):
        storage = Storage()
        storage.create(self.TEST_FILE_PATH, self.SIMPLE_DESCRIPTOR)

        # File was created
        self.assertTrue(os.path.exists(self.TEST_FILE_PATH))
        self.assertEqual(storage.buckets, None)

        storage.delete(self.TEST_FILE_PATH)

        # File was deleted
        self.assertFalse(os.path.exists(self.TEST_FILE_PATH))
        self.assertEqual(storage.buckets, None)

    def test_delete_file_doesnot_exist(self):
        storage = Storage(base_path=self.TEST_BASE_PATH)
        storage.create('delme.sav', self.SIMPLE_DESCRIPTOR)

        # File was created
        self.assertTrue(os.path.exists(self.TEST_FILE_PATH))

        # File is removed externally
        os.remove(self.TEST_FILE_PATH)

        with self.assertRaises(RuntimeError):
            storage.delete('delme.sav')

    def test_delete_bucket_doesnot_exist(self):
        storage = Storage(base_path=self.TEST_BASE_PATH)
        storage.create('delme.sav', self.SIMPLE_DESCRIPTOR)

        # File was created
        self.assertTrue(os.path.exists(self.TEST_FILE_PATH))

        # File is removed externally
        os.remove(self.TEST_FILE_PATH)

        with self.assertRaises(RuntimeError):
            storage.delete('no-file-here.sav')

    def test_delete_file_doesnot_exist_no_base_path(self):
        storage = Storage()
        storage.create(self.TEST_FILE_PATH, self.SIMPLE_DESCRIPTOR)

        # File was created
        self.assertTrue(os.path.exists(self.TEST_FILE_PATH))

        # File is removed externally
        os.remove(self.TEST_FILE_PATH)

        with self.assertRaises(RuntimeError):
            storage.delete(self.TEST_FILE_PATH)

    def test_delete_file_doesnot_exist_ignore(self):
        storage = Storage(base_path=self.TEST_BASE_PATH)
        storage.create('delme.sav', self.SIMPLE_DESCRIPTOR)

        # File was created
        self.assertTrue(os.path.exists(self.TEST_FILE_PATH))

        # File is removed externally
        os.remove(self.TEST_FILE_PATH)

        # File was deleted
        self.assertFalse(os.path.exists(self.TEST_FILE_PATH))

        # Delete and ignore missing
        try:
            storage.delete('delme.sav', ignore=True)
        except(RuntimeError):
            self.fail('delete() shouldn\'t raise exception')

        self.assertEqual(storage.buckets, [])

    def test_delete_file_doesnot_exist_ignore_no_base_path(self):
        storage = Storage()
        storage.create(self.TEST_FILE_PATH, self.SIMPLE_DESCRIPTOR)

        # File was created
        self.assertTrue(os.path.exists(self.TEST_FILE_PATH))

        # File is removed externally
        os.remove(self.TEST_FILE_PATH)

        # File was deleted
        self.assertFalse(os.path.exists(self.TEST_FILE_PATH))

        # Delete and ignore missing
        try:
            storage.delete(self.TEST_FILE_PATH, ignore=True)
        except(RuntimeError):
            self.fail('delete() shouldn\'t raise exception')

        self.assertEqual(storage.buckets, None)

    def test_delete_all_files(self):
        storage = Storage(base_path=self.TEST_BASE_PATH)
        storage.create('delme.sav', self.SIMPLE_DESCRIPTOR)
        storage.create('delme_too.sav', self.SIMPLE_DESCRIPTOR)
        storage.create('delme_also.sav', self.SIMPLE_DESCRIPTOR)

        expected_buckets = ['delme.sav', 'delme_also.sav', 'delme_too.sav']

        self.assertEqual(os.listdir(self.TEST_BASE_PATH), expected_buckets)
        self.assertEqual(storage.buckets, expected_buckets)

        # no specified bucket, delete everything!
        storage.delete()

        self.assertEqual(os.listdir(self.TEST_BASE_PATH), [])
        self.assertEqual(storage.buckets, [])


class TestSafeFilePath(BaseTestClass):

    TEST_BASE_PATH = BaseTestClass.get_base_path()
    SIMPLE_DESCRIPTOR = json.load(io.open('data/simple.json', encoding='utf-8'))

    def test_valid_bucket_name(self):
        storage = Storage(base_path=self.TEST_BASE_PATH)
        try:
            storage.create('delme.sav', self.SIMPLE_DESCRIPTOR)
            storage.create('delme_too', self.SIMPLE_DESCRIPTOR)
        except RuntimeError:
            self.fail('Creating with a valid bucket name shouldn\'t fail')

    def test_invalid_bucket_name(self):
        storage = Storage(base_path=self.TEST_BASE_PATH)
        with self.assertRaises(RuntimeError):
            storage.create('../delme.sav', self.SIMPLE_DESCRIPTOR)

        with self.assertRaises(RuntimeError):
            storage.create('/delme.sav', self.SIMPLE_DESCRIPTOR)

        with self.assertRaises(RuntimeError):
            storage.create('/delme', self.SIMPLE_DESCRIPTOR)

        with self.assertRaises(RuntimeError):
            storage.create('../../delme', self.SIMPLE_DESCRIPTOR)
