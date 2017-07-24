# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
import io
import json

import savReaderWriter

from tableschema_spss import Storage

import logging
log = logging.getLogger(__name__)


# def test_storage():
#     # Get resources
#     # articles_descriptor = json.load(
#     #     io.open('data/articles.json', encoding='utf-8'))
#     # comments_descriptor = json.load(
#     #     io.open('data/comments.json', encoding='utf-8'))
#     # articles_rows = Stream('data/articles.csv', headers=1).open().read()
#     # comments_rows = Stream('data/comments.csv', headers=1).open().read()

#     # Storage
#     storage = Storage(base_path='data')

#     print(storage)


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
            self.assertEqual(metadata.formats, {'name': 'A12', 'person_id': 'F8.2'})
            self.assertEqual(metadata.varNames, ['person_id', 'name'])
            self.assertEqual(metadata.varTypes, {'name': 12, 'person_id': 0})
