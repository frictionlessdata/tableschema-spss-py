import io
import json
import unittest

from tableschema_spss import mappers


def test_bucket_to_filename():
    assert mappers.bucket_to_filename('bucket') == 'bucket.sav'
    assert mappers.bucket_to_filename('bucket.sav') == 'bucket.sav'
    assert mappers.bucket_to_filename('bucket.zsav') == 'bucket.zsav'


class TestDescriptorToWriterArgs(unittest.TestCase):

    def test_descriptor_to_savreaderwriter_args_simple_descriptor(self):
        '''A simple schema returns expected kwarg dict.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))
        kwargs = mappers.descriptor_to_savreaderwriter_args(simple_descriptor)

        expected = {
            'formats': {
                u'bdate': u'ADATE10',
                u'name': u'A10',
                u'person_id': u'F8',
                u'salary': u'DOLLAR8',
                u'var_datetime': u'DATETIME19',
                u'var_time': u'TIME10'
            },
            'varNames': [u'person_id', u'name', u'salary', u'bdate', u'var_datetime',
                         u'var_time'],
            'varTypes': {u'bdate': 0, u'name': 10, u'person_id': 0, u'salary': 0,
                         u'var_datetime': 0, u'var_time': 0}
        }

        self.assertEqual(kwargs, expected)

    def test_descriptor_to_savreaderwriter_args_missing_number_formats(self):
        '''Missing formats from number types should be discoverable.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))
        for f in simple_descriptor["fields"]:
            # Remove the format from person_id and name
            if f['name'] == 'person_id':
                del f['spss:format']
        kwargs = mappers.descriptor_to_savreaderwriter_args(simple_descriptor)

        expected = {
            'formats': {u'bdate': u'ADATE10', u'name': u'A10', u'salary': u'DOLLAR8',
                        u'var_datetime': u'DATETIME19', u'var_time': u'TIME10'},
            'varNames': [u'person_id', u'name', u'salary', u'bdate', u'var_datetime',
                         u'var_time'],
            'varTypes': {u'bdate': 0, u'name': 10, u'person_id': 0, u'salary': 0,
                         u'var_datetime': 0, u'var_time': 0}
        }

        self.assertEqual(kwargs, expected)

    def test_descriptor_to_savreaderwriter_args_missing_string_formats(self):
        '''Missing formats from string types raise error.'''
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))
        for f in simple_descriptor["fields"]:
            # Remove the format from person_id and name
            if f['name'] == 'name':
                del f['spss:format']

        with self.assertRaises(ValueError):
            mappers.descriptor_to_savreaderwriter_args(simple_descriptor)


class TestSPSSTypeMapper(unittest.TestCase):

    def test_spss_type_format_to_schema_type_string(self):
        self.assertEqual(mappers.spss_type_format_to_schema_type('A1'), 'string')
        self.assertEqual(mappers.spss_type_format_to_schema_type('A2'), 'string')
        self.assertEqual(mappers.spss_type_format_to_schema_type('A9'), 'string')
        self.assertEqual(mappers.spss_type_format_to_schema_type('A10'), 'string')
        self.assertEqual(mappers.spss_type_format_to_schema_type('A128'), 'string')

    def test_spss_type_format_to_schema_type_integer(self):
        self.assertEqual(mappers.spss_type_format_to_schema_type('F1'), 'integer')
        self.assertEqual(mappers.spss_type_format_to_schema_type('F2'), 'integer')
        self.assertEqual(mappers.spss_type_format_to_schema_type('F9'), 'integer')
        self.assertEqual(mappers.spss_type_format_to_schema_type('F10'), 'integer')
        self.assertEqual(mappers.spss_type_format_to_schema_type('F128'), 'integer')

    def test_spss_type_format_to_schema_type_number(self):
        # Fortran numbers
        self.assertEqual(mappers.spss_type_format_to_schema_type('F1.0'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('F2.1'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('F9.2'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('F10.3'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('F128.5'), 'number')

        # Numbers with exponent
        self.assertEqual(mappers.spss_type_format_to_schema_type('E1.0'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('E2'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('E2.1'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('E9.2'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('E10.3'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('E128.5'), 'number')

        # Numbers with leading zeros
        self.assertEqual(mappers.spss_type_format_to_schema_type('N1.0'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('N2'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('N2.1'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('N9.2'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('N10.3'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('N128.5'), 'number')

        # Currency with possible currency symbols
        self.assertEqual(mappers.spss_type_format_to_schema_type('DOLLAR8'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('DOLLAR10'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('DOLLAR8.2'), 'number')

        # Percentage
        self.assertEqual(mappers.spss_type_format_to_schema_type('PCT2'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('PCT6.2'), 'number')
        self.assertEqual(mappers.spss_type_format_to_schema_type('PCT8.2'), 'number')

    def test_spss_type_format_to_schema_type_date(self):
        self.assertEqual(mappers.spss_type_format_to_schema_type('DATE10'), 'date')
        self.assertEqual(mappers.spss_type_format_to_schema_type('ADATE10'), 'date')
        self.assertEqual(mappers.spss_type_format_to_schema_type('SDATE10'), 'date')
        self.assertEqual(mappers.spss_type_format_to_schema_type('JDATE10'), 'date')
        self.assertEqual(mappers.spss_type_format_to_schema_type('EDATE10'), 'date')

        # Not a valid date
        self.assertEqual(mappers.spss_type_format_to_schema_type('ZDATE10'), 'string')

    def test_spss_type_format_to_schema_type_datetime(self):
        self.assertEqual(mappers.spss_type_format_to_schema_type('DATETIME20'),
                         'datetime')

    def test_spss_type_format_to_schema_type_time(self):
        self.assertEqual(mappers.spss_type_format_to_schema_type('TIME8'), 'time')
