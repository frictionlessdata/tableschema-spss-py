import io
import json
import unittest
import tableschema
from tableschema_spss.mapper import Mapper


# Tests

def test_mapper_convert_bucket():
    mapper = Mapper()
    assert mapper.convert_bucket('bucket') == 'bucket.sav'
    assert mapper.convert_bucket('bucket.sav') == 'bucket.sav'
    assert mapper.convert_bucket('bucket.zsav') == 'bucket.zsav'


class TestMapperConvertDescriptor(unittest.TestCase):

    def test_convert_descriptor_simple_descriptor(self):
        '''A simple schema returns expected kwarg dict.'''
        mapper = Mapper()
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))
        kwargs = mapper.convert_descriptor(simple_descriptor)

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

    def test_convert_descriptor_missing_number_formats(self):
        '''Missing formats from number types should be discoverable.'''
        mapper = Mapper()
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))
        for f in simple_descriptor["fields"]:
            # Remove the format from person_id and name
            if f['name'] == 'person_id':
                del f['spss:format']
        kwargs = mapper.convert_descriptor(simple_descriptor)

        expected = {
            'formats': {u'bdate': u'ADATE10', u'name': u'A10', u'salary': u'DOLLAR8',
                        u'var_datetime': u'DATETIME19', u'var_time': u'TIME10'},
            'varNames': [u'person_id', u'name', u'salary', u'bdate', u'var_datetime',
                         u'var_time'],
            'varTypes': {u'bdate': 0, u'name': 10, u'person_id': 0, u'salary': 0,
                         u'var_datetime': 0, u'var_time': 0}
        }

        self.assertEqual(kwargs, expected)

    def test_convert_descriptor_missing_string_formats(self):
        '''Missing formats from string types raise error.'''
        mapper = Mapper()
        simple_descriptor = json.load(io.open('data/simple.json', encoding='utf-8'))
        for f in simple_descriptor["fields"]:
            # Remove the format from person_id and name
            if f['name'] == 'name':
                del f['spss:format']

        with self.assertRaises(tableschema.exceptions.StorageError):
            mapper.convert_descriptor(simple_descriptor)


class TestMapperRestoreType(unittest.TestCase):

    def test_restore_type_string(self):
        mapper = Mapper()
        self.assertEqual(mapper.restore_type('A1'), 'string')
        self.assertEqual(mapper.restore_type('A2'), 'string')
        self.assertEqual(mapper.restore_type('A9'), 'string')
        self.assertEqual(mapper.restore_type('A10'), 'string')
        self.assertEqual(mapper.restore_type('A128'), 'string')

    def test_restore_type_integer(self):
        mapper = Mapper()
        self.assertEqual(mapper.restore_type('F1'), 'integer')
        self.assertEqual(mapper.restore_type('F2'), 'integer')
        self.assertEqual(mapper.restore_type('F9'), 'integer')
        self.assertEqual(mapper.restore_type('F10'), 'integer')
        self.assertEqual(mapper.restore_type('F128'), 'integer')

    def test_restore_type_number(self):
        mapper = Mapper()

        # Fortran numbers
        self.assertEqual(mapper.restore_type('F1.0'), 'number')
        self.assertEqual(mapper.restore_type('F2.1'), 'number')
        self.assertEqual(mapper.restore_type('F9.2'), 'number')
        self.assertEqual(mapper.restore_type('F10.3'), 'number')
        self.assertEqual(mapper.restore_type('F128.5'), 'number')

        # Numbers with exponent
        self.assertEqual(mapper.restore_type('E1.0'), 'number')
        self.assertEqual(mapper.restore_type('E2'), 'number')
        self.assertEqual(mapper.restore_type('E2.1'), 'number')
        self.assertEqual(mapper.restore_type('E9.2'), 'number')
        self.assertEqual(mapper.restore_type('E10.3'), 'number')
        self.assertEqual(mapper.restore_type('E128.5'), 'number')

        # Numbers with leading zeros
        self.assertEqual(mapper.restore_type('N1.0'), 'number')
        self.assertEqual(mapper.restore_type('N2'), 'number')
        self.assertEqual(mapper.restore_type('N2.1'), 'number')
        self.assertEqual(mapper.restore_type('N9.2'), 'number')
        self.assertEqual(mapper.restore_type('N10.3'), 'number')
        self.assertEqual(mapper.restore_type('N128.5'), 'number')

        # Currency with possible currency symbols
        self.assertEqual(mapper.restore_type('DOLLAR8'), 'number')
        self.assertEqual(mapper.restore_type('DOLLAR10'), 'number')
        self.assertEqual(mapper.restore_type('DOLLAR8.2'), 'number')

        # Percentage
        self.assertEqual(mapper.restore_type('PCT2'), 'number')
        self.assertEqual(mapper.restore_type('PCT6.2'), 'number')
        self.assertEqual(mapper.restore_type('PCT8.2'), 'number')

    def test_restore_type_date(self):
        mapper = Mapper()
        self.assertEqual(mapper.restore_type('DATE10'), 'date')
        self.assertEqual(mapper.restore_type('ADATE10'), 'date')
        self.assertEqual(mapper.restore_type('SDATE10'), 'date')
        self.assertEqual(mapper.restore_type('JDATE10'), 'date')
        self.assertEqual(mapper.restore_type('EDATE10'), 'date')

        # Not a valid date
        self.assertEqual(mapper.restore_type('ZDATE10'), 'string')

    def test_restore_type_datetime(self):
        mapper = Mapper()
        self.assertEqual(mapper.restore_type('DATETIME20'), 'datetime')

    def test_restore_type_time(self):
        mapper = Mapper()
        self.assertEqual(mapper.restore_type('TIME8'), 'time')
