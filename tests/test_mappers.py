import unittest

from tableschema_spss import mappers


def test_bucket_to_filename():
    assert mappers.bucket_to_filename('bucket') == 'bucket.sav'
    assert mappers.bucket_to_filename('bucket.sav') == 'bucket.sav'
    assert mappers.bucket_to_filename('bucket.zsav') == 'bucket.zsav'


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
