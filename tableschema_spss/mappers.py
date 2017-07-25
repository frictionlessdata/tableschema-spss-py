import re

import tableschema

import logging
log = logging.getLogger(__name__)


def bucket_to_filename(bucket):
    '''Ensure bucket has appropriate file extension'''
    if not bucket.endswith(('.sav', '.zsav')):
        bucket = '{}.sav'.format(bucket)
    return bucket


def descriptor_to_varnames_and_vartypes(descriptor):
    TYPE_MAPPING = {
        'string': 1,
        'number': 0,
        'integer': 0,
        'boolean': 0,
        'array': 1,
        'object': 1,
        'date': 0,
        'time': 0,
        'datetime': 0,
        'year': 0,
        'yearmonth': 0,
        'geopoint': 0,
        'geojson': 0,
        'duration': 0,
        'any': 1,
    }

    schema = tableschema.Schema(descriptor)

    var_names = [f['name'] for f in descriptor['fields']]
    var_types = {n: TYPE_MAPPING[schema.get_field(n).type] for n in var_names}

    return var_names, var_types


def spss_header_to_descriptor(header):
    '''Return a Schema descriptor from the passed SPSS header.'''

    fields = []
    for var in header.varNames:
        field = {
            'name': var,
            'type': spss_type_format_to_schema_type(header.formats[var]),
            'title': header.varLabels[var]
        }
        fields.append(field)
    return {'fields': fields}


SPSS_TYPE_MAPPING = [
    ('string', re.compile(r'\bA\d+')),
    ('number', re.compile(r'\bF\d+\.\d+')),  # Basic decimal number
    ('number', re.compile(r'\b[E|N]\d+\.?\d*')),  # Exponent or N format number
    ('integer', re.compile(r'\bF\d+')),  # Integer (must come after Basic decimal in list)
    ('date', re.compile(r'\b[A|E|J|S]?DATE\d+')),  # Various date formats
    ('datetime', re.compile(r'\bDATETIME\d+')),
    ('time', re.compile(r'\bTIME\d+')),
    ('number', re.compile(r'\bDOLLAR\d+')),
    ('number', re.compile(r'\bPCT\d+'))  # Percentage format
]


def spss_type_format_to_schema_type(format):
    '''Return a TableSchema type for the passed SPSS format.

    Use the SPSS_TYPE_MAPPING look up to match formats, return 'string' if unknown.
    '''

    for ts_type, pattern in SPSS_TYPE_MAPPING:
        if pattern.match(format):
            return ts_type
    # Unknown format, return 'string'
    return 'string'
