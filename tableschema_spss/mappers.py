import re

import tableschema

import logging
log = logging.getLogger(__name__)


def bucket_to_filename(bucket):
    '''Ensure bucket has appropriate file extension'''
    if not bucket.endswith(('.sav', '.zsav')):
        bucket = '{}.sav'.format(bucket)
    return bucket


def descriptor_to_savreaderwriter_args(descriptor):
    '''Return a dict of method args that can be used by savReaderWriter.SavWriter(),
    derived from the passed `descriptor`.
    '''

    schema = tableschema.Schema(descriptor)

    def get_format_for_name(name):
        return schema.get_field(name).descriptor.get('spss:format')

    def get_spss_type_for_name(name):
        '''Return spss number type for `name`.

        First we try to get the spss format (A10, F8.2, etc), and derive the spss type
        from that.

        If there's not spss format defined, we see if the type is a number and return the
        appropriate type.

        All else fails, we return a string type of width 1 (the default string format is
        A1).
        '''
        spss_format = get_format_for_name(name)
        if spss_format:
            string_pattern = re.compile("(?P<printFormat>A(HEX)?)(?P<printWid>\d+)",
                                        re.IGNORECASE)
            is_string = string_pattern.match(spss_format)
            if is_string:
                # Return the 'width' discovered from the passed `format`.
                return int(is_string.group('printWid'))
            else:
                return 0
        else:
            descriptor_type = schema.get_field(name).type
            if descriptor_type == 'integer' or descriptor_type == 'number':
                return 0

        raise ValueError('Field "{}" requires a "spss:format" property.'.format(name))

    var_names = [f['name'] for f in descriptor['fields']]
    var_types = {n: get_spss_type_for_name(n) for n in var_names}
    formats = {n: get_format_for_name(n) for n in var_names if get_format_for_name(n)}
    return {'varNames': var_names, 'varTypes': var_types, 'formats': formats}


def spss_header_to_descriptor(header):
    '''Return a Schema descriptor from the passed SPSS header.

    Includes a custom `spss:format` property which defines the SPSS format used for this
    field type.
    '''
    fields = []
    for var in header.varNames:
        field = {
            'name': var,
            'type': spss_type_format_to_schema_type(header.formats[var]),
            'title': header.varLabels[var],
            'spss:format': header.formats[var]
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
