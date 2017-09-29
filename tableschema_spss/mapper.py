# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import re
import logging
import tableschema
log = logging.getLogger(__name__)


# Module API

class Mapper(object):

    # Public

    DATE_FORMAT = "%Y-%m-%d"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    TIME_FORMAT = "%H:%M:%S.%f"

    def convert_bucket(self, bucket):
        """Convert bucket to SPSS
        """
        if not bucket.endswith(('.sav', '.zsav')):
            bucket = '{}.sav'.format(bucket)
        return bucket

    def convert_descriptor(self, descriptor):
        """Convert descriptor to SPSS

        Return a dict of method args that can be used by savReaderWriter.SavWriter(),
        derived from the passed `descriptor`.

        """
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

            message = 'Field "{}" requires a "spss:format" property.'.format(name)
            raise tableschema.exceptions.StorageError(message)

        var_names = [f['name'] for f in descriptor['fields']]
        var_types = {n: get_spss_type_for_name(n) for n in var_names}
        formats = {n: get_format_for_name(n) for n in var_names if get_format_for_name(n)}
        return {'varNames': var_names, 'varTypes': var_types, 'formats': formats}

    def restore_descriptor(self, header):
        """Restore descriptor from SPSS

        Return a Schema descriptor from the passed SPSS header.  Includes a custom
        `spss:format` property which defines the SPSS format used for this field type.

        """
        fields = []
        for var in header.varNames:
            field_type = self.restore_type(header.formats[var])
            field = {
                'name': var,
                'type': field_type,
                'title': header.varLabels[var],
                'spss:format': header.formats[var]
            }
            date_formats = {
                'time': self.TIME_FORMAT,
                'date': self.DATE_FORMAT,
                'datetime': self.DATETIME_FORMAT
            }
            if field_type in date_formats.keys():
                field['format'] = date_formats[field_type]

            fields.append(field)
        return {'fields': fields}

    def restore_type(self, format):
        """Resoter type from SPSS

        Return a TableSchema type for the passed SPSS format.
        Use the SPSS_TYPE_MAPPING look up to match formats, return 'string' if unknown.

        """
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

        for ts_type, pattern in SPSS_TYPE_MAPPING:
            if pattern.match(format):
                return ts_type

        # Unknown format, return 'string'
        return 'string'
