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
