# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os

import six
import tableschema
import savReaderWriter

from . import mappers


# Module API

class Storage(object):
    """SPSS Tabular Storage.

    An implementation of `tableschema.Storage`.

    Args:
        base_path (str): a valid file path where .sav files can be created and
        read.
    """

    def __init__(self, base_path):
        self.__descriptors = {}
        if not os.path.isdir(base_path):
            message = '"{}" is not a directory, or doesn\'t exist'.format(base_path)
            raise RuntimeError(message)
        self.__base_path = base_path
        # List all .sav and .zsav files at __base_path
        self.__buckets = self.__list_bucket_filenames()

    def __repr__(self):
        return 'Storage <{}>'.format(self.__base_path)

    def __list_bucket_filenames(self):
        '''Find .sav files at base_path and return bucket filenames'''
        return [f for f in os.listdir(self.__base_path) if f.endswith(('.sav', '.zsav'))]

    @property
    def buckets(self):
        '''List all .sav and .zsav files at __base_path'''
        return self.__buckets

    def create(self, bucket, descriptor, force=False):
        """Create bucket with descriptor.

        Parameters
        ----------
        bucket: str/list
            File name or list of file names.
        descriptor: dict/list
            TableSchema descriptor or list of descriptors.
        force: bool
            Will force creation of a new file, overwriting existing file with same name.

        Raises
        ------
        RuntimeError
            If file already exists.

        """

        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        descriptors = descriptor
        if isinstance(descriptor, dict):
            descriptors = [descriptor]
        assert len(buckets) == len(descriptors)

        # Check buckets for existence
        for bucket in reversed(self.buckets):
            if bucket in buckets:
                if not force:
                    message = 'File "%s" already exists.' % bucket
                    raise RuntimeError(message)
                self.delete(bucket)

        # Define buckets
        for bucket, descriptor in zip(buckets, descriptors):

            # Add to schemas
            self.__descriptors[bucket] = descriptor

            # Create .sav file
            tableschema.validate(descriptor)
            filename = mappers.bucket_to_filename(bucket)
            file_path = os.path.join(self.__base_path, filename)

            if not force and os.path.exists(file_path):
                message = 'File "%s" already exists.' % file_path
                raise RuntimeError(message)

            # map descriptor to sav header format so we can use the method below.
            args = mappers.descriptor_to_savreaderwriter_args(descriptor)
            writer = savReaderWriter.SavWriter(file_path, ioUtf8=True, **args)
            writer.close()

        self.__buckets = self.__list_bucket_filenames()

    def delete(self, bucket=None, ignore=False):
        pass

    def describe(self, bucket, descriptor=None):
        # Set descriptor
        if descriptor is not None:
            self.__descriptors[bucket] = descriptor

        # Get descriptor
        else:
            descriptor = self.__descriptors.get(bucket)
            if descriptor is None:
                filename = mappers.bucket_to_filename(bucket)
                file_path = os.path.join(self.__base_path, filename)
                with savReaderWriter.SavHeaderReader(file_path, ioUtf8=True) as header:
                    descriptor = mappers.spss_header_to_descriptor(header.all())

        return descriptor

    def iter(self, bucket):
        # Get response
        descriptor = self.describe(bucket)
        schema = tableschema.Schema(descriptor)
        filename = mappers.bucket_to_filename(bucket)
        file_path = os.path.join(self.__base_path, filename)

        # Yield rows
        with savReaderWriter.SavReader(file_path, ioUtf8=True, rawMode=False) as reader:
            for r in reader:
                row = []
                for i, field in enumerate(schema.fields):
                    value = r[i]
                    # Fix decimals that should be integers
                    if field.type == 'integer':
                        value = int(float(value))
                    # In Py3, date is returned as 'seconds since Gregorian epoch',
                    # transform it to a string date.
                    if field.type == 'date' and type(value) is float:
                        value = reader.spss2strDate(value, "%Y-%m-%d", None)
                    elif field.type == 'datetime' and type(value) is float:
                        value = reader.spss2strDate(value, "%Y-%m-%d %H:%M:%S", None)
                    elif field.type == 'time' and type(value) is float:
                        value = reader.spss2strDate(value, "%H:%M:%S.%f", None)
                    row.append(value)
                yield schema.cast_row(row)

    def read(self, bucket):
        return list(self.iter(bucket))

    def write(self, bucket, rows):
        pass
