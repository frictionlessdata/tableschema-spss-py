# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import six
import datetime
import tableschema
import savReaderWriter
from .mapper import Mapper


# Module API

class Storage(object):

    # Public

    def __init__(self, base_path=None):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """
        self.__descriptors = {}
        self.__buckets = None
        self.__mapper = Mapper()
        if base_path is not None and not os.path.isdir(base_path):
            message = '"{}" is not a directory, or doesn\'t exist'.format(base_path)
            raise RuntimeError(message)
        self.__base_path = base_path
        # List all .sav and .zsav files at __base_path
        if base_path:
            self.__reindex_buckets()

    def __repr__(self):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """
        return 'Storage <{}>'.format(self.__base_path)

    @property
    def buckets(self):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """
        return self.__buckets

    def create(self, bucket, descriptor, force=False):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        descriptors = descriptor
        if isinstance(descriptor, dict):
            descriptors = [descriptor]
        assert len(buckets) == len(descriptors)

        # Check buckets for existence
        if self.buckets:
            for bucket in reversed(self.buckets):
                if bucket in buckets:
                    if not force:
                        message = 'Bucket "%s" already exists.' % bucket
                        raise RuntimeError(message)
                    self.delete(bucket)

        # Define buckets
        for bucket, descriptor in zip(buckets, descriptors):

            # Add to schemas
            self.__descriptors[bucket] = descriptor

            # Create .sav file
            tableschema.validate(descriptor)
            file_path = self.__get_safe_file_path(bucket)

            if not force and os.path.exists(file_path):
                message = 'File "%s" already exists.' % file_path
                raise RuntimeError(message)

            # map descriptor to sav header format so we can use the method below.
            kwargs = self.__mapper.convert_descriptor(descriptor)
            writer = savReaderWriter.SavWriter(file_path, ioUtf8=True, **kwargs)
            writer.close()

        if self.buckets is not None:
            self.__reindex_buckets()

    def delete(self, bucket=None, ignore=False):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        elif bucket is None:
            buckets = reversed(self.buckets)

        # Iterate over buckets
        for bucket in buckets:
            # Check bucket exists
            if self.buckets is not None and bucket not in self.buckets:
                if not ignore:
                    message = 'Bucket "%s" doesn\'t exist.' % bucket
                    raise RuntimeError(message)

            # Remove corresponding descriptor
            if bucket in self.__descriptors:
                del self.__descriptors[bucket]

            file_path = self.__get_safe_file_path(bucket)
            if os.path.exists(file_path):
                os.remove(file_path)
            elif not ignore:
                message = 'File "%s" doesn\'t exist.' % file_path
                raise RuntimeError(message)

        if self.buckets is not None:
            self.__reindex_buckets()

    def describe(self, bucket, descriptor=None):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """

        # Set descriptor
        if descriptor is not None:
            self.__descriptors[bucket] = descriptor

        # Get descriptor
        else:
            descriptor = self.__descriptors.get(bucket)
            if descriptor is None:
                file_path = self.__get_safe_file_path(bucket, check_exists=True)
                with savReaderWriter.SavHeaderReader(file_path, ioUtf8=True) as header:
                    descriptor = self.__mapper.restore_descriptor(header.all())

        return descriptor

    def iter(self, bucket):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """

        # Prepare
        descriptor = self.describe(bucket)
        schema = tableschema.Schema(descriptor)
        file_path = self.__get_safe_file_path(bucket, check_exists=True)

        # Yield rows
        with savReaderWriter.SavReader(file_path, ioUtf8=False, rawMode=False) as reader:
            for r in reader:
                row = []
                for i, field in enumerate(schema.fields):
                    value = r[i]
                    # Fix decimals that should be integers
                    if field.type == 'integer' and value is not None:
                        value = int(float(value))
                    # We need to decode bytes to strings
                    if isinstance(value, six.binary_type):
                        value = value.decode(reader.fileEncoding)
                    # Time values need a decimal, add one if missing.
                    if field.type == 'time' and not re.search(r'\.\d*', value):
                            value = '{}.0'.format(value)
                    row.append(value)
                yield schema.cast_row(row)

    def read(self, bucket):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """
        return list(self.iter(bucket))

    def write(self, bucket, rows):
        """https://github.com/frictionlessdata/tableschema-spss-py#storage
        """
        file_path = self.__get_safe_file_path(bucket, check_exists=True)

        descriptor = self.describe(bucket)
        kwargs = self.__mapper.convert_descriptor(descriptor)

        schema = tableschema.Schema(descriptor)

        with savReaderWriter.SavWriter(file_path, mode=b"ab",
                                       ioUtf8=True, **kwargs) as writer:
            for r in rows:
                row = []
                for i, field in enumerate(schema.fields):
                    value = r[i]
                    if field.type == 'date' and isinstance(value, datetime.date):
                        value = writer.spssDateTime(
                            value.strftime(self.__mapper.DATE_FORMAT).encode(),
                            self.__mapper.DATE_FORMAT)
                    elif field.type == 'datetime' and isinstance(value, datetime.datetime):
                        value = writer.spssDateTime(
                            value.strftime(self.__mapper.DATETIME_FORMAT).encode(),
                            self.__mapper.DATETIME_FORMAT)
                    elif field.type == 'time' and isinstance(value, datetime.time):
                        value = writer.spssDateTime(
                            value.strftime(self.__mapper.TIME_FORMAT).encode(),
                            self.__mapper.TIME_FORMAT)
                    row.append(value)
                writer.writerow(row)

    # Private

    def __reindex_buckets(self):
        self.__buckets = sorted(self.__list_bucket_filenames())

    def __list_bucket_filenames(self):
        """Find .sav files at base_path and return bucket filenames
        """
        return [f for f in os.listdir(self.__base_path) if f.endswith(('.sav', '.zsav'))]

    def __get_safe_file_path(self, bucket, check_exists=False):
        """Return a file_path to `bucket` that doesn't traverse outside the base directory
        """

        if self.__base_path:
            # base_path exists, so `bucket` is relative to base_path
            filename = self.__mapper.convert_bucket(bucket)
            file_path = os.path.join(self.__base_path, filename)
            norm_file_path = os.path.normpath(file_path)
            if not norm_file_path.startswith(os.path.normpath(self.__base_path)):
                raise RuntimeError('Bucket name "{}" is not valid.'.format(bucket))
        else:
            norm_file_path = bucket

        if check_exists and not os.path.isfile(norm_file_path):
            # bucket isn't a valid file path, bail
            raise RuntimeError('File "{}" does not exist.'.format(norm_file_path))

        return norm_file_path
