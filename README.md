# tableschema-spss-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-spss-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-spss-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-spss-py/master.svg)](https://coveralls.io/r/frictionlessdata/tableschema-spss-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-spss.svg)](https://pypi.python.org/pypi/tableschema-spss)
[![Github](https://img.shields.io/badge/github-master-brightgreen)](https://github.com/frictionlessdata/tableschema-spss-py)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load SPSS files based on [Table Schema](http://specs.frictionlessdata.io/table-schema/) descriptors.

## Features

- implements `tableschema.Storage` interface

## Contents

<!--TOC-->

  - [Getting Started](#getting-started)
    - [Installation](#installation)
  - [Documentation](#documentation)
    - [With a base path](#with-a-base-path)
    - [Without a base path](#without-a-base-path)
    - [Reading .sav files](#reading-sav-files)
    - [Creating .sav files](#creating-sav-files)
  - [API Reference](#api-reference)
    - [`Storage`](#storage)
  - [Contributing](#contributing)
  - [Changelog](#changelog)

<!--TOC-->

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's highly recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
pip install tableschema-spss
```

## Documentation

> For this example your schema should be compatible with SPSS storage - https://github.com/frictionlessdata/tableschema-spss-py#creating-sav-files

```python
from tableschema import Table

# Load and save table to SPSS
table = Table('data.csv', schema='schema.json')
table.save('data', storage='spss', base_path='dir/path')
```

### With a base path

We can get storage with a specified base path this way:

```python
from tableschema_spss import Storage

storage_base_path = 'path/to/storage/dir'
storage = Storage(storage_base_path)
```

We can then interact with storage buckets ('buckets' are SPSS .sav/.zsav files in this context):

```python
storage.buckets  # list buckets in storage
storage.create('bucket', descriptor)
storage.delete('bucket')  # deletes named bucket
storage.delete()  # deletes all buckets in storage
storage.describe('bucket') # return tableschema descriptor
storage.iter('bucket') # yields rows
storage.read('bucket') # return rows
storage.write('bucket', rows)
```

### Without a base path

We can also create storage without a base path this way:

```python
from tableschema_spss import Storage

storage = Storage()  # no base path argument
```

Then we can specify SPSS files directly by passing their file path (instead of bucket names):

```python
storage.create('data/my-bucket.sav', descriptor)
storage.delete('data/my-bucket.sav')  # deletes named file
storage.describe('data/my-bucket.sav') # return tableschema descriptor
storage.iter('data/my-bucket.sav') # yields rows
storage.read('data/my-bucket.sav') # return rows
storage.write('data/my-bucket.sav', rows)
```

Note that storage without base paths does not maintain an internal list of buckets, so calling `storage.buckets` will return `None`.

### Reading .sav files

When reading SPSS data, SPSS date formats, `DATE`, `JDATE`, `EDATE`, `SDATE`, `ADATE`, `DATETIME`, and `TIME` are transformed into Python `date`, `datetime`, and `time` objects, where appropriate.

Other SPSS date formats, `WKDAY`, `MONTH`, `MOYR`, `WKYR`, `QYR`, and `DTIME` are not supported for native transformation and will be returned as strings.

### Creating .sav files

When creating SPSS files from Table Schemas, `date`, `datetime`, and `time` field types must have a format property defined with the following patterns:

- `date`: `%Y-%m-%d`
- `datetime`: `%Y-%m-%d %H:%M:%S`
- `time`: `%H:%M:%S.%f`

Table Schema descriptors passed to `storage.create()` should include a custom `spss:format` property, defining the SPSS type format the data is expected to represent. E.g.:

```json
{
    "fields": [
        {
            "name": "person_id",
            "type": "integer",
            "spss:format": "F8"
        },
        {
            "name": "name",
            "type": "string",
            "spss:format": "A10"
        },
        {
            "type": "number",
            "name": "salary",
            "title": "Current Salary",
            "spss:format": "DOLLAR8"
        },
        {
           "type": "date",
           "name": "bdate",
           "title": "Date of Birth",
           "format": "%Y-%m-%d",
           "spss:format": "ADATE10"
        }
    ]
}
```

## API Reference

### `Storage`
```python
Storage(self, base_path=None)
```
SPSS storage

Package implements
[Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage)
interface (see full documentation on the link):

![Storage](https://i.imgur.com/RQgrxqp.png)

> Only additional API is documented

__Arguments__
- __base_path (str)__:
        a valid directory path where .sav files can be created and read.
        If no base_path is provided, the Storage object methods
        will accept file paths rather than bucket names.


#### `storage.buckets`
List all .sav and .zsav files at base path.

Bucket list is only maintained if Storage has a valid base path,
otherwise will return None.

__Returns__

`str[]/None`: returns bucket list or None


## Contributing

> The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into active environment:

```bash
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions could be found in nicely formatted [commit history](https://github.com/frictionlessdata/tableschema-spss-py/commits/master).

#### v1.0

- Initial driver release

