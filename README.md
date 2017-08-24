# tableschema-spss-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-spss-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-spss-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-spss-py/master.svg)](https://coveralls.io/r/frictionlessdata/tableschema-spss-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-spss.svg)](https://pypi.python.org/pypi/tableschema-spss)
[![SemVer](https://img.shields.io/badge/versions-SemVer-brightgreen.svg)](http://semver.org/)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Read and write between SPSS and Table Schema.


## Getting Started

### Installation

```bash
pip install tableschema-spss
```

### Storage

Package implements the [Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage) interface.

#### With a base path

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

#### Without a base path

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

#### Reading .sav files

When reading SPSS data, SPSS date formats, `DATE`, `JDATE`, `EDATE`, `SDATE`, `ADATE`, `DATETIME`, and `TIME` are transformed into Python `date`, `datetime`, and `time` objects, where appropriate.

Other SPSS date formats, `WKDAY`, `MONTH`, `MOYR`, `WKYR`, `QYR`, and `DTIME` are not supported for native transformation and will be returned as strings.

#### Creating .sav files

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
