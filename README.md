# tableschema-spss-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-spss-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-spss-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-spss-py/master.svg)](https://coveralls.io/r/frictionlessdata/tableschema-spss-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-spss.svg)](https://pypi.python.org/pypi/tableschema-spss)
[![SemVer](https://img.shields.io/badge/versions-SemVer-brightgreen.svg)](http://semver.org/)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Read and write between SPSS and Table Schema.


## Date and Time support

### Reading .sav files

When reading SPSS data, SPSS formats, `DATE`, `JDATE`, `EDATE`, `SDATE`, `ADATE`, `DATETIME`, and `TIME` are transformed into Python `date`, `datetime`, and `time` objects, where appropriate.

Other SPSS formats, `WKDAY`, `MONTH`, `MOYR`, `WKYR`, `QYR`, and `DTIME` are not supported for native transformation and will be returned as strings.

### Writing to .sav files

When writing SPSS data from Table Schemas, `date`, `datetime`, and `time` field types must have a format property defined with the following patterns:

- `date`: `%Y-%m-%d`
- `datetime`: `%Y-%m-%d %H:%M:%S`
- `time`: `%H:%M:%S.%f`
