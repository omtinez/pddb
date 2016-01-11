==============
PandasDatabase
==============

.. image:: https://img.shields.io/pypi/v/pddb.svg
        :target: https://pypi.python.org/pypi/pddb

.. image:: https://img.shields.io/travis/omtinez/pddb.svg
        :target: https://travis-ci.org/omtinez/pddb

.. image:: https://readthedocs.org/projects/pddb/badge/?version=latest
        :target: https://readthedocs.org/projects/pddb/?badge=latest
        :alt: Documentation Status


Prototyping database engine for Python

* Free software: MIT License
* Documentation: https://pddb.readthedocs.org.

Introduction
------------

PandasDatabase is a RESTful database engine application built on top of Pandas. Essentially, it is
an abstraction layer that projects the database-table-column model into a very simple set of API's.
As a database engine, it has some useful features that make it a good candidate for prototype work:

* Inherits all the performance and robustness of Pandas.
* Very simple and intuitive API set.
* Tables support dynamic schema, so every time columns are changed during development there is no
  need to alter tables or CREATE statements.
* All data is persisted in plaintext, human-readable CSV format.

Some of those features come at a cost that probably makes PandasDatabase less than ideal for
production environments:

* Security. At the server, the security model is based on file permissions. For the API's,
  production environments should very likely never expose database API's of any form.
* Performance. While low latency production environments might not run into an issue,
  performance-critical applications will probably run into a bottleneck when writing to disk.
* Data types. Exposing all the table data in CSV format means that complex data types such as date
  cannot be supported.

The Problem
-----------

A very large number of small projects have fairly simple requirements for data storage. For all
those projects, interfacing with a database engine is boilerplate work that adds unnecessary
overhead during development. This project provides a very simple yet powerful solution that is
great for prototype work to enable those small projects to hit the ground running. Once the
critical components of the project are finished and a proof of concept version is running, projects
can transition to a more mature database engine that can better suit the needs of a production
environment.

Getting Started
---------------

This project is entirely Python based. To be able to use it, first install the dependencies::

    $ pip install pandas bottle

The easiest way to install PandasDatabase is using pip::

    $ pip install pddb

To fire up the database engine, simply run::

    $ python -m pddb.pddb dbname --permissions w

By default, the database is started in read-only mode, which is why we need to pass the
``--permissions w`` flag. This should start a bottle application with the following endpoints
available:

* ``/pddb``
* ``/pddb/find/<table>``
* ``/pddb/insert/<table>``
* ``/pddb/upsert/<table>``
* ``/pddb/delete/<table>``

The parameters to those endpoints can be passed as a GET query string, or via POST. For example,
to insert a new record, the user can simply visit the following URL once the database engine is
running::

    http://127.0.0.1:8080/pddb/insert/table_name?Name=John

Likewise, the user can find the inserted record by visiting::

    http://127.0.0.1:8080/pddb/find/table_name

Matching conditions can also be added::

    http://127.0.0.1:8080/pddb/find/table_name?Name=John

Performing an update is a little more complicated. Rather than exposing multiple API's, a single
API is used and the parameters are parsed to understand the user's desired operation. So, instead
of using ``/pddb/upsert/table_name?column_name=column_value``, the user must use
``/pddb/upsert/table_name?record__column_name=column_value&where__condition_name=condition_value``. Essentially,
prepend ``record__`` or ``where__`` to let the database engine know which pair of key-value corresponds
to what parameter. For example, to change the name ``John`` to ``Jane`` in our record, we can simply
visit::

    http://127.0.0.1:8080/pddb/upsert/table_name?record__Name=Jane&where__Name=John

Note that this also applies to the rest of the API's, even though the parameters being parsed
default to the most obvious choice. For example, ``/pddb/find`` assumes that the parameters in the
query string correspond to the equivalent of a ``WHERE`` in ``SQL``. However, the find query can also
be written as::

    http://127.0.0.1:8080/pddb/find/table_name?where__Name=John

The usefulness of this does not appear evident until more complex queries are used, such as
``WHERE-NOT``::

    http://127.0.0.1:8080/pddb/find/table_name?where_not__Name=John

While admittedly a bit quirky, these very simple API's allow for any application in any language to
interface with the database engine by performing very simple GET requests. The user does not need
to worry about exposing an API or interfacing with a database in another process or server, which
gives more time to developing the critical parts of the project first.

License
-------

Copyright (c) 2016 Oscar Martinez
All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT
OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.