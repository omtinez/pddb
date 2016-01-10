.. highlight:: shell

=====
Usage
=====

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
of using ``/pddb/upsert/column_name=column_value``, the user must use
``/pddb/upsert/record__column_name=column_value&where__condition_name=condition_value``. Essentially,
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
