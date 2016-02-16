# -*- coding: utf-8 -*-

import os
import os.path
import re
import sys
import uuid
import argparse
import threading
from time import time, sleep
from shutil import rmtree
from pandas import Series, DataFrame, read_csv, isnull

# Python 2.x vs 3.x compatibility
def tostr(s, cast=False, enc='utf8'):
    if sys.version_info >= (3, 0, 0) and isinstance(s, bytes):
        s = s.decode(enc)
    elif sys.version_info < (3, 0, 0) and isinstance(s, unicode):
        s = s.encode(enc)
    elif not isinstance(s, str) and cast:
        s = str(s)
    return s

class PandasDatabase(object):
# pylint: disable=too-many-instance-attributes,attribute-defined-outside-init
    '''
    Base class of database engine.

    Each instance of this class corresponds to a database in the database-table-column model. To 
    run more than one database simultaneously, simply instantiate this class multiple times.

    All tables contain a special column named `__id__` used internally and that should not be
    modified by users. It is guaranteed to be unique for each database record.

    By default, the database will be persistent on disk and will attempt to load an existing
    database with the provided name. To run in memory only, pass the argument `persistent=False` to
    the constructor.

    Options
    -------
    The only mandatory parameter is the database name, all other options will fallback to their
    default values if they are not provided. The possible options are:
    debug : boolean (default `True`)
        When true, prints all the database engine operations to the console.
    astype : type or None (default `None`)
        Default type to be returned by the database engine operations. If None, the type will be
        whatever requires less type conversions internally but might be inconsistent depending on
        the type of operation (e.g. insert returns Series but find returns DataFrame). Possible
        values are: `nonetype`, `dataframe`, `series`, `str`, `dict`, `json`. Depending on the
        operation, `dataframe` or `series` might not be allowed and `dict` might be wrapped in a
        `list`.
    root_dir : str (default `os.getcwd()`)
        Root directory where the file operations will be run from when database is persistent.
    auto_load : boolean (default `True`)
        Attempt to load previously existing databases from the root directory upon start.
    auto_save: boolean (default `True`)
        Automatically save to disk after each operation that modifies records in the database.
    auto_cast: boolean (default `False`)
        Automatically cast not supported data types to string.
    dynamic_schema: boolean (default `True`)
        Automatically add or delete columns in all tables based on the schema assumed by 
        database operations. For example, inserting a record like `{"Name": "John"}` to a table
        that only has the default column `__id__`, will add a new column to the table which will
        result in the following schema: `["__id__", "Name"]`
    persistent: boolean (default `True`)
        Allow for the database operations to persist to disk. If false, calling `save()` fails.
    deferred_save: boolean (default `False`)
        When this and auto_save are true, saving will be delayed by `save_wait` seconds after the
        last operation or when the queue of unsaved operations reaches `save_queue_max`. This is an
        experimental feature meant to optimize performance by minimizing the number of disk write
        operations at the risk of partial data loss.
    save_wait : numeric (default `1`)
        When `deferred_save` is true, this is the numbers of seconds to wait after a `save()`. Each
        call to `save()` resets the timer to zero.
    save_queue_max : int (default `10`)
        Maximum length of the queue for unsaved operations. This is used to mitigate the maximum
        possible data loss.

    '''

    _id_colname = '__id__'
    _blank_schema = [_id_colname]
    _colname_rgx = r'^[a-zA-Z0-9_]*$'

    # Retrieve regex type at runtime for later comparison
    _regex_type = type(re.compile(''))

    def __init__(self, name, **kwargs):

        self._db = {}
        self._rowgens = {}
        self._schemas = {}
        self._lock = threading.Lock()
        self._save_last = 0
        self._save_queue_len = 0
        self._save_pending_flag = False

        self.defaults()
        for arg_key, arg_val in kwargs.items():
            setattr(self, arg_key, arg_val)

        # Normalize case, since Windows paths are case insensitive
        name = self._check_case_str(name, warn=True)
        self.name = name

        if self.persistent:
            self._create_db_folder()

        # Load all tables in the database folder
        if self.auto_load:
            table_folder = os.path.join(self.root_dir, self.name)
            if os.path.exists(table_folder):
                table_files = [tname for tname in os.listdir(table_folder) if tname[-4:] == '.csv']
                for tname in table_files:
                    self.load(tname[:-4])

    def __del__(self):
        for tname in list(self._db.keys()):
            del self._db[tname]
        for tname in list(self._schemas.keys()):
            del self._schemas[tname]
        for tname in list(self._rowgens.keys()):
            del self._rowgens[tname]

    def defaults(self):
        self.debug = True
        self.astype = None
        self.root_dir = os.getcwd()
        self.auto_load = True
        self.auto_save = True
        self.auto_cast = False
        self.dynamic_schema = True
        self.persistent = True
        self.deferred_save = False
        self.save_wait = 1
        self.save_queue_max = 10

    def get_table_names(self):
        ''' Returns the existing table names in this database '''
        return list(self._db.keys())

    def get_table_schema(self, tname):
        ''' Returns a list of column names of the provided table name '''
        tname = self._check_tname(tname, noload=True)
        if tname not in self._schemas:
            raise ValueError('Table "%s" not found in schema store' % tname)
        return list(self._schemas[tname])

    def _print(self, msg):
        if self.debug:
            print(msg)

    def _create_db_folder(self):

        # Create a folder for this database
        table_folder = os.path.join(self.root_dir, self.name)
        if not os.path.exists(table_folder):
            os.makedirs(table_folder)

    def load(self, table_names=None, table_schemas=None, table_rowgens=None):
        '''
        Initiates the tables, schemas and record generators for this database.

        Parameters
        ----------
        table_names : list of str, str or None
            List of tables to load into this database. If `auto_load` is true, inserting a record
            into a new table not provided here will automatically create that table.
        table_schemas : dict of <table_name, column_list> or None
            Dictionary with each table name as a key and a list of its columns as value. Any keys
            present here but not present in `table_names` will also trigger table creation, so
            table names provided in both parameters are redundant but harmless.
        table_rowgens: dict of <table_name, function> or None
            For all tables present in the keys of the provided dictionary, when an insert operation
            occurs, the corresponding function is called. The function must return a dictionary and
            is used as a "base record" which is complemented by the actual record being inserted.
            For example, when a table has a rowgen like `lambda: {"Timestamp": time.ctime()}` and
            a record like `{"Name": "John"}` is inserted, the database will then contain a record
            like `{"Timestamp": "Sun Jan 10 08:36:12 2016", "Name": "John"}`.
        '''

        # Check for table schemas
        if table_schemas is not None:
            table_schemas = self._check_case_dict(table_schemas, warn=True)
            for schema_key, schema_value in table_schemas.items():
                table_schemas[schema_key] = self._check_columns(schema_value, add_id=True)
        elif not self.dynamic_schema:
            raise ValueError('Table schemas must be provided if dynamic schema is disabled')

        # Check for row generators
        if table_rowgens is not None:
            table_rowgens = self._check_case_dict(table_rowgens, warn=True)

        # If table_names is not directly provided, infer it from one of the other parameters
        if table_names is None:
            if table_schemas is not None:
                table_names = list(table_schemas.keys())
                self._print(
                    'Inferring table name from table_schemas for tables %r'% table_names)
            elif table_rowgens is not None:
                table_names = list(table_rowgens.keys())
                self._print(
                    'Inferring table name from table_rowgens for tables %r' % table_names)
            else:
                req_params = 'table_names,table_schemas,table_rowgens'
                raise ValueError(
                    'At least one of the parameters must be provided: [%s]' % req_params)

        table_names = self._check_table_names(table_names, warn=True)
        self._print('Loading tables %r' % table_names)

        # Update schemas and row generators without losing previous ones
        for tname in table_names:
            if table_schemas is not None and tname in table_schemas:
                self._schemas[tname] = list(table_schemas[tname]) # make a copy
            if table_rowgens is not None and tname in table_rowgens:
                self._rowgens[tname] = table_rowgens[tname]

        with self._lock:
            for tname in table_names:

                # Standardize case, since Windows paths are case insensitive
                tname = self._check_case_str(tname, warn=True)

                # CSV has same filename as table under database folder
                tpath = os.path.join(self.root_dir, self.name, tname + '.csv')

                # Table already exists, simply load it
                if os.path.isfile(tpath):
                    if self.auto_load:
                        dataframe = read_csv(tpath, dtype=str)
                        self._db[tname] = dataframe
                        schema = self._check_columns(dataframe.columns.tolist())
                        self._schemas[tname] = schema
                    elif self.persistent:
                        raise ValueError(
                            'Auto load tables is disabled but table "%s" already exists and would'
                            'be overwritten' % tname)

                # Table not found, try to create it using given schema
                elif table_schemas is not None and tname in self._schemas:
                    self._db[tname] = DataFrame(columns=self._schemas[tname], dtype=str)

                # Table not found, dynamic schema
                elif self.dynamic_schema:
                    self._print('Creating table "%s" using dynamic schema' % tname)
                    self._db[tname] = DataFrame(columns=self._blank_schema, dtype=str)
                    self._schemas[tname] = list(self._blank_schema)

                # Table not found and schema not given when dynamic_schema not enabled
                else:
                    raise ValueError(
                        'Table %s not found and schema was not passed as a parameter' % tname)

    def drop(self, table_names):
        '''
        Drops the provided table(s) from this database.

        Parameters
        ----------
        table_names : list of str, str or None
            Table(s) to be dropped
        '''
        table_names = self._check_table_names(table_names)

        for tname in table_names:
            tname = self._check_tname(tname, noload=True)

            # Warning: if auto_load is on the next insert will re-create the table
            with self._lock:
                del self._db[tname]

                filepath = os.path.join(self.root_dir, self.name, tname + '.csv')
                if self.persistent and os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        self._print('Drop %s: Success' % tname)
                    except (IOError, WindowsError):
                        self._print('Drop %s: Failed' % tname)

    def drop_all(self):
        ''' Drops all tables from this database '''
        self.drop(self.get_table_names())
        if self.persistent:
            with self._lock:
                try:
                    dbfolder = os.path.join(self.root_dir, self.name)
                    if os.path.exists(dbfolder) and not os.listdir(dbfolder):
                        rmtree(dbfolder)
                except (IOError, WindowsError):
                    self._print('Failed to delete folder %s when dropping database' % self.name)
                finally:
                    del self

    def save(self):
        ''' Saves all tables to disk in CSV format '''
        if self.deferred_save:
            self._save_defer()
        else:
            self._save_now()

    def _save_now(self):

        if not self.persistent:
            self._print('Warning: Calling save() on a non-persistent database, ignore')
            return

        with self._lock:
            for dataframe_key, dataframe_val in self._db.items():
                filepath = os.path.join(self.root_dir, self.name, dataframe_key + '.csv')

                # Remove all NaN columns when dynamic_schema is enabled
                if self.dynamic_schema:
                    pass # TODO

                # Save dataframe to csv file only if there is more than zero rows
                if len(dataframe_val) > 0:
                    dataframe_val.to_csv(filepath, cols=self._schemas[dataframe_key], index=False)

                    self._print('Saved %s: %s' % (filepath, 'Success'
                                                  if os.path.exists(filepath) else 'Fail'))

                # If the dataframe is empty, delete csv file
                elif os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        self._print('Deleted %s: Success' % filepath)
                    except (IOError, WindowsError):
                        self._print('Deleted %s: Failed' % filepath)


            # Update last saved time
            self._save_last = time()
            self._save_queue_len = 0

    def _save_defer(self, waittime=0):

        if waittime > 0:
            sleep(waittime)

            # Cancelling a pending save is signaled by setting the save_pending_flag to False
            if not self._save_pending_flag:
                return

        now = time()
        self._save_pending_flag = False # Reset flag

        # The first time, save_last will be 0
        if self._save_last == 0:
            self._save_last = now

        # Has it been longer than self.save_wait?
        if self.save_wait > 0 and now - self._save_last > self.save_wait:
            threading.Thread(target=self._save_now).start()
        elif self.save_wait > 0 and not self._save_pending_flag:
            self._save_pending_flag = True
            worder = lambda: self._save_defer(self.save_wait - now + self._save_last)
            threading.Thread(target=worder).start()

        # Is the queue long enough?
        if self.save_queue_max > 0 and self._save_queue_len > self.save_queue_max:
            threading.Thread(target=self._save_now).start()
        elif self.save_queue_max > 0:
            self._save_queue_len += 1

    def _get_condition_mask(self, dataframe, condition):

        mask = Series([True] * len(dataframe))
        for cond_key, cond_val in condition.items():
            if cond_key not in dataframe.columns:
                self._print(
                    'Warning: Table does not contain column named "%s"' % cond_key)
                mask = Series([False] * len(dataframe))
                break
            else:
                if len(cond_val) == 1:
                    cond_val = cond_val[0]

                    # If condition is of regex type, pass the underlying pattern to Pandas' match
                    if isinstance(cond_val, PandasDatabase._regex_type):
                        mask &= dataframe[cond_key].astype(str).str.match(cond_val.pattern)
                    # If condition evaluates to null, check for all nullable values in column
                    elif isnull(cond_val):
                        mask &= dataframe[cond_key].isnull()
                    # Otherwise use standard string comparison
                    else:
                        mask &= dataframe[cond_key] == cond_val

                elif len(cond_val) > 1:
                    # If condition is of regex type, fail unless there is a single item
                    if any([isinstance(cond, PandasDatabase._regex_type) for cond in cond_val]):
                        raise ValueError('Regex matching cannot be applied to WHERE IN case')
                    # If condition evaluates to null, fail unless there is a single item
                    elif any([isnull(cond) for cond in cond_val]):
                        raise ValueError('Null checking cannot be applied to WHERE IN case')
                    # Otherwise use Pandas' .isin() operator
                    else:
                        mask &= dataframe[cond_key].isin(cond_val)
        return mask

    def find(self, tname, where=None, where_not=None, columns=None, astype=None):
        '''
        Find records in the provided table from the database. If no records are found, return empty
        list, str or dataframe depending on the value of `astype`.

        Parameters
        ----------
        tname : str
            Table to search records from.
        where : dict or None (default `None`)
            Dictionary of <column, value> where value can be of str type for exact match or a
            compiled regex expression for more advanced matching.
        where_not : dict or None (default `None`)
            Identical to `where` but for negative-matching.
        columns: list of str, str or None (default `None`)
            Column(s) to return for the found records, if any.
        astype: str, type or None (default `None`)
            Type to cast the output to. Possible values are: `nonetype`, `dataframe`, `str`,
            `dict`, `json`. If this is `None`, falls back to the type provided to the constructor.
            If a type was provided to the constructor but the user wants to avoid any casting,
            "nonetype" should be passed as the value.

        Returns
        -------
        records : str, list or dataframe
            Output type depends on `astype` parameter.

        Examples
        --------
        >>> db = PandasDatabase("test")
        >>> db.insert("test", record={"Name": "John"})
            Name                                      John
            __id__    dc876999-1f5b-4262-b6bf-c23b875f3a54
            dtype: object
        >>> db.find("test", astype="dict")
            [{'Name': 'John', '__id__': 'dc876999-1f5b-4262-b6bf-c23b875f3a54'}]
        >>> db.find("test", astype="dataframe")
                                             __id__  Name
            0  dc876999-1f5b-4262-b6bf-c23b875f3a54  John
        >>> db.find("test", astype=None)
                                             __id__  Name
            0  dc876999-1f5b-4262-b6bf-c23b875f3a54  John
        >>> db.find("test", where={"Name": "John"}, astype="dict")
            [{'Name': 'John', '__id__': 'dc876999-1f5b-4262-b6bf-c23b875f3a54'}]
        >>> db.find("test", where_not={"Name": "John"}, astype="dict")
            []
        '''
        try:
            # Find is inherently read-only so don't try to autoload table
            tname = self._check_tname(tname, noload=True)
        except ValueError:
            return self._output(DataFrame(), astype=astype)
        where = PandasDatabase._check_conditions(where)
        where_not = PandasDatabase._check_conditions(where_not)
        columns = PandasDatabase._check_type_iter(str, columns)

        dataframe = self._db[tname]
        if len(columns) > 0 and len(dataframe) > 0:
            dataframe = dataframe[columns]

        # Parse the conditions to match
        if len(where) > 0:
            dataframe = dataframe[self._get_condition_mask(dataframe, where)]

        # Parse the conditions not to match
        if len(where_not) > 0:
            dataframe = dataframe[~self._get_condition_mask(dataframe, where_not)]

        self._print('Found %d records in table "%s" where %r and where not %r'
                    % (len(dataframe), tname, where, where_not))
        return self._output(dataframe, astype=astype)

    def find_one(self, tname, where=None, where_not=None, columns=None, astype=None):
        '''
        Find a single record in the provided table from the database. If multiple match, return
        the first one based on the internal order of the records. If no records are found, return
        empty dictionary, string or series depending on the value of `astype`.

        Parameters
        ----------
        tname : str
            Table to search records from.
        where : dict or None (default `None`)
            Dictionary of <column, value> where value can be of str type for exact match or a
            compiled regex expression for more advanced matching.
        where_not : dict or None (default `None`)
            Identical to `where` but for negative-matching.
        columns: list of str, str or None (default `None`)
            Column(s) to return for the found records, if any.
        astype: str, type or None (default `None`)
            Type to cast the output to. Possible values are: `nonetype`, `series`, `str`, `dict`,
            `json`. If this is `None`, falls back to the type provided to the constructor.
            If a type was provided to the constructor but the user wants to avoid any casting,
            "nonetype" should be passed as the value.

        Returns
        -------
        records : str, dict or series
            Output type depends on `astype` parameter.

        Examples
        --------
        >>> db = PandasDatabase("test")
        >>> db.insert("test", record={"Name": "John"})
            Name                                      John
            __id__    dc876999-1f5b-4262-b6bf-c23b875f3a54
            dtype: object
        >>> db.find_one("test", astype="dict")
            {'Name': 'John', '__id__': 'dc876999-1f5b-4262-b6bf-c23b875f3a54'}
        >>> db.find_one("test", astype="series")
            __id__    dc876999-1f5b-4262-b6bf-c23b875f3a54
            Name                                      John
            Name: 0, dtype: object
        >>> db.find_one("test", astype=None)
            __id__    dc876999-1f5b-4262-b6bf-c23b875f3a54
            Name                                      John
            Name: 0, dtype: object
        >>> db.find_one("test", where={"Name": "John"}, astype="dict")
            {'Name': 'John', '__id__': 'dc876999-1f5b-4262-b6bf-c23b875f3a54'}
        >>> db.find_one("test", where_not={"Name": "John"}, astype="dict")
            {}
        '''
        records = self.find(tname, where=where, where_not=where_not, columns=columns,
                            astype='dataframe')
        return self._output(records, single=True, astype=astype)

    def delete(self, tname, where=None, where_not=None, columns=None, astype=None):
        '''
        Delete records from the provided table. Parameters, matching and output are identical to
        `find()`.

        Parameters
        ----------
        tname : str
            Table to delete records from.
        where : dict or None (default `None`)
            Dictionary of <column, value> where value can be of str type for exact match or a
            compiled regex expression for more advanced matching.
        where_not : dict or None (default `None`)
            Identical to `where` but for negative-matching.
        columns: list of str, str or None (default `None`)
            Column(s) to return for the deleted records, if any.
        astype: str, type or None (default `None`)
            Type to cast the output to. Possible values are: `nonetype`, `dataframe`, `str`,
            `dict`, `json`. If this is `None`, falls back to the type provided to the constructor.
            If a type was provided to the constructor but the user wants to avoid any casting,
            "nonetype" should be passed as the value.

        Returns
        -------
        records : str, list or dataframe
            Records deleted from the table. Output type depends on `astype` parameter.

        See Also
        --------
        PandasDatabase.find
        '''
        tname = self._check_tname(tname)
        where = PandasDatabase._check_conditions(where)
        where_not = PandasDatabase._check_conditions(where_not)
        columns = PandasDatabase._check_type_iter(str, columns)

        # Find the rows to be deleted
        delrows = self.find(tname, where=where, where_not=where_not, astype=DataFrame)

        # Remove them from the table
        dataframe = self._db[tname]
        dataframe = dataframe[~(dataframe.index.isin(delrows.index))]
        self._db[tname] = dataframe

        self._print('Deleted %d records from table "%s"' % (len(delrows), tname))

        # Save the changes to disk if required
        if self.auto_save:
            self.save()

        # Return deleted rows
        return self._output(delrows, astype=astype)

    def _update_schema(self, tname, columns):
        for field_key in columns:

            # Is the column part of the schema?
            if field_key not in self._schemas[tname]:

                # If dynamic_schema is enabled, add new columns if necessary
                if self.dynamic_schema:
                    self._print('Adding column "%s" to schema for table "%s"'
                                % (field_key, tname))
                    self._schemas[tname].append(field_key)
                    self._schemas[tname] = self._check_columns(self._schemas[tname])
                    self._db[tname][field_key] = None

                # Otherwise this is unexpected
                else:
                    raise ValueError('Column "%s" does not exist in schema for table "%s"'
                                     % (field_key, tname))

    def insert(self, tname, record=None, columns=None, astype=None):
        '''
        Inserts record into the provided table from the database. Returns inserted record as
        list, str or series depending on the value of `astype`.

        Parameters
        ----------
        tname : str
            Table to insert records into.
        where : dict or None (default `None`)
            Dictionary of <column, value> where value can be of str type for exact match or a
            compiled regex expression for more advanced matching.
        where_not : dict or None (default `None`)
            Identical to `where` but for negative-matching.
        columns: list of str, str or None (default `None`)
            Column(s) to return for the inserted records.
        astype: str, type or None (default `None`)
            Type to cast the output to. Possible values are: `nonetype`, `series`, `str`, `dict`
            `json`. If this is `None`, falls back to the type provided to the constructor.
            If a type was provided to the constructor but the user wants to avoid any casting,
            "nonetype" should be passed as the value.

        Returns
        -------
        record : str, dict or series
            Inserted record. Output type depends on `astype` parameter.

        Examples
        --------
        >>> db = PandasDatabase("test")
        >>> db.insert("test", record={"Name": "John"})
            Name                                      John
            __id__    dc876999-1f5b-4262-b6bf-c23b875f3a54
            dtype: object
        '''
        tname = self._check_tname(tname)
        record = PandasDatabase._check_dict_type(str, str, record, cast=self.auto_cast)
        columns = PandasDatabase._check_type_iter(str, columns)
        record[self._id_colname] = str(uuid.uuid4())

        # If a row generation function exists for this table, use that
        record_new = {}
        if tname in self._rowgens:
            self._print('Using row generator to create new record in "%s"' % tname)
            record_new = self._rowgens[tname]()

        # Set as many fields as provided in new record, leave the rest as-is
        if record is not None:
            for field_key, field_val in record.items():
                record_new[field_key] = field_val

        with self._lock:
            self._print('Inserting new record into "%s": %r' % (tname, record_new))
            self._update_schema(tname, record_new.keys())

            row = Series(record_new)
            self._db[tname].loc[len(self._db[tname])] = row

        # Save the changes to disk if required
        if self.auto_save:
            self.save()

        if len(columns) > 0:
            row = row[columns]

        return self._output(row, single=True, astype=astype)

    def upsert(self, tname, record=None, where=None, where_not=None, columns=None, astype=None):
        '''
        Attempts to update records in the provided table from the database. If none are found,
        inserts new record that would match all the conditions. Returns updated or inserted record
        as list, dict, str, dataframe or series depending on the value of `astype`.

        Parameters
        ----------
        tname : str
            Table to update or insert records into.
        where : dict or None (default `None`)
            Dictionary of <column, value> where value can be of str type for exact match or a
            compiled regex expression for more advanced matching.
        where_not : dict or None (default `None`)
            Identical to `where` but for negative-matching.
        columns: list of str, str or None (default `None`)
            Column(s) to return for the updated or inserted records.
        astype: str, type or None (default `None`)
            Type to cast the output to. Possible values are: `nonetype`, `dataframe`, `series`,
            `str`, `dict`, `json`. If this is `None`, falls back to the type provided to the
            constructor. If a type was provided to the constructor but the user wants to avoid any
            casting, "nonetype" should be passed as the value.

        Returns
        -------
        records : list, dict, str, dataframe or series
            Updated or inserted records. Output type depends on `astype` parameter.

        Examples
        --------
        >>> db = PandasDatabase("test")
        >>> db.upsert("test", record={"Name": "John", "Color": "Blue"})
            Color                                     Blue
            Name                                      John
            __id__    a8f31bdd-8e57-4fa7-96f6-e6b20bf7a9dc
            dtype: object
        >>> db.upsert("test", where={"Name": "Jane", "Color": "Red"})
            Color                                      Red
            Name                                      Jane
            __id__    65c3bc2b-020c-48f0-b448-5fdb4e548abe
            dtype: object
        >>> db.upsert("test", record={"Color": "Yellow"}, where={"Name": "John"})
                                             __id__  Name   Color
            0  a8f31bdd-8e57-4fa7-96f6-e6b20bf7a9dc  John  Yellow
        '''
        tname = self._check_tname(tname)
        where = PandasDatabase._check_conditions(where)
        where_not = PandasDatabase._check_conditions(where_not)
        columns = PandasDatabase._check_type_iter(str, columns)
        record = PandasDatabase._check_dict_type(str, str, record, cast=self.auto_cast)

        # Attempt search only if where conditions are given
        if (where is not None and len(where) > 0) \
                or (where_not is not None and len(where_not) > 0):
            ixs = self.find(tname, where=where, where_not=where_not, astype='dataframe').index

            # If no records matched the where conditions, default to insert
            if len(ixs) == 0:
                self._print(
                    'Warning: No records in "%s" matched the conditions %s' % (tname, where))

                # Add all the key-value pairs from the where condition
                for cond_key, cond_value in where.items():
                    record[cond_key] = cond_value[0] if len(cond_value) > 0 else None

                # Create a new record
                record_new = self.insert(tname, record=record, columns=columns, astype='series')

                # If the default value of the column provided in where_not conflicts, error out
                if where_not is not None and any([record_new[cond_key] in cond_value
                                                  for cond_key, cond_value in where_not.items()]):
                    _id = PandasDatabase._id_colname
                    self.delete(tname, where={_id: record_new[_id]})
                    raise ValueError('Cannot insert new record because default values conflict '
                                     'with conditions provided: %s' % where_not)

                # Otherwise return created record
                return self._output(record_new, astype=astype)

            # If existing record(s) must be updated
            elif len(ixs) > 0:
                self._print('Updating %d record(s) in "%s" where %r and where not %r'
                            % (len(ixs), tname, where, where_not))

                with self._lock:
                    self._update_schema(tname, record.keys())
                    for field_key, field_val in record.items():
                        self._db[tname].loc[ixs, field_key] = field_val

            # Save the changes to disk if required
            if self.auto_save:
                self.save()

            # Return updated records
            rows = self._db[tname].loc[ixs]
            if len(columns) > 0:
                rows = rows[columns]
            return self._output(rows, astype=astype)

        # Insert if no where conditions are given
        else:

            # Return the new record
            new_record = self.insert(tname, record=record, columns=columns, astype='series')
            return self._output(new_record, astype=astype)


    ###########################################################################
    #         The boiler plate section with all the checks begins here        #
    ###########################################################################


    def _output(self, records, single=False, astype=None):

        if astype is None:
            astype = self.astype

        if isinstance(astype, str):
            astype_str = astype
        elif isinstance(astype, type):
            astype_str = astype.__name__
        else:
            astype_str = type(astype).__name__
        astype_str = astype_str.lower()

        supported_formats = ['nonetype', 'series', 'str', 'dict', 'json'] if single else \
            ['nonetype', 'dataframe', 'dict', 'str', 'json']
        if astype not in supported_formats and astype_str not in supported_formats:
            raise RuntimeError('Record output type is "%s", must be one of [%s]' %
                               (astype_str, ', '.join(
                                   (fmt for fmt in supported_formats if isinstance(fmt, str)))))

        # Retrieve the first record if single
        if single and isinstance(records, DataFrame):
            records = records.iloc[0] if len(records) > 0 else Series()

        # If astype is left as default or records type matches output type, return as-is
        if astype is None or astype_str == type(records).__name__.lower():
            return records

        elif isinstance(records, Series):
            if astype_str == 'dict':
                return records.to_dict() if single else [records.to_dict()]
            elif astype_str == 'str' or astype_str == 'json':
                return records.to_json() if single else '[' + records.to_json() + ']'
            else:
                raise RuntimeError('Records of type "%s" could not be cast into "%s"' %
                                   (type(records).__name__, astype_str))

        elif isinstance(records, DataFrame):
            if astype_str == 'dict':
                return records.to_dict('records')
            elif astype_str == 'str' or astype_str == 'json':
                return records.to_json(orient='records')
            else:
                raise RuntimeError('Records of type "%s" could not be cast into "%s"' %
                                   (type(records).__name__, astype_str))

        else:
            raise ValueError('Instance type of records is unknown: "%s"' % type(records).__name__)

    def _check_case_dict(self, dic, warn=False):
        for k in list(dic.keys()):
            k_chk = self._check_case_str(k, warn)
            if k != k_chk:
                dic[k_chk] = dic.pop(k)
        return dic

    def _check_case_str(self, string, warn=False):
        if string != string.lower():
            if warn:
                self._print('Warning: Converting "%s" into lower case' % string)
            string = string.lower()
        return string

    def _check_table_names(self, table_names, warn=False):
        table_names = PandasDatabase._check_type_iter(str, table_names)

        if warn:
            for tname in table_names:
                if tname != tname.lower():
                    self._print('Warning: Converting "%s" into lower case' % tname)

        return [tname.lower() for tname in table_names]

    def _check_tname(self, tname, noload=False):
        tname = tname.lower()
        if tname not in self._db.keys():

            # If table is not found but auto_load is enabled, just load/create it
            if self.auto_load and not noload:
                self.load(tname)

            # Otherwise block execution
            else:
                raise ValueError('Selected table "%s" does not exist in this database: [%s]'
                                 % (tname, ','.join(self._db.keys())))
        return tname

    @staticmethod
    def _check_type_iter(var_type, var_iter, cast=False):
        if var_iter is None:
            var_iter = list()
        elif not isinstance(var_iter, list) and not isinstance(var_iter, tuple):
            var_iter = (var_iter,)

        if not isinstance(var_type, list) and not isinstance(var_type, tuple):
            var_type = (var_type,)

        var_iter = [tostr(it, cast=cast) for it in var_iter]

        if not all([type(it) in var_type for it in var_iter]):
            raise ValueError(
                'Parameter must be of type "%s" or iterable of such type. Instead found "%s".'
                % ('" or "'.join([ty.__name__ for ty in var_type]),
                    [type(it).__name__ for it in var_iter if not type(it) in var_type][0]))
        return var_iter

    def _check_columns(self, columns, add_id=False):
        columns = list(PandasDatabase._check_type_iter(str, columns))

        # Check if the id column is present
        if PandasDatabase._id_colname not in columns:
            if add_id:
                columns.append(PandasDatabase._id_colname)
            else:
                raise ValueError('Schema must contain column named "%s"' % self._id_colname)
        elif add_id:
            self._print('Warning: Column named "%s" is used internally as the primary key and '
                        'should not be modified manually' % self._id_colname)

        # Check for column names
        for col_name in columns:
            if not re.match(PandasDatabase._colname_rgx, col_name):
                raise ValueError('Invalid column name "%s". Column names must match the following '
                                 'regex: "%s"' % (col_name, PandasDatabase._colname_rgx))

        return list(columns)

    @staticmethod
    def _check_conditions(conditions):
        conditions = PandasDatabase._check_dict_type(str, (str, list, PandasDatabase._regex_type), conditions)
        for cond_key, cond_val in conditions.items():
            if not hasattr(cond_val, '__iter__') and \
                    (isnull(cond_val) or isinstance(cond_val, PandasDatabase._regex_type)):
                conditions[cond_key] = [cond_val]
            else:
                conditions[cond_key] = PandasDatabase._check_type_iter(str, cond_val)
        return conditions

    @staticmethod
    def _check_dict_type(key_type, val_type, obj, cast=False):
        if obj is None:
            obj = dict()
        if not isinstance(obj, dict):
            raise ValueError('Parameter must be None or of type dict: %s' % obj)

        if not isinstance(key_type, list) and not isinstance(key_type, tuple):
            key_type = (key_type,)
        if not isinstance(val_type, list) and not isinstance(val_type, tuple):
            val_type = (val_type,)

        obj = {tostr(obj_k, cast=cast): tostr(obj_v, cast=cast) for obj_k, obj_v in obj.items()}

        if not cast and not all([type(it) in key_type for it in obj.keys()]):
            raise ValueError(
                'Dictionary keys must be of type "%s". Instead found "%s".'
                % ('" or "'.join([ty.__name__ for ty in key_type]),
                    [type(it).__name__ for it in obj.keys() if not type(it) in key_type][0]))
        if not cast and not all([type(it) in val_type for it in obj.values()]):
            raise ValueError(
                'Dictionary values must be of type "%s". Instead found "%s".'
                % ('" or "'.join([ty.__name__ for ty in val_type]),
                    [type(it).__name__ for it in obj.values() if not type(it) in val_type][0]))
        return obj

    ###########################################################################
    #                     The web API section begins here                     #
    ###########################################################################

    @staticmethod
    def _request(request, request_fallback=None):
        ''' Extract request fields wherever they may come from: GET, POST, forms, fallback '''
        # Use lambdas to avoid evaluating bottle.request.* which may throw an Error
        all_dicts = [
            lambda: request.json,
            lambda: request.forms,
            lambda: request.query,
            lambda: request.files,
            #lambda: request.POST,
            lambda: request_fallback
        ]
        request_dict = dict()
        for req_dict_ in all_dicts:
            try:
                req_dict = req_dict_()
            except KeyError:
                continue
            if req_dict is not None and hasattr(req_dict, 'items'):
                for req_key, req_val in req_dict.items():
                    request_dict[req_key] = req_val
        return request_dict

    @staticmethod
    def _extract_params(request_dict, param_list, param_fallback=False):
        ''' Extract pddb parameters from request '''

        if not param_list or not request_dict:
            return dict()

        query = dict()
        for param in param_list:
            # Retrieve all items in the form of {param: value} and
            # convert {param__key: value} into {param: {key: value}}
            for query_key, query_value in request_dict.items():
                if param == query_key:
                    query[param] = query_value
                else:
                    query_key_parts = query_key.split('__', 1)
                    if param == query_key_parts[0]:
                        query[param] = {query_key_parts[1]: query_value}

        # Convert special string "__null__" into Python None
        nullifier = lambda d: {k:(nullifier(v) if isinstance(v, dict) else # pylint: disable=used-before-assignment
                                  (None if v == '__null__' else v)) for k, v in d.items()}

        # When fallback is enabled and no parameter matched, assume query refers to first parameter
        if param_fallback and all([param_key not in query.keys() for param_key in param_list]):
            query = {param_list[0]: dict(request_dict)}

        # Return a dictionary with only the requested parameters
        return {k:v for k, v in nullifier(query).items() if k in param_list}

    def bind_bottle_routes(self, bottle_app=None, bottle_router='route', default_permissions='r', table_permissions=None):

        # Parameter guards
        allowed_permissions = (None, 'r', 'w')
        table_permissions = self._check_dict_type(str, str, table_permissions)
        if default_permissions not in allowed_permissions:
            raise ValueError('Parameter "default_permission" must be one of %r' %
                             allowed_permissions)

        # Imports exclusive only to web API
        from inspect import getargspec
        from bottle import Bottle, HTTPResponse, request, static_file, template

        # Wrap db method into a function that returns HTTPResponse
        def route_method(table, action, request_fallback=None):
            pddb_method = getattr(self, action)
            argspec = getargspec(pddb_method)
            arg_ix = 0 if not argspec.defaults else len(argspec.defaults)
            param_list = None if arg_ix == 0 else argspec.args[-arg_ix:]
            request_dict = self._request(request, request_fallback=request_fallback)
            query = self._extract_params(request_dict, param_list, param_fallback=True)
            if 'astype' in argspec.args[-arg_ix:]:
                query['astype'] = 'json'

            res, code = None, 0
            try:
                res, code = pddb_method(table, **query), 200
            except ValueError as ex:
                res, code = ex, 400
            except RuntimeError as ex:
                res, code = ex, 500
            finally:
                return HTTPResponse(status=code, body=str(res)) # pylint: disable=lost-exception

        # Retrieve action method by name and table and return it
        def action_method_factory(table, action):
            action_default = action
            if table is None:
                return lambda table, action=action_default, request_fallback=None: \
                    route_method(table, action, request_fallback)
            else:
                return lambda table=table, action=action_default, request_fallback=None: \
                    route_method(table, action, request_fallback)

        # Use a hidden object property to store route callbacks
        if not hasattr(self, '_action_callbacks'):
            self._action_callbacks = dict()

        # Initialize a bottle app if none is given
        bottle_app = bottle_app or Bottle()
        bottle_router = getattr(bottle_app, bottle_router)

        # Read permissions only allow access to "find", write permissions allow everything
        all_actions = ('find', 'insert', 'upsert', 'delete', 'drop')
        allowed_actions = lambda perm: {'r': ('find',), 'w': all_actions}.get(perm, tuple())

        # Cache all action callbacks
        for action in all_actions:
            self._action_callbacks[action] = action_method_factory(None, action)

        # Generic routes for all tables and bind actions to callbacks
        generic_actions = allowed_actions(default_permissions)
        for action in generic_actions:
            self._print('Binding action "%s" for all tables' % action)
            bottle_router('/pddb/%s/<table>' % action, method=['GET', 'POST'],
                             callback=self._action_callbacks[action])

        # Specific actions for user provided table names
        for tname, perm in table_permissions.items():
            table_actions = allowed_actions(perm)
            for action in table_actions:
                bottle_router('/pddb/%s/%s' % (action, tname), method=['GET', 'POST'],
                                 callback=action_method_factory(tname, action))

        # Helper function for relative paths
        relpath = lambda x: os.path.join(self.root_dir, x)

        # Database explorer UI routes
        @bottle_router('/pddb/html/<table>', method='GET')
        def table_html(table):
            data = self.find(table, astype='dict')
            schema = self.get_table_schema(table)
            if request.query.get('columns'):
                columns = request.query.get('columns').split(',')
                schema = [col for col in columns if col in schema] # Keep ordering given by columns
            rows = [[row[PandasDatabase._id_colname]] +
                    [row[col] for col in schema] for row in data]
            template_path = os.path.join(
                os.path.dirname(__file__), 'templates', 'pddb_table.tpl')
            html_out = template(template_path, dom_id='table_%s' % table, schema=schema, rows=rows)
            return html_out

        @bottle_router('/pddb/html/tables', method='GET')
        def schemas_html():
            schema = ['Table Name']
            rows = [['', tname] for tname in self.get_table_names()]
            template_path = os.path.join(
                os.path.dirname(__file__), 'templates', 'pddb_table.tpl')
            html_out = template(template_path, dom_id='table_list', schema=schema, rows=rows)
            return html_out

        @bottle_router('/pddb/pddb.js', method='GET')
        def db_js():
            return static_file(os.path.join('js', 'pddb.js'), root=os.path.dirname(__file__))

        @bottle_router('/pddb', method='GET')
        def db_html():
            return static_file(
                os.path.join('templates', 'pddb.html'), root=os.path.dirname(__file__))

        self.bottle_app = bottle_app
        return bottle_app

def main(args):

    parser = argparse.ArgumentParser(
        description='Load local database and exposed its API via http requests.')
    parser.add_argument('dbname', type=str,
                        help='name of the database')
    parser.add_argument('--permissions', dest='permissions', type=str, default='r',
                        help='default permissions for all tables')
    parser.add_argument('--root-dir', dest='root_dir', type=str, default=os.getcwd(),
                        help='root folder where the database will be stored')
    parser.add_argument('--port', dest='port', type=int, default=8080,
                        help='http port where the API will be made available')
    args = parser.parse_args(args)

    pddb = PandasDatabase(args.dbname, root_dir=args.root_dir)
    pddb.bind_bottle_routes(default_permissions=args.permissions).run(host='0.0.0.0', port=args.port, debug=True)

if __name__ == '__main__':
    main(sys.argv[1:])
