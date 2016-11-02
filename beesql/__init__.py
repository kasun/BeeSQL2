# -*- coding: utf-8 -*-
from functools import wraps

import pymysql


DATABASE_MYSQL = 'mysql'
DATABASE_SQLITE = 'sqlite'


class BeeSQLError(Exception):
    pass


def validate_keyword_chaining(query, keyword):
    if not query.higher_order_keyword:
        return True

    return isinstance(query.higher_order_keyword, type(keyword))


def higher_order_keyword(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.table:
            raise BeeSQLError('No table selected. Use Query.on to select a table first')

        keyword = func(self, *args, **kwargs)
        if not validate_keyword_chaining(self, keyword):
            raise BeeSQLError('Invalid keyword chaining.')

        self.higher_order_keyword = keyword
        return self

    return wrapper


class Row(object):
    """ table row """
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

    def get(self, column):
        return getattr(self, column)


class LogicalOperator(object):
    def __init__(self, db, data_operator):
        self.data_operator = data_operator

    def get_sql(self):
        return ' {} {}'.format(self.KEYWORD, self.data_operator.get_sql())

    def __repr__(self):
        return self.get_sql()


class LogicalAND(LogicalOperator):
    KEYWORD = 'AND'


class LogicalOR(LogicalOperator):
    KEYWORD = 'OR'


class DataOperator(object):
    def __init__(self, query, column, value):
        self.query = query
        self.column = column
        self.value = value

    def set_column(self, column_name):
        self.column = column_name

    def get_sql(self):
        return '{} {} {}'.format(self.column, self.get_operator(), self.value)

    def __repr__(self):
        return self.get_sql()


class EqualOperator(DataOperator):
    def get_operator(self):
        return '='

    def get_value(self):
        return self.query.db.escape(self.value)


class NotEqualOperator(DataOperator):
    def get_operator(self):
        return '!='

    def get_value(self):
        return self.query.db.escape(self.value)


class Keyword(object):
    def __init__(self, query, **kwargs):
        self.query = query


class Select(Keyword):
    def __init__(self, query, table_name, all=False, *args):
        super().__init__(query)
        self.table_name = table_name
        self.all = all
        self.fields = list(set(args))

    def update(self, *args):
        fields = list(args)
        if self.all:
            return

        all = True if len(fields) == 0 else False
        if all:
            self.all = all
            return

        fields_set = set(self.fields)
        fields_set.update(fields)
        self.fields = list(fields_set)

    def get_sql(self):
        fields = "*" if self.all else ', '.join(self.fields)
        params = {
            'fields': fields,
            'table': self.table_name,
        }
        sql = "SELECT {fields} FROM {table}".format(**params)
        return sql


class ColumnSelector(object):
    def __init__(self, query, column_name, logical_operator_class):
        self.query = query
        self.column_name = column_name
        self.logical_operator_class = logical_operator_class

    def complete(self, data_operator):
        if not self.query.condition:
            condition = Where(self.query, op)
            self.query.condition = condition
        else:
            logical_operator = logical_operator_class(self.query, op)
            self.query.condition.chain([logical_operator])

    def eq(self, value):
        op = EqualOperator(self.query, self.column_name, value)
        self.complete(op)
        return self.query


class Where(Keyword):
    def __init__(self, query, data_operator, logical_operators=None):
        super().__init__(query)
        self.data_operator = data_operator
        self.logical_operators = [] if not logical_operators else logical_operators[:]

    def chain(self, logical_operators):
        self.logical_operators.extend(logical_operators)

    def get_sql(self):
        sql = 'WHERE {}'.format(self.data_operator.get_sql())
        for lop in self.logical_operators:
            sql = sql + lop.get_sql()

        return sql


class Query(object):
    """ SQL generator """
    def __init__(self, db):
        self.db = db
        self.higher_order_keyword = None
        self.condition = None
        self._table = None
        self.partial_condition_column = None

    def __repr__(self):
        return '{}: {}'.format(self.__class__, self.get_sql())

    def reset(self):
        self.higher_order_keyword = None
        self.condition = None

    @property
    def table(self):
        return self._table

    def on(self, table):
        self.reset()
        self._table = table
        return self

    @higher_order_keyword
    def select(self, *args):
        if self.higher_order_keyword and isinstance(self.higher_order_keyword, Select):
            self.higher_order_keyword.update(*args)
            return self.higher_order_keyword

        fields = args[:]
        all = True if len(fields) == 0 else False
        select = Select(self, self.table, all, *fields)
        return select

    def where(self, column_name=None, **kwargs):
        if column_name is None and not kwargs:
            raise BeeSQLError('where statement can\'t be empty')

        if column_name is not None:
            selector = ColumnSelector(self, column_name, LogicalAND)
            return selector
        else:
            data_ops = [EqualOperator(self, key, val) for key, val in kwargs.iteritems()]
            if not self.condition:
                data_op = data_ops.pop(0)
                logical_ops = [LogicalAND(dop) for dop in data_ops]
                where_keyword = Where(self.query, data_op, logical_ops)
                self.condition = where_keyword
            else:
                logical_ops = [LogicalAND(dop) for dop in data_ops]
                self.condition.chain(logical_ops)

        return self


    def get_format_params(self, formatted=False):
        return {
            'newtab': '\n  ' if formatted else ' ',
            'newln': '\n' if formatted else ' ',
        }

    def get_sql(self):
        if not self.higher_order_keyword:
            raise BeeSQLError('Incomplete SQL')

        sql = self.higher_order_keyword.get_sql()
        return sql


class MySQLQuery(Query):

    def get_sql(self, formatted=False):
        sql = ''
        params = self.get_format_params(formatted)
        params.update({
            'select': self._select,
            'from': self._table,
            'where': self._where,
        })
        if self._select:
            sql = 'SELECT{newtab}{select}{newln}FROM{newtab}{from}'.format(**params)
        if sql and self._where:
            sql = sql + '{newln}WHERE{newtab}{where}'.format(**params)

        return sql

    def where(self, **kwargs):
        where_list = ["{}={}".format(key, self.db.escape(val)) for key, val in kwargs.items()]
        self._where = ' AND '.join(where_list)

        return self


class Connection(object):
    def __init__(self, username, password, db=None, host='localhost', port=3306, unix_socket=None):
        self.username = username
        self.password = password
        self.db = db
        self.host = host
        self.port = port
        self.unix_socket = unix_socket
        self._connection = None

    def __repr__(self):
        return '{}: {}'.format(self.__class__, 'Open' if self.is_open() else 'Closed')

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class MySQLConnection(Connection):

    def is_open(self):
        return bool(self._connection) and self._connection.open

    def open(self):
        if self.is_open():
            return

        if not self.unix_socket:
            self._connection = pymysql.connect(user=self.username, passwd=self.password, db=self.db,
                                               host=self.host, port=self.port, autocommit=True)
        else:
            self._connection = pymysql.connect(user=self.username, passwd=self.password, db=self.db,
                                               unix_socket=self.unix_socket, autocommit=True)

    def execute(self, query):
        if not isinstance(query, Query):
            raise BeeSQLError('Expected instance of {}. Got instance of'.format(type(Query), type(query)))

        rows = []
        sql = query.get_sql()
        cursor = self._connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)
        results = cursor.fetchall()
        rows = [Row(**r) for r in results]

        return rows

    def close(self):
        if self.is_open():
            self._connection.close()


class DB(object):
    """ Database connection """
    supported_databases = [DATABASE_MYSQL, DATABASE_SQLITE]
    database_type_to_query_class = {
        DATABASE_MYSQL: MySQLQuery,
    }
    database_type_to_connection = {
        DATABASE_MYSQL: MySQLConnection,
    }

    def __init__(self, database_type, db_name=None, username=None, password=None,
                 host='localhost', port=3306, unix_socket=None):

        if database_type not in self.supported_databases:
            raise BeeSQLError('database_type: {} not supported'.format(database_type))

        self.database_type = database_type
        self.db_name = db_name
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.unix_socket = unix_socket

    def query(self):
        _query = Query(self)
        return _query

    def __repr__(self):
        return '<DB {}:{}'.format(self.database_type, self.db_name)

    def connect(self):
        klass = self.database_type_to_connection[self.database_type]
        conn = klass(username=self.username, password=self.password, db=self.db_name,
                     host=self.host, port=self.port, unix_socket=self.unix_socket)
        return conn

    def use(self, db_name):
        self.db_name = db_name
        return self

    def auth(self, username, password):
        self.username = username
        self.password = password
        return self

    def escape(self, item):
        if self.database_type == DATABASE_MYSQL:
            tmp_connection = pymysql.connections.Connection()
            return tmp_connection.escape(item)

        return item
