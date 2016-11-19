# -*- coding: utf-8 -*-
import pymysql

from .settings import DATABASE_MYSQL, DATABASE_SQLITE
from .exceptions import BeeSQLError
from .query.base import Statement, ColumnSelector
from .query.mysql import MySQLQuery


class Row(object):
    """ table row """
    def __init__(self, **kwargs):
        self.values = kwargs.copy()

    def __getattr__(self, key):
        if key in self.values:
            return self.values[key]

        super().__getattr__(key)

    def __repr__(self):
        return '< {} >: {}'.format('Row', self.values)

    def get(self, column):
        return getattr(self, column)


class Rows(object):
    def __init__(self, rows):
        self.rows = rows

    def __repr__(self):
        return '< {} >: {}'.format('Rows', self.count)

    def __iter__(self):
        return iter(self.rows)

    def __getitem__(self, key):
        return self.all()[key]

    @property
    def count(self):
        return len(self.rows)

    def all(self):
        return self.rows[:]


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
        if isinstance(query, ColumnSelector):
            raise BeeSQLError('No operation performed on {}'.format(query))

        if not isinstance(query, Statement):
            raise BeeSQLError('Expected instance of {}. Got instance of {}'.format(Statement, query.__class__))

        rows = []
        sql = query.get_sql()
        cursor = self._connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)
        results = cursor.fetchall()
        rows = [Row(**r) for r in results]

        return Rows(rows)

    def close(self):
        if self.is_open():
            self._connection.close()


class DB(object):
    """ Database connection """
    supported_databases = [DATABASE_MYSQL, DATABASE_SQLITE]
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

    def query(self, db_name=None):
        if not self.db_name:
            raise BeeSQLError('No database chosen')

        if self.database_type == DATABASE_MYSQL:
            _query = MySQLQuery(self, db_name)
            return _query

    def __repr__(self):
        return '<DB {}:{}'.format(self.database_type, self.db_name)

    def connect(self):
        Connection = self.database_type_to_connection[self.database_type]
        conn = Connection(username=self.username, password=self.password, db=self.db_name,
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
            str_item = str(item)
            return pymysql.escape_string(str_item)

        return item
