# -*- coding: utf-8 -*-
import pymysql


DATABASE_MYSQL = 'mysql'
DATABASE_SQLITE = 'sqlite'


class BeeSQLError(Exception):
    pass


class Row(object):
    """ table row """
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

    def get(self, column):
        return getattr(self, column)


class Query(object):
    """ SQL generator """
    higher_order_keywords = ['select', 'insert', 'update', 'delete']

    def __init__(self, db):
        self.db = db
        self._table = None
        self._select = None
        self._insert = None
        self._update = None
        self._delete = None
        self._where = None

    def __repr__(self):
        return '{}: {}'.format(self.__class__, self.get_sql())

    @property
    def table(self):
        return self._table

    def on(self, table):
        self._table = table
        return self

    def get_format_params(self, formatted=False):
        return {
            'newtab': '\n  ' if formatted else ' ',
            'newln': '\n' if formatted else ' ',
        }


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

    def select(self, *args):
        fields = args
        if len(fields) == 0:
            self._select = '*'
        else:
            self._select = ', '.join(fields)

        return self

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
        klass = self.database_type_to_query_class[self.database_type]
        _query = klass(self)
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
