.. _install:

Quick Start
===========

.. note::
    Current supported engines are ``mysql``.

Common operations
-----------------

**Importing beesql**::

    import beesql


**Creating a database connection**::
    
    from beesql import DB

    db = DB(database_type='mysql', db_name="database_name", username="username" password='password',
            host="database_host", port="database_port")

    db = DB('mysql', 'db_name').auth('username', 'password')

The database_type should be one of supported engines.

** Creating a statement **::

    statement = db.query('table_name').select()
    statement = db.query('table_name').update(name="new name")
    statement = db.query('table_name').delete()

** Select statement **::
    * statement = db.query().on('table_name').select() => `` SELECT * FROM table_name ``

    * statement = db.query().on('table_name').select('id', 'age') => `` SELECT id, age FROM table_name ``

    * statement = db.query('table_name').select('id', 'age').select('location') => `` SELECT id, age, location FROM table_name ``

** Update statement **::
    * statement = db.query('table_name').update(age=23) => `` UPDATE table_name SET age=23 ``

** Delete statement **::
    * statement = db.query('table_name').delete() => `` DELETE FROM table_name ``

** Where condition **::
    * db.query('table_name').select().where(age=20, code='100') => `` SELECT * FROM table_name WHERE age = 20 AND code = 100
    * db.query('table_name').select().where(age=20)._and('code').eq(100) => `` SELECT * FROM table_name WHERE age = 20 AND code = 100
    * db.query('table_name').select().where('age').lt(100)._or('code').gte(10) => `` SELECT * FROM table_name WHERE age < 20 OR code >= 100
