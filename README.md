# BeeSQL2 - Pythonic SQL Library #

### This is still under development

BeeSQL2 is a SQL abstraction library targetting python that helps,

+ Minimize repetitive steps in Python DB-API.
+ Use python methods for SQL generation.
+ Map SQL to Python datastructures.

BeeSQL2 is not an ORM.

BeeSQL2 converts this,
```
db.query('community').select('id', 'name').where(role='editor')._and('age').gt(30).order_by('-age').limit(10)
```
into this,
```
SELECT name, id FROM community WHERE role = 'editor' AND age > 30 ORDER BY age DESC LIMIT 10 OFFSET 0
```

## Documentation ##
Documentation of BeeSQL2 can be found at <http://beesql.readthedocs.org>.

## Installation ##
Run python setup.py install
