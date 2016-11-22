import heapq

from .mixins import DataOperatorFuncs, AggregationFuncs
from ..exceptions import BeeSQLError
from ..aggregation import AggregationField
from .decorators import primary_keyword, secondary_keyword, logical_operator, complete_condition
from .decorators import aggregation


class LogicalOperator(object):
    def __init__(self, statement, data_operator):
        self.statement = statement
        self.data_operator = data_operator

    def get_sql(self):
        return '{} {}'.format(self.KEYWORD, self.data_operator.get_sql())

    def __repr__(self):
        return self.get_sql()


class LogicalAND(LogicalOperator):
    KEYWORD = 'AND'


class LogicalOR(LogicalOperator):
    KEYWORD = 'OR'


class DataOperator(object):
    def __init__(self, statement, column, value):
        self.statement = statement
        self.column = column
        self.value = value

    def set_column(self, column_name):
        self.column = column_name

    def get_sql(self):
        return '{} {} {}'.format(self.column, self.get_operator(), self.get_value())

    def __repr__(self):
        return self.get_sql()


class EqualOperator(DataOperatorFuncs, DataOperator):
    OPERATOR = '='


class NotEqualOperator(DataOperatorFuncs, DataOperator):
    OPERATOR = '<>'


class InOperator(DataOperatorFuncs, DataOperator):
    OPERATOR = 'IN'

    def _filter(self, value):
        if isinstance(value, int):
            return str(value)

        return "'{}'".format(self.statement.query.db.escape(value))

    def get_value(self):
        return '({})'.format(', '.join(map(self._filter, self.value)))


class NotInOperator(DataOperatorFuncs, DataOperator):
    OPERATOR = 'NOT IN'

    def _filter(self, value):
        if isinstance(value, int):
            return str(value)

        return "'{}'".format(self.statement.query.db.escape(value))

    def get_value(self):
        return '({})'.format(', '.join(map(self._filter, self.value)))


class LessThanOperator(DataOperatorFuncs, DataOperator):
    OPERATOR = '<'


class LessThanOrEqualOperator(DataOperatorFuncs, DataOperator):
    OPERATOR = '<='


class GreaterThanOperator(DataOperatorFuncs, DataOperator):
    OPERATOR = '>'


class GreaterThanOrEqualOperator(DataOperatorFuncs, DataOperator):
    OPERATOR = '>='


class Keyword(object):
    def __init__(self, statement):
        self.statement = statement

    def __lt__(self, other):
        return self.KEYWORD_PRIORITY > other.KEYWORD_PRIORITY


class Condition(Keyword):
    def __init__(self, statement, data_operator, logical_operators=None):
        super().__init__(statement)
        self.data_operator = data_operator
        self.logical_operators = [] if not logical_operators else logical_operators[:]
        self.active = False

    def chain(self, logical_operators):
        self.logical_operators.extend(logical_operators)

    def get_sql(self):
        sql = '{} {}'.format(self.CLAUSE, self.data_operator.get_sql())
        for lop in self.logical_operators:
            sql = '{} {}'.format(sql, lop.get_sql())

        return sql

    def mark_active(self):
        self.active = True

    def unmark_active(self):
        self.active = False

    def is_active(self):
        return self.active


class StatementWithCondition(object):

    def __init__(self, statement, **kwargs):
        super().__init__(statement, **kwargs)

    def get_active_condition(self):
        for kw in self.get_secondary_keywords():
            if isinstance(kw, Condition) and kw.is_active():
                return kw

    def set_active_condition(self, condition):
        kw = self.get_active_condition()
        if kw:
            kw.unmark_active()

        condition.mark_active()

    def chain_condition(self, logical_operators):
        condition = self.get_active_condition()
        if condition:
            condition.chain(logical_operators)

    def is_condition_set(self):
        return bool(self.get_active_condition())

    @logical_operator
    def _and(self, column_name=None, **kwargs):
        LogicalANDClass = self.query.get_query_maker().make('logical_and')
        EqualOperatorClass = self.query.get_query_maker().make('equal_operator')

        if column_name is None and not kwargs:
            raise BeeSQLError('and operator can\'t be empty')

        if column_name is not None:
            selector = ANDColumnSelector(self, column_name)
            return selector
        else:
            data_ops = [EqualOperatorClass(self, key, val) for key, val in kwargs.items()]
            logical_ops = [LogicalANDClass(self, dop) for dop in data_ops]
            self.chain_condition(logical_ops)

        return self

    @logical_operator
    def _or(self, column_name=None, **kwargs):
        LogicalANDClass = self.query.get_query_maker().make('logical_and')
        LogicalORClass = self.query.get_query_maker().make('logical_or')
        EqualOperatorClass = self.query.get_query_maker().make('equal_operator')

        if column_name is None and not kwargs:
            raise BeeSQLError('and operator can\'t be empty')

        if column_name is not None:
            selector = ORColumnSelector(self, column_name)
            return selector
        else:
            data_ops = [EqualOperatorClass(self, key, val) for key, val in kwargs.items()]
            first_data_op = data_ops.pop(0)
            logical_ops = [LogicalANDClass(self, dop) for dop in data_ops]
            logical_ops.insert(0, LogicalORClass(self, first_data_op))
            self.chain_condition(logical_ops)

        return self


class WhereFuncMixin(object):
    def where(self, column_name=None, **kwargs):
        LogicalANDClass = self.query.get_query_maker().make('logical_and')
        EqualOperatorClass = self.query.get_query_maker().make('equal_operator')
        WhereClass = self.query.get_query_maker().make('where')

        if column_name is None and not kwargs:
            raise BeeSQLError('where statement can\'t be empty')

        if column_name is not None:
            if self.is_condition_set():
                selector = ANDColumnSelector(self, column_name)
            else:
                selector = WhereColumnSelector(self, column_name)

            return selector
        else:
            data_ops = [EqualOperatorClass(self, key, val) for key, val in kwargs.items()]
            data_op = data_ops.pop(0)
            logical_ops = [LogicalANDClass(self, dop) for dop in data_ops]
            where_keyword = WhereClass(self, data_op, logical_ops)
            self.set_active_condition(where_keyword)
            self.add_secondary_keyword(where_keyword)

            return self


class HavingFuncMixin(object):
    def having(self, column_name=None, **kwargs):
        LogicalANDClass = self.query.get_query_maker().make('logical_and')
        EqualOperatorClass = self.query.get_query_maker().make('equal_operator')
        HavingClass = self.query.get_query_maker().make('having')

        if column_name is None and not kwargs:
            raise BeeSQLError('having statement can\'t be empty')

        if column_name is not None:
            selector = HavingColumnSelector(self, column_name)
            return selector
        else:
            data_ops = [EqualOperatorClass(self, key, val) for key, val in kwargs.items()]
            data_op = data_ops.pop(0)
            logical_ops = [LogicalANDClass(self, dop) for dop in data_ops]
            having_keyword = HavingClass(self, data_op, logical_ops)
            self.set_active_condition(having_keyword)
            self.add_secondary_keyword(having_keyword)

        return self


class WhereCondition(Condition):
    KEYWORD_PRIORITY = 2
    CLAUSE = 'WHERE'


class GroupBy(Keyword):
    KEYWORD_PRIORITY = 3

    def __init__(self, statement, *column_names):
        if not column_names:
            raise BeeSQLError('Group by expects one or more column names')

        super().__init__(statement)
        self.columns = column_names

    def get_sql(self):
        sql = 'GROUP BY {}'.format(', '.join(self.columns))
        return sql


class HavingCondition(Condition):
    KEYWORD_PRIORITY = 4
    CLAUSE = 'HAVING'


class OrderBy(Keyword):
    KEYWORD_PRIORITY = 5

    def __init__(self, statement, **column_names):
        super().__init__(statement)
        self.columns = [{'name': key, 'order': 'ASC' if val == 1 else 'DESC'} for key, val in column_names.items()]

    def get_sql(self):
        sql = 'ORDER BY {}'.format(', '.join(['{} {}'.format(col['name'], col['order']) for col in self.columns]))
        return sql


class Limit(Keyword):
    KEYWORD_PRIORITY = 6

    def __init__(self, statement, limit, offset=0):
        super().__init__(statement)
        self.limit = int(limit)
        self.offset = int(offset)

    def get_sql(self):
        sql = 'LIMIT {} OFFSET {}'.format(self.limit, self.offset)
        return sql


class Aggregation(object):
    def __init__(self, column_name, as_name=None):
        self.column_name = column_name
        self.as_name = as_name

    def get_sql(self):
        sql = self._get_sql()
        return sql


class CountAggregation(AggregationFuncs, Aggregation):
    FUNCTION_NAME = 'COUNT'


class SumAggregation(AggregationFuncs, Aggregation):
    FUNCTION_NAME = 'SUM'


class AvgAggregation(AggregationFuncs, Aggregation):
    FUNCTION_NAME = 'AVG'


class MaxAggregation(AggregationFuncs, Aggregation):
    FUNCTION_NAME = 'MAX'


class MinAggregation(AggregationFuncs, Aggregation):
    FUNCTION_NAME = 'MIN'


class Statement(object):

    def __init__(self, query, **kwargs):
        self.query = query
        self.secondary_keywords = []

    def __repr__(self):
        return '{}: {}'.format(self.__class__, self.get_sql())

    def __iter__(self):
        rows = self.execute().all()
        return iter(rows)

    def __getitem__(self, key):
        rows = self.execute().all()
        return rows[key]

    def add_secondary_keyword(self, keyword):
        self.secondary_keywords.append(keyword)

    def get_secondary_keywords(self, ordered=False):
        if not ordered:
            return self.secondary_keywords

        ordered = []
        kws = self.secondary_keywords[:]
        heapq.heapify(kws)

        while kws:
            ordered.insert(0, heapq.heappop(kws))

        return ordered

    def execute(self):
        res = None
        with self.query.db.connect() as conn:
            res = conn.execute(self)

        return res

    def get_sql(self):
        sql = self._get_sql()
        for sk in self.get_secondary_keywords(ordered=True):
            sql = '{} {}'.format(sql, sk.get_sql())

        return sql


class Select(WhereFuncMixin, HavingFuncMixin, StatementWithCondition, Statement):

    def __init__(self, query, aggregations=None, *args):
        super().__init__(query)
        self.aggregations = aggregations or []
        self.all = all
        self.fields = list(set(args))

    def select(self, *args):
        q_maker = self.query.get_query_maker()
        fields = filter(lambda x: isinstance(x, str), args)
        fields_set = set(self.fields)
        fields_set.update(fields)
        self.fields = list(fields_set)

        agg_fields = filter(lambda x: isinstance(x, AggregationField), args)
        aggregation_queries = [q_maker.make(agg.QUERY_PART_NAME)(agg.column_name, agg.as_name) for agg in agg_fields]
        for agg_query in aggregation_queries:
            self.add_aggregation(agg_query)

        return self

    @secondary_keyword
    def group_by(self, *column_names):
        if not column_names:
            raise BeeSQLError('group_by expects one or more column names.')

        GroupByClass = self.query.get_query_maker().make('group_by')
        group_by = GroupByClass(self, *column_names)
        return group_by

    @secondary_keyword
    def order_by(self, *column_names):
        if not column_names:
            raise BeeSQLError('order_by expects one or more column names.')

        columns_dict = {}
        for col in column_names:
            column_name = col[1:] if col.startswith('-') else col
            order = 0 if col.startswith('-') else 1
            columns_dict[column_name] = order

        OrderByClass = self.query.get_query_maker().make('order_by')
        order_by_keyword = OrderByClass(self, **columns_dict)
        return order_by_keyword

    @secondary_keyword
    def limit(self, limit, offset=0):
        limit = int(limit)
        offset = int(offset)
        LimitClass = self.query.get_query_maker().make('limit')
        limit_keyword = LimitClass(self, limit, offset)
        return limit_keyword

    @aggregation
    def sum(self, column_name, as_name=None):
        AggregationClass = self.query.get_query_maker().make('sum_aggregation')
        return AggregationClass(column_name, as_name)

    @aggregation
    def avg(self, column_name, as_name=None):
        AggregationClass = self.query.get_query_maker().make('avg_aggregation')
        return AggregationClass(column_name, as_name)

    @aggregation
    def count(self, column_name, as_name=None):
        AggregationClass = self.query.get_query_maker().make('count_aggregation')
        return AggregationClass(column_name, as_name)

    @aggregation
    def max(self, column_name, as_name=None):
        AggregationClass = self.query.get_query_maker().make('max_aggregation')
        return AggregationClass(column_name, as_name)

    @aggregation
    def min(self, column_name, as_name=None):
        AggregationClass = self.query.get_query_maker().make('min_aggregation')
        return AggregationClass(column_name, as_name)

    def add_aggregation(self, aggregation):
        self.aggregations.append(aggregation)

    def _get_sql(self):
        if not self.fields and not self.aggregations:
            fields = '*'
        else:
            fields = self.fields[:]
            fields.extend([ag.get_sql() for ag in self.aggregations])
            fields = ', '.join(fields)

        if self.query.table_alias:
            table_name = '{} AS {}'.format(self.query.table, self.query.table_alias)
        else:
            table_name = self.query.table

        params = {
            'fields': fields,
            'table': table_name,
        }
        sql = "SELECT {fields} FROM {table}".format(**params)
        return sql


class Update(WhereFuncMixin, StatementWithCondition, Statement):
    def __init__(self, query, prevent_update_all=True, **kwargs):
        if not kwargs:
            raise BeeSQLError('Update values not provided.')

        super().__init__(query)
        self.prevent_update_all = prevent_update_all
        self.values = kwargs.copy()

    def _format(self, value):
        db = self.query.db
        if isinstance(value, int):
            return value

        return "'{}'".format(db.escape(value))

    def _get_sql(self):
        params = {
            'values': ' AND '.join(['{} = {}'.format(key, self._format(val)) for key, val in self.values.items()]),
            'table': self.query.table,
        }
        sql = "UPDATE {table} SET {values}".format(**params)
        return sql

    def update(self, **kwargs):
        self.values.update(kwargs)
        return self


class Delete(WhereFuncMixin, StatementWithCondition, Statement):
    def __init__(self, query, prevent_delete_all=True):
        super().__init__(query)
        self.prevent_delete_all = prevent_delete_all

    def _get_sql(self):
        sql = "DELETE FROM {}".format(self.query.table)
        return sql


class Insert(Statement):
    def __init__(self, query, *args):
        super().__init__(query)
        self.fields = args
        self.values = []

    def row(self, *args):
        self.values.append(args)
        return self

    def _format(self, value):
        db = self.query.db
        if isinstance(value, int):
            return str(value)

        return "'{}'".format(db.escape(value))

    def _get_sql(self):
        sql = 'INSERT INTO {table} {fields} VALUES {values}'
        values = ','.join(['({})'.format(','.join([self._format(v) for v in values])) for values in self.values])
        params = {
            'table': self.query.table,
            'fields': '({})'.format(', '.join(self.fields)) if self.fields else '',
            'values': values,
        }
        sql = sql.format(**params)
        return sql


class Count(StatementWithCondition, Statement):
    def __init__(self, query):
        super().__init__(query)

    def _get_sql(self):
        sql = "SELECT count(*) AS count FROM {}".format(self.query.table)
        return sql


class ColumnSelector(object):
    def __init__(self, statement, column_name):
        self.statement = statement
        self.column_name = column_name

    def get_operator(self, query_operator_name, value):
        OperatorClass = self.statement.query.get_query_maker().make(query_operator_name)
        op = OperatorClass(self.statement, self.column_name, value)
        return op

    @complete_condition('equal_operator')
    def eq(self, value):
        pass

    @complete_condition('not_equal_operator')
    def neq(self, value):
        pass

    @complete_condition('less_than_operator')
    def lt(self, value):
        pass

    @complete_condition('less_than_or_equal_operator')
    def lte(self, value):
        pass

    @complete_condition('greater_than_operator')
    def gt(self, value):
        pass

    @complete_condition('greater_than_or_equal_operator')
    def gte(self, value):
        pass

    def _in(self, *args):
        if not args:
            raise BeeSQLError('No arguments provided for in operator')

        value = args[:]
        InOperatorClass = self.statement.query.get_query_maker().make('in_operator')
        op = InOperatorClass(self.statement, self.column_name, value)
        return self.complete(op)

    def nin(self, *args):
        if not args:
            raise BeeSQLError('No arguments provided for in operator')

        value = args[:]
        NotInOperatorClass = self.statement.query.get_query_maker().make('not_in_operator')
        op = NotInOperatorClass(self.statement, self.column_name, value)
        return self.complete(op)

    def __repr__(self):
        return '{}: {}'.format(self.__class__, self.column_name)


class ConditionalColumnSelector(ColumnSelector):
    def complete(self, data_operator):
        condition_class = self.statement.query.get_query_maker().make(self.CONDITION_QUERY_PART)
        condition = condition_class(self.statement, data_operator)
        self.statement.set_active_condition(condition)
        self.statement.add_secondary_keyword(condition)

        return self.statement


class LogicalColumnSelector(ColumnSelector):
    def complete(self, data_operator):
        operator_class = self.statement.query.get_query_maker().make(self.OPERATOR_QUERY_PART)
        logical_operator = operator_class(self.statement.query, data_operator)
        self.statement.chain_condition([logical_operator])

        return self.statement


class WhereColumnSelector(ConditionalColumnSelector):
    CONDITION_QUERY_PART = 'where'


class HavingColumnSelector(ConditionalColumnSelector):
    CONDITION_QUERY_PART = 'having'


class ANDColumnSelector(LogicalColumnSelector):
    OPERATOR_QUERY_PART = 'logical_and'


class ORColumnSelector(LogicalColumnSelector):
    OPERATOR_QUERY_PART = 'logical_or'


class Query(object):
    """ SQL generator """

    def __init__(self, db, table_name=None, table_alias=None):
        self.db = db
        self.statement = None
        self._table = table_name
        self.table_alias = table_alias

    def __repr__(self):
        try:
            sql = self.get_sql()
        except BeeSQLError:
            sql = ''

        return '{}: {}'.format(self.__class__, sql)

    def reset(self):
        self.statement = None

    @property
    def table(self):
        return self._table

    def on(self, table, table_alias):
        self.reset()
        self._table = table
        self.table_alias = table_alias
        return self

    @primary_keyword
    def select(self, *args):
        q_maker = self.get_query_maker()
        fields = filter(lambda x: isinstance(x, str), args)
        agg_fields = filter(lambda x: isinstance(x, AggregationField), args)
        aggregation_queries = [q_maker.make(agg.QUERY_PART_NAME)(agg.column_name, agg.as_name) for agg in agg_fields]
        select = self.get_query_maker().make('select')(self, aggregation_queries, *fields)
        return select

    @primary_keyword
    def update(self, **kwargs):
        if not kwargs:
            raise BeeSQLError('Values can\'t be empty')

        update = self.get_query_maker().make('update')(self, **kwargs)
        return update

    @primary_keyword
    def delete(self):
        delete_keyword = self.get_query_maker().make('delete')(self)
        return delete_keyword

    @primary_keyword
    def insert(self, *args):
        insert_keyword = self.get_query_maker().make('insert')(self, *args)
        return insert_keyword

    @primary_keyword
    def count(self):
        count_statement = self.get_query_maker().make('count')(self)
        return count_statement

    def set_statement(self, statement):
        if not isinstance(statement, Statement):
            raise BeeSQLError('Expected instance of {}. got {}'.format(type(Statement), type(statement)))

        self.statement = statement

    def get_query_maker(self):
        return QueryMaker

    def get_sql(self):
        if not self.statement:
            raise BeeSQLError('No statement created.')

        return self.statement.get_sql()


class QueryRegistry(type):
    def __new__(cls, name, bases, attrs):
        if 'query_parts' in attrs:
            query_parts = {}
            for base in bases:
                try:
                    query_parts = base.query_parts.copy()
                except AttributeError:
                    pass
                else:
                    break

            query_parts.update(attrs['query_parts'])
            attrs['query_parts'] = query_parts

        return super(QueryRegistry, cls).__new__(cls, name, bases, attrs)


class QueryMaker(metaclass=QueryRegistry):

    query_parts = {
        'select': Select,
        'update': Update,
        'delete': Delete,
        'insert': Insert,
        'count': Count,
        'where': WhereCondition,
        'having': HavingCondition,
        'group_by': GroupBy,
        'order_by': OrderBy,
        'limit': Limit,
        'logical_and': LogicalAND,
        'logical_or': LogicalOR,
        'equal_operator': EqualOperator,
        'not_equal_operator': NotEqualOperator,
        'in_operator': InOperator,
        'not_in_operator': NotInOperator,
        'less_than_operator': LessThanOperator,
        'greater_than_operator': GreaterThanOperator,
        'less_than_or_equal_operator': LessThanOrEqualOperator,
        'greater_than_or_equal_operator': GreaterThanOrEqualOperator,
        'count_aggregation': CountAggregation,
        'sum_aggregation': SumAggregation,
        'avg_aggregation': AvgAggregation,
        'max_aggregation': MaxAggregation,
        'min_aggregation': MinAggregation,
    }

    @classmethod
    def make(cls, query_part_name):
        try:
            return cls.query_parts[query_part_name]
        except KeyError:
            raise AttributeError("No query part named '{}'".format(query_part_name))
