from functools import wraps

from ..exceptions import BeeSQLError


def primary_keyword(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.table:
            raise BeeSQLError('No table selected. Use Query.on to select a table first')

        statement = func(self, *args, **kwargs)
        self.set_statement(statement)
        return statement

    return wrapper


def secondary_keyword(func):
    """ Convert a statement method into a secondary keyword generator. """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        keyword = func(self, *args, **kwargs)
        self.add_secondary_keyword(keyword)
        return self

    return wrapper


def logical_operator(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.is_condition_set():
            raise BeeSQLError('No where condition set.')

        return func(self, *args, **kwargs)

    return wrapper


def aggregation(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        aggregation_ = func(self, *args, **kwargs)
        self.add_aggregation(aggregation_)
        return self

    return wrapper


def complete_condition(query_part_name):
    """ Works with ColumnSelector class. """
    def decorator(func):
        @wraps(func)
        def wrapper(self, value, **kwargs):
            operator = self.get_operator(query_part_name, value)
            return self.complete(operator)

        return wrapper

    return decorator
