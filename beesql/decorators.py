from functools import wraps

from .utils import validate_keyword_chaining
from .exceptions import BeeSQLError


def primary_keyword(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.table:
            raise BeeSQLError('No table selected. Use Query.on to select a table first')

        keyword = func(self, *args, **kwargs)
        if not validate_keyword_chaining(self, keyword):
            raise BeeSQLError('Invalid keyword chaining.')

        self.primary_keyword = keyword
        return self

    return wrapper


def logical_operator(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.is_condition_set():
            raise BeeSQLError('No where condition set.')

        return func(self, *args, **kwargs)

    return wrapper
