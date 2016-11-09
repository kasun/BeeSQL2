from .base import Query
from .base import QueryMaker, Select
from .mixins import QueryMakerFuncs


class MySQLSelect(Select):
    pass


class MySQLQuery(Query):
    def get_query_maker(self):
        return MySQLQueryMaker


class MySQLQueryMaker(QueryMakerFuncs, QueryMaker):
    SELECT_CLASS = MySQLSelect

    query_parts = {
        'select': MySQLSelect,
    }
