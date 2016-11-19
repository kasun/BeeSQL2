class DataOperatorFuncs(object):
    def get_operator(self):
        return self.OPERATOR

    def get_value(self):
        if isinstance(self.value, int):
            return self.value

        return "'{}'".format(self.statement.query.db.escape(self.value))


class QueryMakerFuncs(object):

    @classmethod
    def make_select(cls):
        try:
            return cls.SELECT_CLASS
        except AttributeError:
            return super(QueryMakerFuncs, cls).make_select()

    @classmethod
    def make_where(cls):
        try:
            return cls.WHERE_CLASS
        except AttributeError:
            return super(QueryMakerFuncs, cls).make_where()

    @classmethod
    def make_logical_and(cls):
        try:
            return cls.LogicalAND_CLASS
        except AttributeError:
            return super(QueryMakerFuncs, cls).make_logical_and()

    @classmethod
    def make_logical_or(cls):
        try:
            return cls.LogicalOR_CLASS
        except AttributeError:
            return super(QueryMakerFuncs, cls).make_logical_or()

    @classmethod
    def make_equal_operator(cls):
        try:
            return cls.EqualOperatorClass
        except AttributeError:
            return super(QueryMakerFuncs, cls).make_equal_operator()

    @classmethod
    def make_not_equal_operator(cls):
        try:
            return cls.NotEqualOperatorClass
        except AttributeError:
            return super(QueryMakerFuncs, cls).make_not_equal_operator()

    @classmethod
    def make_in_operator(cls):
        try:
            return cls.InOperatorClass
        except AttributeError:
            return super(QueryMakerFuncs, cls).make_in_operator()

    @classmethod
    def make_not_in_operator(cls):
        try:
            return cls.NotInOperatorClass
        except AttributeError:
            return super(QueryMakerFuncs, cls).make_not_in_operator()


class AggregationFuncs(object):
    def _get_sql(self):
        return '{}({}) AS {}'.format(self.FUNCTION_NAME, self.column_name,
                                     self.as_name or '{}_{}'.format(self.FUNCTION_NAME.lower(), self.column_name))
