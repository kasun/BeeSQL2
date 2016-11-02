class DataOperatorFuncs(object):
    def get_operator(self):
        return self.OPERATOR

    def get_value(self):
        if isinstance(self.value, int):
            return self.value

        return "'{}'".format(self.query.db.escape(self.value))
