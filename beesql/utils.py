def validate_keyword_chaining(query, keyword):
    if not query.primary_keyword:
        return True

    return isinstance(query.primary_keyword, type(keyword))


class Alias(object):
    """ Represent an alias. """
    def __init__(self, name, alias_name):
        self.name = name
        self.alias = alias_name


def alias(name, alias_name):
    return Alias(name, alias_name)


def field(table_name, field):
    return '{}__{}'.format(table_name, field)
