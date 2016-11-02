def validate_keyword_chaining(query, keyword):
    if not query.primary_keyword:
        return True

    return isinstance(query.primary_keyword, type(keyword))
