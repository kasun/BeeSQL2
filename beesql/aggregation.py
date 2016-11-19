class AggregationField(object):
    def __init__(self, column_name, as_name=None):
        self.column_name = column_name
        self.as_name = as_name


class SumAggregationField(AggregationField):
    QUERY_PART_NAME = 'sum_aggregation'


class AvgAggregationField(AggregationField):
    QUERY_PART_NAME = 'avg_aggregation'


class CountAggregationField(AggregationField):
    QUERY_PART_NAME = 'count_aggregation'


class MaxAggregationField(AggregationField):
    QUERY_PART_NAME = 'max_aggregation'


class MinAggregationField(AggregationField):
    QUERY_PART_NAME = 'min_aggregation'


def _sum(column_name, as_name=None):
    return SumAggregationField(column_name, as_name)


def avg(column_name, as_name=None):
    return AvgAggregationField(column_name, as_name)


def count(column_name, as_name=None):
    return CountAggregationField(column_name, as_name)


def _max(column_name, as_name=None):
    return MaxAggregationField(column_name, as_name)


def _min(column_name, as_name=None):
    return MinAggregationField(column_name, as_name)
