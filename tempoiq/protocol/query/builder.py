from selection import Selection, ScalarSelector, OrClause, AndClause
from functions import *


class QueryBuilder(object):
    def __init__(self, client, object_type):
        self.client = client
        self.object_type = object_type.__name__.lower() + 's'
        self.selection = {
            'devices': Selection(),
            'sensors': Selection()
        }
        self.pipeline = []
        self.operation = None

    def _normalize_pipeline_functions(self, start, end):
        for function in self.pipeline:
            if isinstance(function, (Rollup, MultiRollup, Find)):
                function.args.append(start)
            elif isinstance(function, Interpolation):
                function.args.extend([start, end])

    def aggregate(self, function):
        self.pipeline.append(Aggregation(function))
        return self

    def convert_timezone(self, tz):
        self.pipeline.append(ConvertTZ(tz))
        return self

    def filter(self, selector):
        if not isinstance(selector, (ScalarSelector, OrClause, AndClause)):
            raise TypeError('Invalid object for filter: "%s"' % selector)
        self.selection[selector.selection_type].add(selector)
        return self

    def find(self, function, period):
        self.pipeline.append(Find(function, period))
        return self

    def interpolate(self, function, period):
        self.pipeline.append(Interpolation(function, period))
        return self

    def multi_rollup(self, functions, period):
        self.pipeline.append(MultiRollup(functions, period))
        return self

    def rollup(self, function, period):
        self.pipeline.append(Rollup(function, period))
        return self

    def read(self, start, end):
        args = {'start': start, 'stop': end}
        self.operation = APIOperation('read', args)
        self._normalize_pipeline_functions(start, end)
        self.client.read(self)
