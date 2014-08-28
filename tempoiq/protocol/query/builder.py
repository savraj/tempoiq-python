import warnings
import exceptions
from selection import Selection, ScalarSelector, OrClause, AndClause
from functions import *


PIPEMSG = 'Pipeline functions passed to monitor call currently have no effect'
DEVICEMSG = 'Pipeline functions passed to device reads have no effect'


def extract_key_for_monitoring(selection):
    if len(selection.selection.selectors) > 1:
        msg = 'monitoring rules may only be read out by one key at a time'
        raise ValueError(msg)
    return selection.selection.selectors[0].value


class QueryBuilder(object):
    def __init__(self, client, object_type):
        self.client = client
        self.object_type = object_type.__name__.lower() + 's'
        self.selection = {
            'devices': Selection(),
            'sensors': Selection(),
            'rules': Selection()
        }
        self.pipeline = []
        self.operation = None

    def _handle_monitor_read(self, **kwargs):
        key = extract_key_for_monitoring(self.selection['rules'])
        method_name = kwargs['__method$$']
        method = getattr(self.client.monitoring_client, method_name)
        return method(key)

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

    def monitor(self, rule):
        if self.pipeline:
            warnings.warn(PIPEMSG, exceptions.FutureWarning)
        rule.selection = self.selection
        return self.client.monitor(rule)

    def multi_rollup(self, functions, period):
        self.pipeline.append(MultiRollup(functions, period))
        return self

    def rollup(self, function, period):
        self.pipeline.append(Rollup(function, period))
        return self

    def read(self, **kwargs):
        if self.object_type == 'sensors':
            start = kwargs['start']
            end = kwargs['end']
            args = {'start': start, 'stop': end}
            self.operation = APIOperation('read', args)
            self._normalize_pipeline_functions(start, end)
            return self.client.read(self)
        elif self.object_type == 'devices':
            if self.pipeline:
                self.pipeline = []
                warnings.warn(DEVICEMSG, exceptions.FutureWarning)
            size = kwargs.get('size', 5000)
            self.operation = APIOperation('find', {'quantifier': 'all'})
            return self.client.search_devices(self, size=size)
        elif self.object_type == 'rules':
            kwargs['__method$$'] = 'get_rule'
            return self._handle_monitor_read(**kwargs)
        else:
            msg = 'Only sensors, devices, and rules can be selected'
            raise TypeError(msg)
