import warnings
import exceptions
from selection import Selection, ScalarSelector, OrClause, AndClause
from functions import *
from tempoiq.protocol import Rule


PIPEMSG = 'Pipeline functions passed to monitor call currently have no effect'
DEVICEMSG = 'Pipeline functions passed to device reads have no effect'
ROLLUPMSG = 'Rollup, find, and multi-rollup must have a start and end passed to them'
DELETEMSG = 'Deleting data from sensors requires a start and end time'


def extract_key_for_monitoring(selection):
    if hasattr(selection.selection, 'selectors'):
        if len(selection.selection.selectors) > 1:
            msg = 'monitoring rules may only be read out by one key at a time'
            raise ValueError(msg)
        return selection.selection.selectors[0].value
    else:
        return selection.selection.value


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
                if start is None or end is None:
                    raise ValueError(ROLLUPMSG)
                function.args.append(start)
            elif isinstance(function, Interpolation):
                function.args.extend([start, end])

    def aggregate(self, function):
        self.pipeline.append(Aggregation(function))
        return self

    def annotations(self):
        if not isinstance(self.object_type, Rule):
            raise TypeError('Annotations only applies to monitoring rules')
        key = extract_key_for_monitoring(self.selection['rules'])
        return self.client.monitoring_client.get_annotations(key)

    def changes(self):
        if not isinstance(self.object_type, Rule):
            raise TypeError('Changes only applies to monitoring rules')
        key = extract_key_for_monitoring(self.selection['rules'])
        return self.client.monitoring_client.get_changelog(key)

    def convert_timezone(self, tz):
        self.pipeline.append(ConvertTZ(tz))
        return self

    def delete(self, **kwargs):
        if self.object_type == 'devices':
            self.operation = APIOperation('find', {'quantifier': 'all'})
            self.client.delete_device(self)
        elif self.object_type == 'sensors':
            start = kwargs.get('start')
            end = kwargs.get('end')
            if start is None or end is None:
                raise ValueError(DELETEMSG)
            args = {'start': start, 'stop': end}
            self.operation = APIOperation('delete', args)
            self.client.delete_from_sensors(self, start, end)
        elif self.object_type == 'rules':
            key = extract_key_for_monitoring(self.selection['rules'])
            self.client.monitoring_client.delete_rule(key)

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

    def logs(self):
        if not isinstance(self.object_type, Rule):
            raise TypeError('Logs only applies to monitoring rules')
        key = extract_key_for_monitoring(self.selection['rules'])
        return self.client.monitoring_client.get_logs(key)

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
            #this is set here to be used by the encoder to correctly specify
            #the last step of the operation in the JSON
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

    #TODO: latest() will eventually be implemented in terms of this
    #def single_value(self, start=None, end=None, include_selection=False):
    #    if self.object_type == 'sensors':
    #        args = {'include_selection': include_selection}
    #        self.operation = APIOperation('single_value', args)
    #        self._normalize_pipeline_functions(start, end)
    #        return(self.client.single_value(self))
    #    else:
    #        msg = 'Single value only applies to sensors'
    #        raise TypeError(msg)

    def latest(self, start=None, end=None, include_selection=False):
        if self.object_type == 'sensors':
            args = {'include_selection': include_selection}
            self.operation = APIOperation('single_value', args)
            self._normalize_pipeline_functions(start, end)
            return(self.client.single_value(self))
        else:
            msg = 'Latest function only applies to sensors'
            raise TypeError(msg)

    def usage(self):
        if not isinstance(self.object_type, Rule):
            raise TypeError('Usage only applies to monitoring rules')
        key = extract_key_for_monitoring(self.selection['rules'])
        return self.client.monitoring_client.get_usage(key)
