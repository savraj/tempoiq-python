import json
from query.selection import AndClause, Compound


class TempoIQEncoder(json.JSONEncoder):
    def encode_point(self, point):
        return {
            't': self.encode_datetime(point.timestamp),
            'v': point.value}

    def encode_datetime(self, dt):
        return dt.isoformat()


class WriteEncoder(TempoIQEncoder):
    encoders = {
        'Device': 'encode_device',
        'Sensor': 'encode_sensor',
        'Point': 'encode_point',
        'datetime': 'encode_datetime',
        'Rule': 'encode_rule',
        'Trigger': 'encode_trigger',
        'Webhook': 'encode_webhook'
    }

    def default(self, o):
        encoder_name = self.encoders.get(o.__class__.__name__)
        if encoder_name is None:
            super(TempoIQEncoder, self).default(o)
        encoder = getattr(self, encoder_name)
        return encoder(o)

    def encode_condition(self, condition):
        return {
            'trigger': self.encode_trigger(condition.trigger),
            'filter': {
                'and': map(self.encode_filter, condition.filters)
            }
        }

    def encode_device(self, device):
        return device.key

    def encode_filter(self, _filter):
        return {
            'operation': _filter.inclusion,
            'type': _filter.filter_type,
            'arguments': _filter.args
        }

    def encode_rule(self, rule):
        read_encoder = ReadEncoder()
        j = {
            'conditions': map(self.encode_condition, rule.conditions),
            'name': rule.name,
            'alerts': rule.alert_by,
            'actions': [self.default(rule.action)],
            'selection': {
                'search': {
                    'filters': {
                        'devices': read_encoder.default(
                            rule.selection['devices']),
                        'sensors': read_encoder.default(
                            rule.selection['sensors'])
                    }
                }
            }
        }

        if rule.key is not None:
            j['key'] = rule.key
        return j

    def encode_sensor(self, sensor):
        return sensor.key

    def encode_trigger(self, trigger):
        return {
            'name': trigger.trigger_type,
            'arguments': trigger.args
        }

    def encode_webhook(self, webhook):
        return {
            'url': webhook.url
        }


class CreateEncoder(TempoIQEncoder):
    encoders = {
        'Device': 'encode_device',
        'Sensor': 'encode_sensor'
    }

    def default(self, o):
        encoder_name = self.encoders.get(o.__class__.__name__)
        if encoder_name is None:
            super(TempoIQEncoder, self).default(o)
        encoder = getattr(self, encoder_name)
        return encoder(o)

    def encode_device(self, device):
        return {
            'key': device.key,
            'name': device.name,
            'attributes': device.attributes,
            'sensors': map(self.encode_sensor, device.sensors)
        }

    def encode_sensor(self, sensor):
        return {
            'key': sensor.key,
            'name': sensor.name,
            'attributes': sensor.attributes
        }


class ReadEncoder(TempoIQEncoder):
    encoders = {
        'Point': 'encode_point',
        'datetime': 'encode_datetime',
        'ScalarSelector': 'encode_scalar_selector',
        'AndClause': 'encode_compound_clause',
        'OrClause': 'encode_compound_clause',
        'QueryBuilder': 'encode_query_builder',
        'Selection': 'encode_selection',
        'Find': 'encode_function',
        'Interpolation': 'encode_function',
        'MultiRollup': 'encode_function',
        'Rollup': 'encode_function',
        'Aggregation': 'encode_function',
        'ConvertTZ': 'encode_function'
    }

    def default(self, o):
        encoder_name = self.encoders.get(o.__class__.__name__)
        if encoder_name is None:
            super(TempoIQEncoder, self).default(o)
        encoder = getattr(self, encoder_name)
        return encoder(o)

    def encode_compound_clause(self, clause):
        name = None
        if isinstance(clause, AndClause):
            name = 'and'
        else:
            name = 'or'

        return {
            name: map(self.encode_scalar_selector, clause.selectors)
        }

    def encode_function(self, function):
        return {
            'name': function.name,
            'arguments': function.args
        }

    def encode_query_builder(self, builder):
        j = {
            'search': {
                'select': builder.object_type,
                'filters': {
                    'devices': self.encode_selection(
                        builder.selection['devices']),
                    'sensors': self.encode_selection(
                        builder.selection['sensors'])
                }
            },
            builder.operation.name: builder.operation.args
        }

        if not j['search']['filters']['devices']:
            if not j['search']['filters']['sensors']:
                j['search']['filters'] = {}
        else:
            if not j['search']['filters']['sensors']:
                j['search']['filters']['sensors'] = 'all'

        if len(builder.pipeline) > 0:
            j['fold'] = {
                'functions': map(self.encode_function, builder.pipeline)

            }
        return j

    def encode_scalar_selector(self, selector):
        return {
            selector.key: selector.value
        }

    def encode_selection(self, selection):
        if selection.selection is None:
            return {}
        if isinstance(selection.selection, Compound):
            if len(selection.selection.selectors) == 0:
                return {}
            else:
                return self.encode_compound_clause(selection.selection)
        return self.encode_scalar_selector(selection.selection)
