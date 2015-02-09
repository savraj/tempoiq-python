import json
from query.selection import AndClause, Compound, OrClause, ScalarSelector


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
        'Webhook': 'encode_webhook',
        'Email': 'encode_email'
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
            'search': {
                'select': 'sensors',
                'filters': {
                    'devices': read_encoder.default(
                        rule.selection['devices']),
                    'sensors': read_encoder.default(
                        rule.selection['sensors'])
                }
            },
            'alerts': rule.alert_by,
            'rule': {
                'conditions': map(self.encode_condition, rule.conditions),
                'name': rule.name,
                'actions': [self.default(rule.action)],
            }
        }

        if not j['search']['filters']['devices']:
            if not j['search']['filters']['sensors']:
                j['search']['filters']['devices'] = 'all'
                j['search']['filters']['sensors'] = 'all'
            else:
                j['search']['filters']['devices'] = 'all'
        else:
            if not j['search']['filters']['sensors']:
                j['search']['filters']['sensors'] = 'all'

        if rule.key is not None:
            j['rule']['key'] = rule.key

        if rule.status is not None:
            j['rule']['status'] = rule.status

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

    def encode_email(self, email):
        return {
            'address': email.address
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

        object_type = None
        result = []
        for selector in clause.selectors:
            if isinstance(selector, (AndClause, OrClause)):
                result.append(self.encode_compound_clause(selector))
            elif isinstance(selector, ScalarSelector):
                if object_type is None:
                    object_type = selector.selection_type
                else:
                    if object_type != selector.selection_type:
                        msg = 'Selectors in a compound clause must be of uniform type'
                        raise TypeError(msg)
                result.append(self.encode_scalar_selector(selector))
            else:
                raise ValueError("invalid selector type")

        return {
            name: result
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

        if builder.ordering is not None:
            j['search']['ordering'] = builder.ordering

        if not j['search']['filters']['devices']:
            if not j['search']['filters']['sensors']:
                j['search']['filters']['devices'] = 'all'
                j['search']['filters']['sensors'] = 'all'
            else:
                j['search']['filters']['devices'] = 'all'
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
                return self.default(selection.selection)
        return self.encode_scalar_selector(selection.selection)
