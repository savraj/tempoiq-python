from rule import Rule, Condition, Trigger, Filter, Webhook
from device import Device
from sensor import Sensor
from query.selection import Selection, ScalarSelector, AndClause, OrClause
import json


def decode_attributes_selector(selector, selection_type='devices'):
    return ScalarSelector(selection_type, 'attributes', selector['attributes'])


def decode_key_selector(selector, selection_type='devices'):
    return ScalarSelector(selection_type, 'key', selector['key'])


def decode_scalar_selector(selector, selection_type='devices'):
    key = selector.keys()[0]
    if key == 'attributes':
        return decode_attributes_selector(selector, selection_type)
    else:
        return decode_key_selector(selector, selection_type)


def decode_compound_clause(selectors, selection_type='devices', t='and'):
    if t == 'and':
        clause = AndClause()
    else:
        clause = OrClause()

    for s in selectors:
        subt = s.keys()[0]
        if subt in ['and', 'or']:
            subclause = decode_compound_clause(s[subt], selection_type, subt)
            clause.add(subclause)
        else:
            parsed = decode_scalar_selector(s, selection_type)
            clause.add(parsed)

    return clause


def decode_selection(selection, selection_type='devices'):
    s = Selection()
    if selection == 'all':
        return s
    for k in selection:
        if k in ['and', 'or']:
            selector = decode_compound_clause(selection[k], selection_type, k)
        else:
            selector = decode_scalar_selector(selection, selection_type)
        s.add(selector)
    return s


class DeviceDecoder(object):
    def __init__(self):
        pass

    def __call__(self, dct):
        if dct.get('sensors'):  # Main device object
            return self.decode_device(dct)
        return dct

    def decode_device(self, dct):
        key = dct['key']
        attributes = dct['attributes']
        name = dct['name']
        sensors = []
        for s in dct['sensors']:
            _sensor = Sensor(s['key'], name=s['name'], attributes=s['attributes'])
            sensors.append(_sensor)

        return Device(key, name=name, attributes=attributes, sensors=sensors)


class TempoIQDecoder(object):
    def __init__(self):
        pass

    def __call__(self, dct):
        return self.decode(dct)

    def decode(self, dct):
        if dct.get('rule'):
            return self.decode_rule(dct)
        return dct

    def decode_rule(self, rule):
        name = rule['rule']['name']
        key = rule['rule']['key']
        alert_by = rule['alerts']
        action = Webhook(rule['rule']['actions'][0]['url'])

        conditions = []
        for c in rule['rule']['conditions']:
            trigger = Trigger(c['trigger']['name'],
                              c['trigger']['arguments'])
            filters = []
            for f in c['filter']['and']:
                _filter = Filter(f['operation'], f['type'], f['arguments'])
                filters.append(_filter)
            condition = Condition(filters, trigger)
            conditions.append(condition)

        selection = {
            'devices': decode_selection(rule['search']['filters']['devices'],
                                        selection_type='devices'),
            'sensors': decode_selection(rule['search']['filters']['sensors'],
                                        selection_type='sensors'),
        }

        return Rule(name, alert_by=alert_by, key=key, conditions=conditions,
                    action=action, selection=selection)

    def decode_selection(self, dct):
        pass
