from rule import Rule, Condition, Trigger, Filter, Webhook
from query.selection import Selection, ScalarSelector, AndClause, OrClause


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
    for k in selection:
        if k in ['and', 'or']:
            selector = decode_compound_clause(selection[k], selection_type, k)
        else:
            selector = decode_scalar_selector(selection[k], selection_type)
        s.add(selector)
    return s


class TempoIQDecoder(object):
    def __init__(self):
        pass

    def __call__(self, dct):
        self.decode(dct)

    def decode(self, dct):
        if dct.get('rule'):
            return self.decode_rule(dct)
        return dct

    def decode_rule(self, rule):
        name = rule['name']
        key = rule['key']
        alert_by = rule['alerts']
        action = Webhook(rule['action']['url'])

        conditions = []
        for c in rule['conditions']:
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
