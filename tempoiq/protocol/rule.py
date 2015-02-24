from query.selection import ScalarSelectable


class Rule(object):
    key = ScalarSelectable('rules', 'key')

    def __init__(self, name, alert_by=None, key=None, selection=None,
                 conditions=None, action=None, status=None):
        self.name = name
        self.alert_by = alert_by
        self.key = key
        self.selection = selection
        self.conditions = conditions if conditions else []
        self.action = action
        self.status = status if status else "active"


class Trigger(object):
    def __init__(self, trigger_type, args):
        self.trigger_type = trigger_type
        self.args = args


class Filter(object):
    def __init__(self, inclusion, filter_type, args):
        self.inclusion = inclusion
        self.filter_type = filter_type
        self.args = args


class Condition(object):
    def __init__(self, filters, trigger):
        self.filters = filters
        self.trigger = trigger


class Action(object):
    pass


class Webhook(Action):
    def __init__(self, url):
        self.url = url


class Email(Action):
    def __init__(self, address):
        self.address = address


class RuleStatus(object):
    ACTIVE = "active"
    LOGONLY = "logonly"


class ActionLog(object):
    def __init__(self, payload, recipient, response, status, action_type):
        self.payload = payload
        self.recipient = recipient
        self.response = response
        self.status = status
        self.action_type = action_type


class Instigator(object):
    def __init__(self, point, device, sensor):
        self.point = point
        self.sensor = sensor
        self.device = device


class Transition(object):
    def __init__(self, timestamp, instigator, transition_type, action_logs):
        self.timestamp = timestamp
        self.instigator = instigator
        self.to = transition_type
        self.action_logs = action_logs


class Alert(object):
    def __init__(self, alert_id, rule_key, transitions):
        self.id = alert_id
        self.rule_key = rule_key
        self.transitions = transitions

    @property
    def is_resolved(self):
        resolved = False
        for transition in self.transitions:
            if transition.to == 'ok':
                resolved = True
                break
        return resolved

    @property
    def warning_transition(self):
        for transition in self.transitions:
            if transition.to == 'warning':
                return transition

    @property
    def ok_transition(self):
        for transition in self.transitions:
            if transition.to == 'ok':
                return transition
