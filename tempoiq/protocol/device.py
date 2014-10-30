from query.selection import ScalarSelectable, DictSelectable


class Device(object):
    """Representation of a Device in TempoIQ.

    :param string key:
    :param string name:
    :param dict attributes:
    :param list sensors:
    """
    key = ScalarSelectable('devices', 'key')
    name = ScalarSelectable('devices', 'name')
    attributes = DictSelectable('devices', 'attributes')

    def __init__(self, key, name='', attributes={}, sensors=[]):
        self.key = key
        self.name = name
        self.attributes = attributes
        self.sensors = sensors
