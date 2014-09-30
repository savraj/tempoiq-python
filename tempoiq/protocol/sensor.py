from query.selection import ScalarSelectable, DictSelectable


class Sensor(object):
    """Representation of a Sensor in TempoIQ.

    :param string key:
    :param string name:
    :param dict attributes:
    """
    key = ScalarSelectable('sensors', 'key')
    name = ScalarSelectable('sensors', 'name')
    attributes = DictSelectable('sensors', 'attributes')

    def __init__(self, key, name='', attributes={}):
        self.key = key
        self.name = name
        self.attributes = attributes
