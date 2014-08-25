from query.selection import ScalarSelectable, DictSelectable


class Sensor(object):
    key = ScalarSelectable('devices', 'key')
    name = ScalarSelectable('devices', 'name')
    attributes = DictSelectable('devices', 'attributes')

    def __init__(self, key, name='', attributes={}):
        self.key = key
        self.name = name
        self.attributes = attributes
