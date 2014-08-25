from query.selection import ScalarSelectable, DictSelectable


class Sensor(object):
    key = ScalarSelectable('sensors', 'key')
    name = ScalarSelectable('sensors', 'name')
    attributes = DictSelectable('sensors', 'attributes')

    def __init__(self, key, name='', attributes={}):
        self.key = key
        self.name = name
        self.attributes = attributes
