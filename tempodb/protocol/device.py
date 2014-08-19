class Device(object):
    def __init__(self, key, name='', attributes={}, sensors=[]):
        self.key = key
        self.name = name
        self.attributes = attributes
        self.sensors = sensors
