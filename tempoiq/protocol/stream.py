from query.selection import ScalarSelectable


class Stream(object):
    function = ScalarSelectable('streams', 'function')

    def __init__(self, device, sensor, function):
        self.device = device
        self.sensor = sensor
        self.function = function
