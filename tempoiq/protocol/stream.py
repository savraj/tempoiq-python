from query.selection import ScalarSelectable


#used for enabling the search DSL for substream selection, not up for release
#yet
class Stream(object):
    function = ScalarSelectable('streams', 'function')

    def __init__(self, device, sensor, function):
        self.device = device
        self.sensor = sensor
        self.function = function
