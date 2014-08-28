class Row(object):
    def __init__(self, row_json):
        self.timestamp = row_json['t']
        self.values = row_json['data']

    def __getitem__(self, key):
        return self.values[key]

    def __iter__(self):
        for device in self.values:
            for sensor in device:
                yield ((device, sensor), device[sensor])
