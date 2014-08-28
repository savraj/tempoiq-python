from tempoiq.temporal.validate import convert_iso_stamp


class Row(object):
    def __init__(self, row_json):
        self.timestamp = convert_iso_stamp(row_json['t'])
        self.values = row_json['data']

    def __getitem__(self, key):
        return self.values[key]

    def __iter__(self):
        for device in self.values:
            for sensor in self.values[device]:
                yield ((device, sensor), self.values[device][sensor])
