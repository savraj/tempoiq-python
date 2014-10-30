from tempoiq.temporal.validate import convert_iso_stamp


class Row(object):
    """Data from one or more sensors at a single timestamp. Returned when reading
    sensor data.

    Example values dict of a row with a single sensor, *temperature*\ , on a
    single device, *test1*\ ::

        {'test1': {'temperature': 500.0} }

    :var timestamp: DateTime of the sensor data
    :var values: dict mapping device key to a dict of sensor keys to values
    """
    def __init__(self, row_json):
        self.timestamp = convert_iso_stamp(row_json['t'])
        self.values = row_json['data']

    def __getitem__(self, key):
        return self.values[key]

    def __iter__(self):
        for device in self.values:
            for sensor in self.values[device]:
                yield ((device, sensor), self.values[device][sensor])
