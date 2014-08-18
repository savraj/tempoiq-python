import json


class TempoIQEncoder(json.JSONEncoder):
    def encode_point(self, point):
        return self.default({'t': point.timestamp, 'v': point.value})


class WriteEncoder(TempoIQEncoder):
    encoders = {
        'device': 'encode_device',
        'sensor': 'encode_sensor',
        'point': 'encode_point'
    }

    def default(self, o):
        encoder_name = self.encoders.get(o.__class__.__name__)
        if encoder_name is None:
            super(self, json.JSONEncoder).default()
        encoder = getattr(self, encoder_name)
        return encoder(o)

    def encode_device(self, device):
        return device.key

    def encode_sensor(self, sensor):
        return sensor.key
