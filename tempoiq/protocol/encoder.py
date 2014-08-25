import json


class TempoIQEncoder(json.JSONEncoder):
    def encode_point(self, point):
        return {
            't': self.encode_datetime(point.timestamp),
            'v': point.value}

    def encode_datetime(self, dt):
        return dt.isoformat()


class WriteEncoder(TempoIQEncoder):
    encoders = {
        'Device': 'encode_device',
        'Sensor': 'encode_sensor',
        'Point': 'encode_point',
        'datetime': 'encode_datetime'
    }

    def default(self, o):
        encoder_name = self.encoders.get(o.__class__.__name__)
        if encoder_name is None:
            super(TempoIQEncoder, self).default(o)
        encoder = getattr(self, encoder_name)
        return encoder(o)

    def encode_device(self, device):
        return device.key

    def encode_sensor(self, sensor):
        return sensor.key


class CreateEncoder(TempoIQEncoder):
    encoders = {
        'Device': 'encode_device',
        'Sensor': 'encode_sensor'
    }

    def default(self, o):
        encoder_name = self.encoders.get(o.__class__.__name__)
        if encoder_name is None:
            super(TempoIQEncoder, self).default(o)
        encoder = getattr(self, encoder_name)
        return encoder(o)

    def encode_device(self, device):
        return {
            'key': device.key,
            'name': device.name,
            'attributes': device.attributes,
            'sensors': map(self.encode_sensor, device.sensors)
        }

    def encode_sensor(self, sensor):
        return {
            'key': sensor.key,
            'name': sensor.name,
            'attributes': sensor.attributes
        }
