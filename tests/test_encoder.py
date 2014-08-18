import unittest
import json
import datetime
from tempodb.protocol.encoder import WriteEncoder
from tempodb.protocol import Sensor, Device, Point


class TestEncoders(unittest.TestCase):
    write_encoder = WriteEncoder()

    def test_write_encode_device(self):
        device = Device('device-1', attributes={'foo': 'bar'})
        j = json.dumps(device, default=self.write_encoder.default)
        self.assertEquals(j, '"device-1"')

    def test_write_encode_sensor(self):
        sensor = Sensor('sensor-1', attributes={'foo': 'bar'})
        j = json.dumps(sensor, default=self.write_encoder.default)
        self.assertEquals(j, '"sensor-1"')

    def test_write_encode_point(self):
        point = Point(datetime.datetime(2014, 1, 1), 1.0)
        j = json.dumps(point, default=self.write_encoder.default)
        expected = '{"t": "2014-01-01T00:00:00", "v": 1.0}'
        self.assertEquals(j, expected)

    def test_encode_write_request(self):
        request = {
            'device-1': {
                'sensor-1': [
                    Point(datetime.datetime(2014, 1, 1), 1.0),
                    Point(datetime.datetime(2014, 1, 2), 2.0)
                ]
            }
        }

        j = json.dumps(request, default=self.write_encoder.default)
        expected = {
            'device-1': {
                'sensor-1': [
                    {'t': '2014-01-01T00:00:00', 'v': 1.0},
                    {'t': '2014-01-02T00:00:00', 'v': 2.0}
                ]
            }
        }
        self.assertEquals(j, json.dumps(expected))
