import unittest
import json
import datetime
from tempoiq.protocol.encoder import WriteEncoder, CreateEncoder, ReadEncoder
from tempoiq.protocol import Sensor, Device, Point
from tempoiq.protocol.query.selection import *
from tempoiq.protocol.query.builder import QueryBuilder


class TestWriteEncoder(unittest.TestCase):
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


class TestCreateEncoder(unittest.TestCase):
    create_encoder = CreateEncoder()

    def test_create_encode_sensor(self):
        sensor = Sensor('sensor-1', name='foo', attributes={'foo': 'bar'})
        j = json.dumps(sensor, default=self.create_encoder.default)
        expected = {
            'key': 'sensor-1',
            'name': 'foo',
            'attributes': {'foo': 'bar'}
        }
        self.assertEquals(j, json.dumps(expected))

    def test_create_encode_device(self):
        sensor = Sensor('sensor-1', name='foo', attributes={'foo': 'bar'})
        device = Device('device-1', name='foo', attributes={'foo': 'bar'},
                        sensors=[sensor])
        j = json.dumps(device, default=self.create_encoder.default)
        expected = {
            'key': 'device-1',
            'name': 'foo',
            'attributes': {'foo': 'bar'},
            'sensors': [{
                'key': 'sensor-1',
                'name': 'foo',
                'attributes': {'foo': 'bar'}
            }]
        }
        self.assertEquals(j, json.dumps(expected))


class TestReadEncoder(unittest.TestCase):
    read_encoder = ReadEncoder()

    def test_encode_scalar_selector(self):
        selector = Device.key == 'foo'
        j = json.dumps(selector, default=self.read_encoder.default)
        expected = {'key': 'foo'}
        self.assertEquals(j, json.dumps(expected))

    def test_encode_and_clause(self):
        clause = and_([Device.key == 'foo', Device.key == 'bar'])
        j = json.dumps(clause, default=self.read_encoder.default)
        expected = {
            'and': [
                {'key': 'foo'},
                {'key': 'bar'}
            ]
        }
        self.assertEquals(j, json.dumps(expected))

    def test_encode_or_clause(self):
        clause = or_([Device.key == 'foo', Device.key == 'bar'])
        j = json.dumps(clause, default=self.read_encoder.default)
        expected = {
            'or': [
                {'key': 'foo'},
                {'key': 'bar'}
            ]
        }
        self.assertEquals(j, json.dumps(expected))

    def test_encode_selection(self):
        clause = or_([Device.key == 'foo', Device.key == 'bar'])
        selection = Selection()
        selection.add(clause)
        j = json.dumps(selection, default=self.read_encoder.default)
        expected = {
            'or': [
                {'key': 'foo'},
                {'key': 'bar'}
            ]
        }
        self.assertEquals(j, json.dumps(expected))

    def test_encode_empty_selection(self):
        selection = Selection()
        j = json.dumps(selection, default=self.read_encoder.default)
        expected = {}
        self.assertEquals(j, json.dumps(expected))

    def test_encode_query_builder(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.key == 'foo').filter(Sensor.key == 'bar')
        j = json.dumps(qb, default=self.read_encoder.default)
        expected = {
            'search': {
                'select': 'sensors',
                'filters': {
                    'devices': {
                        'and': [
                            {'key': 'foo'}
                        ]
                    },
                    'sensors': {
                        'and': [
                            {'key': 'bar'}
                        ]
                    }
                }
            }
        }
        self.assertEquals(j, json.dumps(expected))
