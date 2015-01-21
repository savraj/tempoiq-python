import unittest
import json
import datetime
import mock
from tempoiq.protocol.encoder import WriteEncoder, CreateEncoder, ReadEncoder
from tempoiq.protocol import Sensor, Device, Point, Rule
from tempoiq.protocol.query.selection import *
from tempoiq.protocol.query.builder import QueryBuilder
from tempoiq.session import get_session
from monkey import monkeypatch_requests


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

    def setUp(self):
        self.client = get_session('http://test.tempo-iq.com/', 'foo', 'bar')
        monkeypatch_requests(self.client.endpoint)
        setattr(self.client, 'read', mock.Mock())
        setattr(self.client.monitoring_client, 'get_rule', mock.Mock())
        setattr(self.client, 'search_devices', mock.Mock())

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

    def test_encode_nested_compound_clause(self):
        and_clause = and_([Device.key == 'foo', Device.key == 'bar'])
        clause = or_([and_clause, and_clause])
        j = json.dumps(clause, default=self.read_encoder.default)
        expected = {
            'or': [
                {'and': [
                    {'key': 'foo'},
                    {'key': 'bar'}
                ]},
                {'and': [
                    {'key': 'foo'},
                    {'key': 'bar'}
                ]}
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
        qb = QueryBuilder(self.client, Sensor)
        start = datetime.datetime(2014, 1, 1)
        end = datetime.datetime(2014, 1, 2)
        qb.filter(Device.key == 'foo').filter(Sensor.key == 'bar')
        qb.read(start=start, end=end)
        j = json.dumps(qb, default=self.read_encoder.default)
        expected = {
            'search': {
                'select': 'sensors',
                'filters': {
                    'devices': {'key': 'foo'},
                    'sensors': {'key': 'bar'}
                }
            },
            'read': {
                'start': '2014-01-01T00:00:00',
                'stop': '2014-01-02T00:00:00'
            }
        }
        self.assertEquals(json.loads(j), expected)

    def test_encode_query_builder_with_pipeline(self):
        qb = QueryBuilder(self.client, Sensor)
        start = datetime.datetime(2014, 1, 1)
        end = datetime.datetime(2014, 1, 2)
        qb.filter(Device.key == 'foo').filter(Sensor.key == 'bar')
        qb.aggregate('max').convert_timezone('CDT')
        qb.rollup('min', '1min').multi_rollup(['max', 'min'], '1min')
        qb.interpolate('linear', '1min').find('max', '1min')
        qb.read(start=start, end=end)
        j = json.dumps(qb, default=self.read_encoder.default)
        expected = {
            'search': {
                'select': 'sensors',
                'filters': {
                    'devices': {'key': 'foo'},
                    'sensors': {'key': 'bar'}
                }
            },
            'fold': {
                'functions': [
                    {
                        'name': 'aggregation',
                        'arguments': ['max']
                    },
                    {
                        'name': 'convert_tz',
                        'arguments': ['CDT']
                    },
                    {
                        'name': 'rollup',
                        'arguments': [
                            'min',
                            '1min',
                            '2014-01-01T00:00:00'
                        ]
                    },
                    {
                        'name': 'multi_rollup',
                        'arguments': [
                            ['max', 'min'],
                            '1min',
                            '2014-01-01T00:00:00'
                        ]
                    },
                    {
                        'name': 'interpolate',
                        'arguments': [
                            'linear',
                            '1min',
                            '2014-01-01T00:00:00',
                            '2014-01-02T00:00:00'
                        ]
                    },
                    {
                        'name': 'find',
                        'arguments': [
                            'max',
                            '1min',
                            '2014-01-01T00:00:00'
                        ]
                    },
                ]
            },
            'read': {
                'start': '2014-01-01T00:00:00',
                'stop': '2014-01-02T00:00:00'
            }
        }
        self.assertEquals(json.loads(j), expected)

    def test_query_builder_with_optional_pipeline_args(self):
        qb = QueryBuilder(self.client, Sensor)
        start = datetime.datetime(2014, 1, 1, 12, 0)
        end = datetime.datetime(2014, 1, 3, 12, 0)
        rollup_start = datetime.datetime(2014, 1, 1, 0, 0)
        qb.filter(Device.key == 'foo').filter(Sensor.key == 'bar')
        qb.rollup('min', '1min', rollup_start)
        qb.read(start=start, end=end)
        j = json.dumps(qb, default=self.read_encoder.default)
        expected = {
            'search': {
                'select': 'sensors',
                'filters': {
                    'devices': {'key': 'foo'},
                    'sensors': {'key': 'bar'}
                }
            },
            'fold': {
                'functions': [
                    {
                        'name': 'rollup',
                        'arguments': [
                            'min',
                            '1min',
                            '2014-01-01T00:00:00'
                        ]
                    }
                ]
            },
            'read': {
                'start': '2014-01-01T12:00:00',
                'stop': '2014-01-03T12:00:00'
            }
        }
        self.assertEquals(json.loads(j), expected)

    def test_query_builder_with_ordering(self):
        qb = QueryBuilder(self.client, Sensor)
        start = datetime.datetime(2014, 1, 1, 12, 0)
        end = datetime.datetime(2014, 1, 3, 12, 0)
        rollup_start = datetime.datetime(2014, 1, 1, 0, 0)
        qb.filter(Device.key == 'foo')
        qb.order_by('date_created', 'asc')
        qb.read(start=start, end=end)
        j = json.dumps(qb, default=self.read_encoder.default)
        expected = {
            'search': {
                'select': 'sensors',
                'filters': {
                    'devices': {'key': 'foo'},
                    'sensors': 'all'
                },
                'ordering': {
                    'attribute': 'date_created',
                    'direction': 'asc'
                }
            },
            'read': {
                'start': '2014-01-01T12:00:00',
                'stop': '2014-01-03T12:00:00'
            }
        }
        self.assertEquals(json.loads(j), expected)

    def test_query_builder_sensor_only_search(self):
        qb = QueryBuilder(self.client, Device)
        qb.filter(Sensor.key == 'foo')
        qb.read()
        j = json.dumps(qb, default=self.read_encoder.default)
        expected = '{"search": {"filters": {"sensors": {"key": "foo"}, "devices": "all"}, "select": "devices"}, "find": {"quantifier": "all"}}'
        self.assertEquals(j, expected)

    def test_query_builder_to_monitoring_read(self):
        qb = QueryBuilder(self.client, Rule)
        qb.filter(Rule.key == 'foo').read()
        #self.client.endpoint.pool.get.assert_called_once_with(
        #    'http://test.tempo-iq.com/v2/monitors/foo',
        #    data='',
        #    auth=self.client.endpoint.auth)

    def test_query_builder_to_device_search(self):
        qb = QueryBuilder(self.client, Device)
        qb.filter(Device.key == 'foo').read()
        expected = '{"search": {"filters": {"sensors": {}, "devices": {"key": "foo"}}, "select": "devices"}, "find": {"quantifier": "all"}}'
        #self.client.endpoint.pool.get.assert_called_once_with(
        #    'http://test.tempo-iq.com/v2/devices/',
        #    data=expected,
        #    auth=self.client.endpoint.auth)
