import unittest
import pytz
import datetime
from tempoiq.protocol.row import Row, SelectionEvaluator, StreamInfo
from tempoiq.protocol.row import PointStream
from tempoiq.protocol.row import NoResultError, TooManyResultsError
from tempoiq.protocol.query.selection import Selection, or_, and_
from tempoiq.protocol.device import Device
from tempoiq.protocol.sensor import Sensor
from tempoiq.protocol.stream import Stream


class TestRow(unittest.TestCase):
    def test_row_timestamp_parsing_with_utc(self):
        data = {
            't': '2014-01-01T00:00:00Z',
            'data': {
                'device-1': {
                    'sensor-1': 1.0,
                    'sensor-2': 2.0
                },
                'device-2': {
                    'sensor-2': 3.0
                }
            }
        }

        row = Row(data)
        self.assertEquals(row.timestamp, datetime.datetime(2014, 1, 1,
                                                           tzinfo=pytz.utc))

    def test_row_timestamp_parsing_with_timezone(self):
        data = {
            't': '2014-01-01T00:00:00-05:00',
            'data': {
                'device-1': {
                    'sensor-1': 1.0,
                    'sensor-2': 2.0
                },
                'device-2': {
                    'sensor-2': 3.0
                }
            }
        }

        row = Row(data)
        tz = pytz.timezone("US/Eastern")
        self.assertEquals(row.timestamp, datetime.datetime(2014, 1, 1,
                                                           tzinfo=tz))

    def test_row_dict_like_access(self):
        data = {
            't': '2014-01-01T00:00:00Z',
            'data': {
                'device-1': {
                    'sensor-1': 1.0,
                    'sensor-2': 2.0
                },
                'device-2': {
                    'sensor-2': 3.0
                }
            }
        }

        row = Row(data)
        value1 = row['device-1']['sensor-1']
        value2 = row['device-1']['sensor-2']
        value3 = row['device-2']['sensor-2']
        self.assertEquals(value1, 1.0)
        self.assertEquals(value2, 2.0)
        self.assertEquals(value3, 3.0)

    def test_row_iterator(self):
        data = {
            't': '2014-01-01T00:00:00Z',
            'data': {
                'device-1': {
                    'sensor-1': 1.0,
                    'sensor-2': 2.0
                },
                'device-2': {
                    'sensor-2': 3.0
                }
            }
        }

        row = Row(data)
        values = [r for r in row]
        expected = [
            (('device-1', 'sensor-1'), 1.0),
            (('device-1', 'sensor-2'), 2.0),
            (('device-2', 'sensor-2'), 3.0)
        ]
        self.assertEquals(values, expected)


class TestSelectionEvaluator(unittest.TestCase):
    def test_selection_evaluator_with_device_key_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Device.key == 'foo')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['id'], 0)

    def test_selection_evaluator_with_sensor_key_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Sensor.key == 'foo')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0]['id'], 0)
        self.assertEquals(results[1]['id'], 1)

    def test_selection_evaluator_with_device_name_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Device.name == 'bar')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['id'], 0)

    def test_selection_evaluator_with_sensor_name_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'baz',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Sensor.name == 'baz')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['id'], 1)

    def test_selection_evaluator_with_device_attribute_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Device.attributes['foo'] == 'bar')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['id'], 0)

    def test_selection_evaluator_with_sensor_attribute_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Sensor.attributes['foo'] == 'bar')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0]['id'], 0)
        self.assertEquals(results[1]['id'], 1)

    def test_selection_evaluator_with_device_compound_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Device.key == 'foo')
        selection.add(Device.attributes['foo'] == 'bar')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['id'], 0)

    def test_selection_evaluator_with_sensor_compound_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Sensor.key == 'foo')
        selection.add(Sensor.attributes['foo'] == 'bar')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0]['id'], 0)
        self.assertEquals(results[1]['id'], 1)

    def test_selection_evaluator_with_hybrid_compound_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        clause = or_([Device.key == 'foo', Sensor.attributes['baz'] == 'boz'])
        selection.add(clause)

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0]['id'], 0)
        self.assertEquals(results[1]['id'], 1)

    def test_selection_evaluator_with_nested_compound_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        clause1 = and_([Device.key == 'foo',
                        Sensor.attributes['baz'] == 'boz'])
        clause2 = and_([Device.key == 'bar',
                        Sensor.attributes['foo'] == 'bar'])
        main_clause = or_([clause1, clause2])
        selection.add(main_clause)

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0]['id'], 0)
        self.assertEquals(results[1]['id'], 1)

    def test_selection_evaluator_with_function_name_selector(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'function': 'max',
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'baz',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'id': 1}
        ]

        selection = Selection()
        selection.add(Stream.function == 'max')

        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(headers)]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['id'], 0)

class TestStreamInfo(unittest.TestCase):
    def test_stream_info_get_one_returns_one(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'foo': 'bar'}},
             'id': 1}
        ]

        streams = StreamInfo(headers)
        result = streams.get_one(device_key='foo')
        self.assertEquals(result['id'], 0)

    def test_stream_info_get_one_returns_none_raises_error(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'foo': 'bar'}},
             'id': 1}
        ]

        streams = StreamInfo(headers)
        with self.assertRaises(NoResultError):
            streams.get_one(device_key='blarg')

    def test_stream_info_get_one_returns_two_raises_error(self):
        headers = [
            {'device': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz', 'foo': 'bar'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'baz': 'boz'}},
             'id': 0},
            {'device': {'key': 'bar', 'name': 'foo',
                        'attributes': {'baz': 'boz'}},
             'sensor': {'key': 'foo', 'name': 'bar',
                        'attributes': {'foo': 'bar'}},
             'id': 1}
        ]

        streams = StreamInfo(headers)
        with self.assertRaises(TooManyResultsError):
            streams.get_one(sensor_name='bar')


class TestPointStream(unittest.TestCase):
    def test_get_device(self):
        info = {'device': {'key': 'foo', 'name': 'bar',
                           'attributes': {'baz': 'boz', 'foo': 'bar'}},
                'sensor': {'key': 'foo', 'name': 'bar',
                           'attributes': {'baz': 'boz'}},
                'id': 0}
        stream = PointStream(info, None)
        self.assertEquals(stream.device.key, 'foo')
        self.assertEquals(stream.device.name, 'bar')
        self.assertEquals(stream.device.attributes,
                          {'baz': 'boz', 'foo': 'bar'})

    def test_get_sensor(self):
        info = {'device': {'key': 'foo', 'name': 'bar',
                           'attributes': {'baz': 'boz', 'foo': 'bar'}},
                'sensor': {'key': 'foo', 'name': 'bar',
                           'attributes': {'baz': 'boz'}},
                'id': 0}
        stream = PointStream(info, None)
        self.assertEquals(stream.sensor.key, 'foo')
        self.assertEquals(stream.sensor.name, 'bar')
        self.assertEquals(stream.sensor.attributes,
                          {'baz': 'boz'})
