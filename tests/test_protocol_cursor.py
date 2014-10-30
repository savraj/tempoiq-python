import mock
import unittest
from tempoiq.protocol.cursor import DataPointsCursor, DeviceCursor


class DummyType(object):
    def __init__(self, data, response, tz=None):
        self.data = data
        self.response = response
        self.tz = tz


class Dummy(object):
    pass


class DummyResponse(object):
    def __init__(self):
        self.resp = Dummy()
        self.resp.headers = {}
        self.headers = {}
        self.text = ''
        self.session = mock.Mock()
        self.status_code = 200
        self.reason = 'OK'
        self.links = {}


class TestProtocolCursor(unittest.TestCase):
    def test_datapoints_cursor_iteration_with_no_fetch(self):
        first_data = {
            'data': [
                {'t': '2014-01-01T00:00:00',
                 'data': {
                     'test1': {'temp': 1.0}
                 }
                 }
            ]
        }

        def fetcher(cursor):
            return {
                'data': [
                    {'t': '2014-01-02T00:00:00',
                     'data': {
                         'test1': {'temp': 2.0}
                     }
                     }
                ]
            }
        resp = DummyResponse()
        resp.data = first_data
        c = DataPointsCursor(resp, first_data, fetcher)
        results = [d for d in c]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['test1']['temp'], 1.0)

    def test_datapoints_cursor_iteration_with_extra_fetch(self):
        first_data = {
            'cursor': {'next_query': None},
            'data': [
                {'t': '2014-01-01T00:00:00',
                 'data': {
                     'test1': {'temp': 1.0}
                 }
                 }
            ]
        }

        def fetcher(cursor):
            return {
                'data': [
                    {'t': '2014-01-02T00:00:00',
                     'data': {
                         'test1': {'temp': 2.0}
                     }
                     }
                ]
            }
        resp = DummyResponse()
        resp.data = first_data
        c = DataPointsCursor(resp, first_data, fetcher)
        results = [d for d in c]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0]['test1']['temp'], 1.0)
        self.assertEquals(results[1]['test1']['temp'], 2.0)

    def test_device_cursor_iteration_with_no_fetch(self):
        first_data = {
            'data': [
                {'key': 'device1', 'attributes': {}, 'sensors': []}
            ]
        }

        def fetcher(cursor):
            return {
                'data': [
                    {'key': 'device2', 'attributes': {}, 'sensors': []}
                ]
            }
        resp = DummyResponse()
        resp.data = first_data
        c = DeviceCursor(resp, first_data, fetcher)
        results = [d for d in c]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0].key, 'device1')

    def test_device_cursor_iteration_with_extra_fetch(self):
        first_data = {
            'cursor': {'next_query': None},
            'data': [
                {'key': 'device1', 'attributes': {}, 'sensors': []}
            ]
        }

        def fetcher(cursor):
            return {
                'data': [
                    {'key': 'device2', 'attributes': {},
                     'sensors': [{'key': 'sensor1', 'attributes': {}}]
                    }
                ]
            }
        resp = DummyResponse()
        resp.data = first_data
        c = DeviceCursor(resp, first_data, fetcher)
        results = [d for d in c]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0].key, 'device1')
        self.assertEquals(results[1].key, 'device2')
        self.assertEquals(results[1].sensors[0].key, 'sensor1')
