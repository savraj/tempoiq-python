import unittest
import json
import datetime
from tempoiq.session import get_session
from monkey import monkeypatch_requests


class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = get_session('http://test.tempo-iq.com/', 'foo', 'bar')
        monkeypatch_requests(self.client.endpoint)

    def test_client_properly_escapes_keys_in_range_delete(self):
        start = datetime.datetime.now()
        end = datetime.datetime.now()
        url = 'http://test.tempo-iq.com/v2/devices/de%20vice%2F1/sensors/se%20nsor%2F1/datapoints'
        data = {
            'start': start.isoformat(),
            'stop': end.isoformat()
        }
        j = json.dumps(data)
        self.client.delete_from_sensors("de vice/1", "se nsor/1", start, end)
        self.client.endpoint.pool.delete.assert_called_once_with(
            url, data=j, auth=self.client.endpoint.auth)
