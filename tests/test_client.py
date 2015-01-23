import unittest
import json
import datetime
from tempoiq.session import get_session
from tempoiq.protocol.device import Device
from tempoiq.client import *
from tempoiq.endpoint import merge_headers, media_type
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
            url, data=j, headers=self.client.endpoint.headers,
            auth=self.client.endpoint.auth)

    def test_client_properly_escapes_keys_in_device_update(self):
        device = Device('de vice/1')
        url = 'http://test.tempo-iq.com/v2/devices/de%20vice%2F1'
        j = json.dumps(device, default=self.client.create_encoder.default)
        self.client.update_device(device)
        self.client.endpoint.pool.put.assert_called_once_with(
            url, data=j, headers=self.client.endpoint.headers,
            auth=self.client.endpoint.auth)

    def test_client_sends_media_type_in_read_query(self):
        query = {}
        j = json.dumps(query, default=self.client.create_encoder.default)
        url = 'http://test.tempo-iq.com/v2/read/'
        DATAPOINT_ACCEPT_TYPE = media_type('datapoint-collection', 'v2')
        accept_headers = [self.client.ERROR_ACCEPT_TYPE,
                          self.client.DATAPOINT_ACCEPT_TYPE]
        content_header = self.client.QUERY_CONTENT_TYPE
        headers = media_types(accept_headers, content_header)
        merged = merge_headers(self.client.endpoint.headers, headers)
        resp = self.client.read(query)
        self.assertIsInstance(resp, SensorPointsResponse)
        self.client.endpoint.pool.get.assert_called_once_with(
            url, data=j, auth=self.client.endpoint.auth,
            headers=merged)

    def test_client_sends_non_default_media_type_in_read_query(self):
        query = {}
        j = json.dumps(query, default=self.client.create_encoder.default)
        url = 'http://test.tempo-iq.com/v2/read/'
        self.client = get_session('http://test.tempo-iq.com/', 'foo', 'bar',
                                  read_version='v3')
        monkeypatch_requests(self.client.endpoint)
        DATAPOINT_ACCEPT_TYPE = media_type('datapoint-collection', 'v3')
        accept_headers = [self.client.ERROR_ACCEPT_TYPE,
                          self.client.DATAPOINT_ACCEPT_TYPE]
        content_header = self.client.QUERY_CONTENT_TYPE
        headers = media_types(accept_headers, content_header)
        merged = merge_headers(self.client.endpoint.headers, headers)
        resp = self.client.read(query)
        self.assertIsInstance(resp, StreamResponse)
        self.client.endpoint.pool.get.assert_called_once_with(
            url, data=j, auth=self.client.endpoint.auth,
            headers=merged)

    def test_client_sends_media_type_in_device_query(self):
        query = {}
        j = json.dumps(query, default=self.client.create_encoder.default)
        url = 'http://test.tempo-iq.com/v2/devices/'
        accept_headers = [self.client.ERROR_ACCEPT_TYPE,
                          self.client.DEVICE_ACCEPT_TYPE]
        content_header = self.client.QUERY_CONTENT_TYPE
        headers = media_types(accept_headers, content_header)
        merged = merge_headers(self.client.endpoint.headers, headers)
        self.client.search_devices(query)
        self.client.endpoint.pool.get.assert_called_once_with(
            url, data=j, auth=self.client.endpoint.auth,
            headers=merged)
