import unittest
from tempodb.session import get_session
from monkey import monkeypatch_requests


class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = get_session('http://test.tempo-iq.com/', 'foo', 'bar')
        monkeypatch_requests(self.client.endpoint)
