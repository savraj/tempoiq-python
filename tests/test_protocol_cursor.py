import mock


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
