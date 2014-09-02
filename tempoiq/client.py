import functools
import urlparse
import urllib
import json
import endpoint
import protocol
from protocol.encoder import WriteEncoder, CreateEncoder, ReadEncoder
from protocol.query.builder import QueryBuilder
from response import Response, SensorPointsResponse, ResponseException
from response import RuleResponse, DeviceResponse


def make_series_url(key):
    """For internal use. Given a series key, generate a valid URL to the series
    endpoint for that key.

    :param string key: the series key
    :rtype: string"""

    url = urlparse.urljoin(endpoint.SERIES_ENDPOINT, 'key/')
    url = urlparse.urljoin(url, urllib.quote_plus(key))
    return url


class with_response_type(object):
    """For internal use. Decorator for ensuring the Response object returned by
    the :class:`Client` object has a data attribute that corresponds to the
    object type expected from the TempoDB API.  This class should not be
    used by user code.

    The "t" argument should be a string corresponding to the name of a class
    from the :mod:`tempodb.protocol.objects` module, or a single element list
    with the element being the name of a class from that module if the API
    endpoint will return a list of those objects.

    :param t: the type of object to cast the TempoDB response to
    :type t: list or string"""

    def __init__(self, t):
        self.t = t

    def __call__(self, f, *args, **kwargs):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            resp = f(*args, **kwargs)
            #dont try this at home kids
            session = args[0].session
            resp_obj = Response(resp, session)
            if resp_obj.status == 200:
                resp_obj._cast_payload(self.t)
            else:
                raise ResponseException(resp_obj)
            return resp_obj
        return wrapper


class with_cursor(object):
    """For internal use. Decorator class for automatically transforming a
    response into a Cursor of the required type.

    :param class cursor_type: the cursor class to use
    :param class data_type: the data type that cursor should generate"""

    def __init__(self, cursor_type, data_type):
        self.cursor_type = cursor_type
        self.data_type = data_type

    def __call__(self, f, *args, **kwargs):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            resp = f(*args, **kwargs)
            session = args[0].session
            resp_obj = Response(resp, session)
            if resp_obj.status == 200:
                data = json.loads(resp_obj.body)
                if self.cursor_type in [protocol.SeriesCursor,
                                        protocol.SingleValueCursor]:
                    return self.cursor_type(data, self.data_type, resp_obj)
                else:
                    return self.cursor_type(data, self.data_type, resp_obj,
                                            kwargs.get('tz'))
            raise ResponseException(resp_obj)
        return wrapper


class MonitoringClient(object):
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def delete_rule(self, key):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/' + key)
        return self.endpoint.delete(url)

    def get_annotations(self, key):
        url = urlparse.urljoin(self.endpoint.base_url,
                               'monitors/annotations/' + key)
        return self.endpoint.get(url)

    def get_changelog(self, key):
        url = urlparse.urljoin(self.endpoint.base_url,
                               'monitors/changes/' + key)
        return self.endpoint.get(url)

    def get_logs(self, key):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/logs/' + key)
        return self.endpoint.get(url)

    def get_rule(self, key):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/' + key)
        return RuleResponse(self.endpoint.get(url), self)

    def get_usage(self, key):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/usage/' + key)
        return self.endpoint.get(url)

    def list_rules(self):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/')
        return RuleResponse(self.endpoint.get(url), self)


class Client(object):
    write_encoder = WriteEncoder()
    create_encoder = CreateEncoder()
    read_encoder = ReadEncoder()

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.monitoring_client = MonitoringClient(self.endpoint)

    def create_device(self, device):
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/')
        j = json.dumps(device, default=self.create_encoder.default)
        self.endpoint.post(url, j)

    def delete_device(self, query):
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/')
        j = json.dumps(query, default=self.create_encoder.default)
        self.endpoint.delete(url, j)

    def monitor(self, rule):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/')
        rule_json = json.dumps(rule, default=self.write_encoder.default)
        self.endpoint.post(url, rule_json)

    def query(self, object_type):
        return QueryBuilder(self, object_type)

    def read(self, query):
        url = urlparse.urljoin(self.endpoint.base_url, 'read/')
        j = json.dumps(query, default=self.read_encoder.default)
        resp = self.endpoint.get(url, j)
        return SensorPointsResponse(resp, self.endpoint)

    def search_devices(self, query, size=5000):
        #TODO - actually use the size param
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/')
        j = json.dumps(query, default=self.read_encoder.default)
        return DeviceResponse(self.endpoint.get(url, j), self.endpoint)

    def write(self, write_request):
        url = urlparse.urljoin(self.endpoint.base_url, 'write/')
        default = self.write_encoder.default
        return self.endpoint.post(url, json.dumps(write_request,
                                                  default=default))
