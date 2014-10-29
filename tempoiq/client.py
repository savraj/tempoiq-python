import json
import urlparse
from protocol.encoder import WriteEncoder, CreateEncoder, ReadEncoder
from protocol.query.builder import QueryBuilder
from response import Response, SensorPointsResponse, DeleteDatapointsResponse
from response import RuleResponse, DeviceResponse


def make_fetcher(endpoint, url):
    def fetcher(cursor):
        resp = endpoint.get(url, data=cursor)
        return json.loads(resp.body)


class MonitoringClient(object):
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def delete_rule(self, key):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/' + key)
        resp = self.endpoint.delete(url)
        return Response(resp, self.endpoint)

    def get_annotations(self, key):
        url = urlparse.urljoin(self.endpoint.base_url,
                               'monitors/annotations/' + key)
        resp = self.endpoint.get(url)
        return Response(resp, self.endpoint)

    def get_changelog(self, key):
        url = urlparse.urljoin(self.endpoint.base_url,
                               'monitors/%s/changes/' % key)
        resp = self.endpoint.get(url)
        return Response(resp, self.endpoint)

    def get_logs(self, key):
        url = urlparse.urljoin(self.endpoint.base_url,
                               'monitors/%s/logs/' % key)
        resp = self.endpoint.get(url)
        return Response(resp, self.endpoint)

    def get_rule(self, key):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/' + key)
        return RuleResponse(self.endpoint.get(url), self)

    def get_usage(self, key):
        url = urlparse.urljoin(self.endpoint.base_url,
                               'monitors/%s/usage/' % key)
        resp = self.endpoint.get(url)
        return Response(resp, self.endpoint)

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
        return self.endpoint.post(url, j)

    def delete_device(self, query):
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/')
        j = json.dumps(query, default=self.read_encoder.default)
        resp = self.endpoint.delete(url, j)
        return Response(resp, self.endpoint)

    def delete_from_sensors(self, device_key, sensor_key, start, end):
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/',
                               device_key + '/', 'sensors/',
                               sensor_key + '/')
        j = json.dumps({'start': start, 'end': end})
        resp = self.endpoint.delete(url, j)
        return DeleteDatapointsResponse(resp, self.endpoint)

    def monitor(self, rule):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/')
        rule_json = json.dumps(rule, default=self.write_encoder.default)
        resp = self.endpoint.post(url, rule_json)
        return RuleResponse(resp, self.endpoint)

    def query(self, object_type):
        return QueryBuilder(self, object_type)

    def read(self, query):
        url = urlparse.urljoin(self.endpoint.base_url, 'read/')
        j = json.dumps(query, default=self.read_encoder.default)
        resp = self.endpoint.get(url, j)
        fetcher = make_fetcher(self.endpoint, url)
        return SensorPointsResponse(resp, self.endpoint, fetcher)

    def search_devices(self, query, size=5000):
        #TODO - actually use the size param
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/')
        j = json.dumps(query, default=self.read_encoder.default)
        return DeviceResponse(self.endpoint.get(url, j), self.endpoint)

    def single_value(self, query):
        url = urlparse.urljoin(self.endpoint.base_url, 'single/')
        j = json.dumps(query, default=self.read_encoder.default)
        return SensorPointsResponse(self.endpoint.get(url, j), self.endpoint)

    def write(self, write_request):
        url = urlparse.urljoin(self.endpoint.base_url, 'write/')
        default = self.write_encoder.default
        resp = self.endpoint.post(url, json.dumps(write_request,
                                                  default=default))
        return Response(resp, self.endpoint)
