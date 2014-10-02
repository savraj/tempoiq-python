import json
import urlparse
from protocol.encoder import WriteEncoder, CreateEncoder, ReadEncoder
from protocol.query.builder import QueryBuilder
from response import Response, SensorPointsResponse
from response import RuleResponse, DeviceResponse


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
    """Entry point for all TempoIQ API calls.

    :param endpoint: backend and credentials to connect to
    :type endpoint: tempoiq.endpoint.HTTPEndpoint
    """

    write_encoder = WriteEncoder()
    create_encoder = CreateEncoder()
    read_encoder = ReadEncoder()

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.monitoring_client = MonitoringClient(self.endpoint)

    def create_device(self, device):
        """Create a new device

        :param device:
        :type device: :class:`~tempoiq.protocol.device.Device`
        :rtype: :class:`tempoiq.response.Response` with a
                :class:`tempoiq.protocol.device.Device` data payload"""
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/')
        j = json.dumps(device, default=self.create_encoder.default)
        resp = self.endpoint.post(url, j)
        return Response(resp, self.endpoint)

    def delete_device(self, query):
        """Delete devices that match the provided query.

        :param query:
        :type query: :class:`tempoiq.protocol.query.builder.QueryBuilder`
        :rtype: :class:`tempoiq.response.Response`"""
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/')
        j = json.dumps(query, default=self.read_encoder.default)
        resp = self.endpoint.delete(url, j)
        return Response(resp, self.endpoint)

    def monitor(self, rule):
        url = urlparse.urljoin(self.endpoint.base_url, 'monitors/')
        rule_json = json.dumps(rule, default=self.write_encoder.default)
        resp = self.endpoint.post(url, rule_json)
        return RuleResponse(resp, self.endpoint)

    def query(self, object_type):
        """Begin to build a query on the given object type.

        :param object_type: Either :class:`~tempoiq.protocol.device.Device` or :class:`~tempoiq.protocol.sensor.Sensor`
        :rtype: :class:`~tempoiq.protocol.query.builder.QueryBuilder`"""
        return QueryBuilder(self, object_type)

    def read(self, query):
        url = urlparse.urljoin(self.endpoint.base_url, 'read/')
        j = json.dumps(query, default=self.read_encoder.default)
        print("READ REQUEST: ", j)
        resp = self.endpoint.get(url, j)
        return SensorPointsResponse(resp, self.endpoint)

    def search_devices(self, query, size=5000):
        #TODO - actually use the size param
        url = urlparse.urljoin(self.endpoint.base_url, 'devices/')
        j = json.dumps(query, default=self.read_encoder.default)
        return DeviceResponse(self.endpoint.get(url, j), self.endpoint)

    def write(self, write_request):
        """Write data points to one or more devices and sensors.

        The write_request argument is a dict which maps device keys to device data.
        The device data is itself a dict mapping sensor key to a list of
        :class:`tempoiq.protocol.point.Point`

        :param dict write_request:
        """
        url = urlparse.urljoin(self.endpoint.base_url, 'write/')
        default = self.write_encoder.default
        resp = self.endpoint.post(url, json.dumps(write_request,
                                                  default=default))
        return Response(resp, self.endpoint)
