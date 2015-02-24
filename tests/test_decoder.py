import unittest
import json
import copy
from tempoiq.protocol.query.selection import AndClause
from tempoiq.protocol.decoder import *
from tempoiq.protocol.rule import RuleStatus


class TestTempoIQDecoder(unittest.TestCase):
    def test_decode_attributes_selector(self):
        j = {"attributes": {"foo": "bar"}}
        selector = decode_attributes_selector(j)
        self.assertEquals(selector.selection_type, 'devices')
        self.assertEquals(selector.key, 'attributes')
        self.assertEquals(selector.value, {'foo': 'bar'})

    def test_decode_key_selector(self):
        j = {"key": "foo"}
        selector = decode_key_selector(j)
        self.assertEquals(selector.selection_type, 'devices')
        self.assertEquals(selector.key, 'key')
        self.assertEquals(selector.value, 'foo')

    def test_decode_scalar_selector_attributes(self):
        j = {"attributes": {"foo": "bar"}}
        selector = decode_attributes_selector(j)
        self.assertEquals(selector.selection_type, 'devices')
        self.assertEquals(selector.key, 'attributes')
        self.assertEquals(selector.value, {'foo': 'bar'})

    def test_decode_scalar_selector_key(self):
        j = {"key": "foo"}
        selector = decode_scalar_selector(j)
        self.assertEquals(selector.selection_type, 'devices')
        self.assertEquals(selector.key, 'key')
        self.assertEquals(selector.value, 'foo')

    def test_decode_basic_compound_clause(self):
        j = [
            {"key": "foo"},
            {"attributes": {"bar": "baz"}}
        ]
        selector = decode_compound_clause(j, t='and')
        self.assertTrue(isinstance(selector, AndClause))
        self.assertEquals(selector.selectors[0].selection_type, 'devices')
        self.assertEquals(selector.selectors[0].key, 'key')
        self.assertEquals(selector.selectors[1].selection_type, 'devices')
        self.assertEquals(selector.selectors[1].key, 'attributes')

    def test_decode_nested_compound_clause(self):
        j = [
            {"key": "foo"},
            {"and": [
                {"key": "foo"},
                {"attributes": {"bar": "baz"}}
            ]}
        ]
        selector = decode_compound_clause(j, t='or')
        self.assertEquals(selector.__class__.__name__, 'OrClause')
        self.assertEquals(selector.selectors[0].selection_type, 'devices')
        self.assertEquals(selector.selectors[0].key, 'key')
        self.assertEquals(selector.selectors[1].__class__.__name__,
                          'AndClause')
        self.assertEquals(selector.selectors[1].selectors[0].selection_type,
                          'devices')
        self.assertEquals(selector.selectors[1].selectors[0].key, 'key')

    def test_decode_basic_selection(self):
        j = {"key": "foo"}
        selection = decode_selection(j)
        self.assertEquals(selection.selection.selection_type, 'devices')
        self.assertEquals(selection.selection.key, 'key')
        self.assertEquals(selection.selection.value, 'foo')

    def test_decode_compound_selection(self):
        j = {
            "and": [
                {"key": "foo"},
                {"key": "bar"}
            ]
        }
        selection = decode_selection(j)
        self.assertTrue(isinstance(selection.selection, AndClause))
        self.assertEquals(selection.selection.selectors[0].selection_type,
                          'devices')
        self.assertEquals(selection.selection.selectors[0].key, 'key')
        self.assertEquals(selection.selection.selectors[0].value, 'foo')
        self.assertEquals(selection.selection.selectors[1].selection_type,
                          'devices')
        self.assertEquals(selection.selection.selectors[1].key, 'key')
        self.assertEquals(selection.selection.selectors[1].value, 'bar')

    def test_decoder_leaves_unrecognized_objects_alone(self):
        j = """{"foo": "bar"}"""
        decoder = TempoIQDecoder()
        decoded = json.loads(j, object_hook=decoder)
        self.assertEquals(decoded, {'foo': 'bar'})

    def test_decoder_for_rule(self):
        search = {
            'select': 'sensors',
            'filters': {
                'devices': {'key': 'foo'},
                'sensors': {'key': 'bar'}
            }
        }
        rule = {
            'name': 'one rule to rule them all',
            'key': 'foo',
            'actions': [{
                'url': 'http://www.foo.bar'
            }],
            'conditions': [
                {
                    'trigger': {
                        'name': 'exp_moving_average',
                        'arguments': [
                            'static',
                            'lt',
                            '300',
                            '5'
                        ]
                    },
                    'filter': {
                        'and': [
                            {
                                'operation': 'select',
                                'type': 'device_key',
                                'arguments': ['foo']
                            }
                        ]
                    }
                }
            ],
            'status': 'logonly'
        }
        j = {
            'rule': rule,
            'search': search,
            'alerts': 'any'
        }
        decoder = TempoIQDecoder()
        decoded = json.loads(json.dumps(j), object_hook=decoder)
        self.assertEquals(decoded.alert_by, 'any')
        self.assertEquals(decoded.key, 'foo')
        self.assertEquals(len(decoded.conditions), 1)
        self.assertEquals(decoded.status, RuleStatus.LOGONLY)
        self.assertTrue(isinstance(decoded.action, Webhook))

    def test_decoder_for_list_of_rules(self):
        search = {
            'select': 'sensors',
            'filters': {
                'devices': {'key': 'foo'},
                'sensors': {'key': 'bar'}
            }
        }
        rule = {
            'name': 'one rule to rule them all',
            'key': 'foo',
            'actions': [{
                'url': 'http://www.foo.bar'
            }],
            'conditions': [
                {
                    'trigger': {
                        'name': 'exp_moving_average',
                        'arguments': [
                            'static',
                            'lt',
                            '300',
                            '5'
                        ]
                    },
                    'filter': {
                        'and': [
                            {
                                'operation': 'select',
                                'type': 'device_key',
                                'arguments': ['foo']
                            }
                        ]
                    }
                }
            ]
        }
        j = {
            'rule': rule,
            'search': search,
            'alerts': 'any'
        }
        decoder = TempoIQDecoder()
        decoded = json.loads(json.dumps([j]), object_hook=decoder)
        self.assertEquals(decoded[0].alert_by, 'any')
        self.assertEquals(decoded[0].key, 'foo')
        self.assertEquals(len(decoded[0].conditions), 1)
        self.assertTrue(isinstance(decoded[0].action, Webhook))

    def test_decoder_for_list_of_rule_kv_pairs(self):
        search = {
            'select': 'sensors',
            'filters': {
                'devices': {'key': 'foo'},
                'sensors': {'key': 'bar'}
            }
        }
        rule = {
            'name': 'one rule to rule them all',
            'key': 'add',
            'actions': [{
                'url': 'http://www.foo.bar'
            }],
            'conditions': [
                {
                    'trigger': {
                        'name': 'exp_moving_average',
                        'arguments': [
                            'static',
                            'lt',
                            '300',
                            '5'
                        ]
                    },
                    'filter': {
                        'and': [
                            {
                                'operation': 'select',
                                'type': 'device_key',
                                'arguments': ['foo']
                            }
                        ]
                    }
                }
            ]
        }
        k1 = copy.deepcopy(rule)
        k1['key'] = 'key1'
        j1 = {
            'rule': k1,
            'search': search,
            'alerts': 'any'
        }

        k2 = copy.deepcopy(rule)
        k2['key'] = 'key2'
        j2 = {
            'rule': k2,
            'search': search,
            'alerts': 'any'
        }

        k3 = copy.deepcopy(rule)
        k3['key'] = 'key3'
        j3 = {
            'rule': k3,
            'search': search,
            'alerts': 'any'
        }

        j = {'data': [j1, j2, j3]}

        decoder = TempoIQDecoder()
        decoder.decoder = decoder.decode_rule_list
        decoded = json.loads(json.dumps(j), object_hook=decoder)
        self.assertEquals(len(decoded), 3)
        for rule in decoded:
            self.assertTrue(isinstance(rule, Rule))

    def test_decoder_for_rule_usage(self):
        j = {
            "data": [
                {
                    "timestamp": "2015-01-26T00:00:00.000Z",
                    "metricType": "partitions",
                    "count": 0
                },
                {
                    "timestamp": "2015-01-26T00:00:00.000Z",
                    "metricType": "datapoints",
                    "count": 27
                },
                {
                    "timestamp": "2015-01-26T00:00:00.000Z",
                    "metricType": "actions_triggered",
                    "count": 9
                },
                {
                    "timestamp": "2015-01-27T00:00:00.000Z",
                    "metricType": "partitions",
                    "count": 32
                }
            ]
        }
        decoder = TempoIQDecoder()
        decoder.decoder = decoder.decode_rule_usage
        decoded = json.loads(json.dumps(j), object_hook=decoder)
        self.assertEquals(len(decoded), 2)
        self.assertTrue(isinstance(decoded[0], RuleUsage))
        self.assertEquals(decoded[0].datapoints, 27)
        self.assertEquals(decoded[1].partitions, 32)

    def test_decoder_for_action_log(self):
        j = """{
                    "payload": "test payload",
                    "recipient": "me",
                    "response": "all good",
                    "status": "200",
                    "action_type": "webhook"
                }"""

        decoder = TempoIQDecoder()
        decoder.decoder = decoder.decode_action_log
        decoded = json.loads(j, object_hook=decoder)
        self.assertEquals(decoded.payload, 'test payload')
        self.assertEquals(decoded.recipient, 'me')
        self.assertEquals(decoded.response, 'all good')
        self.assertEquals(decoded.status, '200')
        self.assertEquals(decoded.action_type, 'webhook')

    def test_decoder_for_instigator(self):
        j = """{
                "datapoint": {
                    "t": "2015-01-01T00:00:00.000Z",
                    "v": 1
                },
                "device": {
                    "key": "key-1",
                    "name": "",
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "sensor": {
                    "key": "temp",
                    "name": "",
                    "attributes": {}
                }
            }"""
        decoder = TempoIQDecoder()
        decoder.decoder = decoder.decode_instigator
        decoded = json.loads(j, object_hook=decoder)
        self.assertEquals(decoded.device.key, 'key-1')
        self.assertEquals(decoded.sensor.key, 'temp')
        self.assertEquals(decoded.point.value, 1)

    def test_decoder_for_alert(self):
        j = """{
    "alert_id": 1,
    "rule_key": "key-1",
    "transitions": [
        {
            "timestamp": "2015-01-01T00:00:00.000Z",
            "instigator": {
                "datapoint": {
                    "t": "2015-01-01T00:00:00.000Z",
                    "v": 1
                },
                "device": {
                    "key": "key-1",
                    "name": "",
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "sensor": {
                    "key": "temp",
                    "name": "",
                    "attributes": {}
                }
            },
            "transition_to": "warning",
            "actions": [
                {
                    "payload": "test payload",
                    "recipient": "me",
                    "response": "all good",
                    "status": "200",
                    "action_type": "webhook"
                }
            ]
        }
    ]
}"""

        decoder = TempoIQDecoder()
        decoder.decoder = decoder.decode_alert
        decoded = json.loads(j, object_hook=decoder)
        self.assertEquals(decoded.id, 1)
        self.assertEquals(decoded.rule_key, 'key-1')
        self.assertTrue(isinstance(decoded.transitions[0], Transition))
        self.assertTrue(isinstance(decoded.transitions[0].instigator,
                                   Instigator))

    def test_decoder_for_list_of_alerts(self):
        j = """{"data":[{
    "alert_id": 1,
    "rule_key": "key-1",
    "transitions": [
        {
            "timestamp": "2015-01-01T00:00:00.000Z",
            "instigator": {
                "datapoint": {
                    "t": "2015-01-01T00:00:00.000Z",
                    "v": 1
                },
                "device": {
                    "key": "key-1",
                    "name": "",
                    "attributes": {
                        "foo": "bar"
                    }
                },
                "sensor": {
                    "key": "temp",
                    "name": "",
                    "attributes": {}
                }
            },
            "transition_to": "warning",
            "actions": [
                {
                    "payload": "test payload",
                    "recipient": "me",
                    "response": "all good",
                    "status": "200",
                    "action_type": "webhook"
                }
            ]
        }
    ]
}]}"""

        decoder = TempoIQDecoder()
        decoder.decoder = decoder.decode_alert
        decoded = json.loads(j, object_hook=decoder)
        self.assertEquals(decoded['data'][0].id, 1)
        self.assertEquals(decoded['data'][0].rule_key, 'key-1')
        self.assertTrue(isinstance(decoded['data'][0].transitions[0],
                                   Transition))
        self.assertTrue(isinstance(
            decoded['data'][0].transitions[0].instigator,
            Instigator))

    def test_device_decoder(self):
        j = """{
                 "key": "test-dev",
                 "name": "",
                 "attributes": {
                   "type": "blarg"
                 },
                 "sensors": [
                   {
                     "key": "vals",
                     "name": "stuff",
                     "attributes": {}
                   }
                 ]
            }"""
        decoder = DeviceDecoder()
        decoded = json.loads(j, object_hook=decoder)
        self.assertEquals(decoded.key, 'test-dev')
        self.assertEquals(decoded.attributes['type'], 'blarg')
        self.assertEquals(decoded.sensors[0].key, 'vals')
