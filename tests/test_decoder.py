import unittest
import json
from tempoiq.protocol.query.selection import AndClause
from tempoiq.protocol.decoder import *


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
            ]
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


