import unittest
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
        self.assertEquals(selector.__class__.__name__, 'AndClause')
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
