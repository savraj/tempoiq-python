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
