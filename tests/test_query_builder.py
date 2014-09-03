import unittest
from tempoiq.protocol.device import Device
from tempoiq.protocol.sensor import Sensor
from tempoiq.protocol.rule import Rule
from tempoiq.protocol.query.builder import QueryBuilder
from tempoiq.protocol.query.builder import extract_key_for_monitoring
from tempoiq.protocol.query.selection import AndClause, OrClause, or_
from tempoiq.protocol.query.selection import ScalarSelector


class TestQueryBuilder(unittest.TestCase):
    def test_query_builder_scalar_filter(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.key == 'foo')
        self.assertTrue(isinstance(qb.selection['devices'].selection,
                                   ScalarSelector))

    def test_query_builder_chained_scalar_filter(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.key == 'foo').filter(Sensor.key == 'bar')
        self.assertTrue(isinstance(qb.selection['devices'].selection,
                                   ScalarSelector))
        self.assertTrue(isinstance(qb.selection['sensors'].selection,
                                   ScalarSelector))

    def test_query_builder_chained_dict_filter(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.attributes['foo'] == 'bar').filter(
            Device.attributes['baz'] == 'boz')
        self.assertTrue(isinstance(qb.selection['devices'].selection,
                                   AndClause))

    def test_query_builder_or_clause(self):
        qb = QueryBuilder(None, Sensor)
        selector = or_([Device.key == 'foo', Device.key == 'bar'])
        qb.filter(selector)
        self.assertTrue(isinstance(qb.selection['devices'].selection,
                                   OrClause))

    def test_query_builder_extract_monitoring_key(self):
        qb = QueryBuilder(None, Rule)
        qb.filter(Rule.key == 'foo')
        key = extract_key_for_monitoring(qb.selection['rules'])
        self.assertEquals(key, 'foo')
