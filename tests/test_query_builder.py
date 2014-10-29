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

    def test_single_value_with_invalid_selection(self):
        qb = QueryBuilder(None, Rule)
        with self.assertRaises(TypeError):
            qb.single_value()

    def test_single_value_with_no_pipeline(self):
        qb = QueryBuilder(None, Sensor)
        #this will raise an error which is fine, just want to check a side
        #effect
        try:
            qb.single_value()
        except:
            pass
        self.assertEquals(qb.operation.name, 'single_value')
        self.assertEquals(qb.operation.args, {'include_selection': False})

    def test_single_value_with_pipeline(self):
        qb = QueryBuilder(None, Sensor)
        qb.aggregate('max').rollup('min', '1day')
        try:
            qb.single_value(start='foo', end='bar')
        except:
            pass
        self.assertEquals(qb.pipeline[1].args[-1], 'foo')

    def test_single_value_with_include_selection(self):
        qb = QueryBuilder(None, Sensor)
        #this will raise an error which is fine, just want to check a side
        #effect
        try:
            qb.single_value(include_selection=True)
        except:
            pass
        self.assertEquals(qb.operation.args, {'include_selection': True})
