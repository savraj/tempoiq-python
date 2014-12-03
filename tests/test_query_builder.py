import unittest
from tempoiq.protocol.device import Device
from tempoiq.protocol.sensor import Sensor
from tempoiq.protocol.rule import Rule
from tempoiq.protocol.query.builder import QueryBuilder
from tempoiq.protocol.query.builder import DELETEKEYMSG
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
            qb.single('latest')

    def test_latest_value(self):
        qb = QueryBuilder(None, Sensor)
        #this will raise an error which is fine, just want to check a side
        #effect
        try:
            qb.latest()
        except:
            pass
        self.assertEquals(qb.operation.name, 'single')
        self.assertEquals(qb.operation.args, {'include_selection': False, 'function': 'latest'})

    def test_single_value_with_timestamp(self):
        qb = QueryBuilder(None, Sensor)
        #this will raise an error which is fine, just want to check a side
        #effect
        try:
            qb.single('before', timestamp='2014-09-15T00:00:01Z')
        except:
            pass
        self.assertEquals(qb.operation.name, 'single')
        self.assertEquals(qb.operation.args, {'include_selection': False,
                                              'function': 'before',
                                              'timestamp': '2014-09-15T00:00:01Z'})

    def test_single_value_with_pipeline(self):
        qb = QueryBuilder(None, Sensor)
        qb.convert_timezone('America/Chicago')
        try:
            qb.single(function='exact', timestamp=None)
        except:
            pass
        self.assertEquals(qb.pipeline[0].name, 'convert_tz')
        self.assertEquals(qb.pipeline[0].args[0], 'America/Chicago')

    def test_single_value_with_include_selection(self):
        qb = QueryBuilder(None, Sensor)
        #this will raise an error which is fine, just want to check a side
        #effect
        try:
            qb.latest(include_selection=True)
        except:
            pass
        self.assertEquals(qb.operation.args, {'include_selection': True, 'function': 'latest'})

    def test_query_builder_with_valid_delete_datapoints(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.key == 'bar')
        qb.filter(Sensor.key == 'foo')
        try:
            qb.delete(start='then', end='now')
        except:
            pass
        self.assertEquals(qb.operation.args, {'start': 'then', 'stop': 'now',
                                              'device_key': 'bar',
                                              'sensor_key': 'foo'})

    def test_query_builder_with_invalid_with_no_selection(self):
        qb = QueryBuilder(None, Sensor)
        with self.assertRaises(ValueError) as e:
            qb.delete(start='then', end='now')
        self.assertEquals(e.exception.message, DELETEKEYMSG)

    def test_query_builder_invalid_with_attr_selection(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.attributes['foo'] == 'bar')
        with self.assertRaises(ValueError) as e:
            qb.delete(start='then', end='now')

        self.assertEquals(e.exception.message, DELETEKEYMSG)

    def test_query_builder_invalid_with_compound_selection(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.key == 'bar')
        qb.filter(Sensor.key == 'foo')
        qb.filter(Sensor.key == 'baz')
        with self.assertRaises(ValueError) as e:
            qb.delete(start='then', end='now')

        self.assertEquals(e.exception.message, DELETEKEYMSG)
