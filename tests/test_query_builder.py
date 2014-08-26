import unittest
from tempoiq.protocol.device import Device
from tempoiq.protocol.sensor import Sensor
from tempoiq.protocol.query.builder import QueryBuilder
from tempoiq.protocol.query.selection import AndClause, OrClause, or_


class TestQueryBuilder(unittest.TestCase):
    def test_query_builder_scalar_filter(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.key == 'foo')
        self.assertTrue(isinstance(qb.selection['devices'].selection,
                                   AndClause))

    def test_query_builder_chained_scalar_filter(self):
        qb = QueryBuilder(None, Sensor)
        qb.filter(Device.key == 'foo').filter(Sensor.key == 'bar')
        self.assertTrue(isinstance(qb.selection['devices'].selection,
                                   AndClause))
        self.assertTrue(isinstance(qb.selection['sensors'].selection,
                                   AndClause))

    def test_query_builder_or_clause(self):
        qb = QueryBuilder(None, Sensor)
        selector = or_([Device.key == 'foo', Device.key == 'bar'])
        qb.filter(selector)
        self.assertTrue(isinstance(qb.selection['devices'].selection,
                                   OrClause))
