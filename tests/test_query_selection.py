import unittest
from tempoiq.protocol.device import Device
from tempoiq.protocol.sensor import Sensor
from tempoiq.protocol.query.selection import Selection, AndClause, \
    ScalarSelector, and_, or_


class TestSelectionAPI(unittest.TestCase):
    def test_device_key_selection(self):
        selector = Device.key == 'foo'
        self.assertEquals(selector.selection_type, 'devices')
        self.assertEquals(selector.key, 'key')
        self.assertEquals(selector.value, 'foo')

    def test_device_name_selection(self):
        selector = Device.name == 'foo'
        self.assertEquals(selector.selection_type, 'devices')
        self.assertEquals(selector.key, 'name')
        self.assertEquals(selector.value, 'foo')

    def test_device_attributes_selection(self):
        selector = Device.attributes['foo'] == 'bar'
        self.assertEquals(selector.selection_type, 'devices')
        self.assertEquals(selector.key, 'attributes')
        self.assertEquals(selector.value, {'foo': 'bar'})

    def test_sensor_key_selection(self):
        selector = Sensor.key == 'foo'
        self.assertEquals(selector.selection_type, 'sensors')
        self.assertEquals(selector.key, 'key')
        self.assertEquals(selector.value, 'foo')

    def test_sensor_name_selection(self):
        selector = Sensor.name == 'foo'
        self.assertEquals(selector.selection_type, 'sensors')
        self.assertEquals(selector.key, 'name')
        self.assertEquals(selector.value, 'foo')

    def test_sensor_attributes_selection(self):
        selector = Sensor.attributes['foo'] == 'bar'
        self.assertEquals(selector.selection_type, 'sensors')
        self.assertEquals(selector.key, 'attributes')
        self.assertEquals(selector.value, {'foo': 'bar'})


class TestSelectionObjects(unittest.TestCase):
    def test_add_compound_selection_first(self):
        selection = Selection()
        clause = AndClause()
        clause.add(ScalarSelector('devices', 'foo', 'bar'))
        selection.add(clause)
        self.assertEquals(selection.selection, clause)

    def test_add_scalar_selector_first(self):
        selection = Selection()
        selector = ScalarSelector('devices', 'foo', 'bar')
        selection.add(selector)
        self.assertTrue(isinstance(selection.selection, ScalarSelector))

    def test_and_clause_sets_correct_selection_type(self):
        selectors = [Device.key == 'foo', Device.key == 'bar']
        clause = and_(selectors)
        self.assertEquals(clause.selection_type, 'devices')

    def test_or_clause_sets_correct_selection_type(self):
        selectors = [Sensor.key == 'foo', Sensor.key == 'bar']
        clause = or_(selectors)
        self.assertEquals(clause.selection_type, 'sensors')
