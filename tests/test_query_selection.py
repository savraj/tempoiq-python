import unittest
from tempoiq.protocol.device import Device
from tempoiq.protocol.sensor import Sensor


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
