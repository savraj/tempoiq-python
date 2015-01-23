import uuid
from tempoiq.temporal.validate import convert_iso_stamp
from device import Device
from sensor import Sensor
from stream import Stream
from point import Point
from query.selection import AndClause, Compound, OrClause, ScalarSelector, and_
from query.selection import Selection


class NoResultError(Exception):
    pass


class TooManyResultsError(Exception):
    pass


class Row(object):
    """Data from one or more sensors at a single timestamp. Returned when
    reading sensor data.

    Example values dict of a row with a single sensor, *temperature*\ , on a
    single device, *test1*\ ::

        {'test1': {'temperature': 500.0} }

    :var timestamp: DateTime of the sensor data
    :var values: dict mapping device key to a dict of sensor keys to values
    """
    def __init__(self, row_json):
        self.timestamp = convert_iso_stamp(row_json['t'])
        self.values = row_json['data']

    def __getitem__(self, key):
        return self.values[key]

    def __iter__(self):
        for device in self.values:
            for sensor in self.values[device]:
                yield ((device, sensor), self.values[device][sensor])


class PointStream(object):
    def __init__(self, stream_info, manager):
        self.stream_info = stream_info
        self.manager = manager
        self._id = str(stream_info['id'])
        self.key = str(uuid.uuid1())
        self._device = None
        self._sensor = None
        self.function = stream_info.get('function')

    def __iter__(self):
        while True:
            data = self.manager.next(self)
            time_str = data['t']
            value = data['data'].get(self._id)
            if value is None:
                continue
            timestamp = convert_iso_stamp(time_str)
            yield Point(timestamp, value)

    @property
    def device(self):
        if self._device is None:
            si = self.stream_info['device']
            self._device = Device(si['key'], si.get('name', ''),
                                  si['attributes'])
        return self._device

    @property
    def sensor(self):
        if self._sensor is None:
            si = self.stream_info['sensor']
            self._sensor = Sensor(si['key'], si.get('name', ''),
                                  si['attributes'])
        return self._sensor


class StreamInfo(object):
    def __init__(self, headers):
        self.headers = headers

    def filter(self, selection):
        evaluator = SelectionEvaluator(selection)
        return evaluator.filter(self.headers)

    def get_one(self, **kwargs):
        selection = self._compile_kwargs(kwargs)
        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(self.headers)]
        if len(results) < 1:
            raise NoResultError('Selection would return no results')
        elif len(results) > 1:
            msg = 'Selection would return more than one result'
            raise TooManyResultsError(msg)
        else:
            return results[0]

    def _compile_kwargs(self, kwargs):
        selectors = []
        for k in kwargs:
            if k == 'device_key':
                selectors.append(Device.key == kwargs[k])
            elif k == 'sensor_key':
                selectors.append(Sensor.key == kwargs[k])
            elif k == 'device_name':
                selectors.append(Device.name == kwargs[k])
            elif k == 'sensor_name':
                selectors.append(Sensor.name == kwargs[k])
            elif k == 'device_attributes':
                for ki in kwargs[k]:
                    selectors.append(Device.attributes[ki] == kwargs[k][ki])
            elif k == 'sensor_attributes':
                for ki in kwargs[k]:
                    selectors.append(Sensor.attributes[ki] == kwargs[k][ki])
            elif k == 'function':
                selectors.append(Stream.function == kwargs[k])
            else:
                msg = 'Invalid bind argument given: "%s"' % k
                raise ValueError(msg)
        selection = Selection()
        selection.add(and_(selectors))
        return selection


class SelectionEvaluator(object):
    def __init__(self, selection):
        self.selection = selection

    def filter(self, headers):
        for header in headers:
            result = self._evaluate_selector(self.selection.selection, header)
            if result:
                yield header

    def _evaluate_compound_clause(self, clause, header):
        if isinstance(clause, AndClause):
            return self._evaluate_and_clause(clause, header)
        elif isinstance(clause, OrClause):
            return self._evaluate_or_clause(clause, header)
        else:
            raise ValueError('Invalid compound clause in selection')

    def _evaluate_and_clause(self, clause, header):
        for selector in clause.selectors:
            matches = self._evaluate_selector(selector, header)
            if not matches:
                return False
        return True

    def _evaluate_or_clause(self, clause, header):
        all_matches = []
        for selector in clause.selectors:
            matches = self._evaluate_selector(selector, header)
            all_matches.append(matches)
        if any(all_matches):
            return True
        return False

    def _evaluate_selector_on_object(self, selector, header, object_type):
        if selector.key == 'key':
            return selector.value == header[object_type]['key']
        elif selector.key == 'name':
            return selector.value == header[object_type]['name']
        elif selector.key == 'function':
            return selector.value == header.get('function')
        else:
            key = selector.value.keys()[0]
            header_value = header[object_type]['attributes'].get(key)
            return selector.value[key] == header_value

    def _evaluate_device_selector(self, selector, header):
        return self._evaluate_selector_on_object(selector, header, 'device')

    def _evaluate_sensor_selector(self, selector, header):
        return self._evaluate_selector_on_object(selector, header, 'sensor')

    def _evaluate_stream_selector(self, selector, header):
        return self._evaluate_selector_on_object(selector, header, 'stream')

    def _evaluate_scalar_selector(self, selector, header):
        if selector.selection_type == 'devices':
            return self._evaluate_device_selector(selector, header)
        elif selector.selection_type == 'sensors':
            return self._evaluate_sensor_selector(selector, header)
        elif selector.selection_type == 'streams':
            return self._evaluate_stream_selector(selector, header)
        else:
            raise ValueError('Invalid selection type in selection')

    def _evaluate_selector(self, selector, header):
        if isinstance(selector, Compound):
            return self._evaluate_compound_clause(selector, header)
        elif isinstance(selector, ScalarSelector):
            return self._evaluate_scalar_selector(selector, header)
        else:
            raise ValueError('Invalid selector in selection')
