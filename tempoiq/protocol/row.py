from tempoiq.temporal.validate import convert_iso_stamp
from query.selection import AndClause, Compound, OrClause, ScalarSelector


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


class StreamInfo(object):
    def __init__(self, headers):
        self.headers = headers

    def filter(self, selection):
        evaluator = SelectionEvaluator(selection)
        return evaluator.filter(self.headers)

    def get_one(self, selection):
        evaluator = SelectionEvaluator(selection)
        results = [r for r in evaluator.filter(self.headers)]
        if len(results) < 1:
            raise ValueError('Selection would return no results')
        elif len(results) > 1:
            raise ValueError('Selection would return more than one result')
        else:
            return results[0]


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
        else:
            key = selector.value.keys()[0]
            header_value = header[object_type]['attributes'].get(key)
            return selector.value[key] == header_value

    def _evaluate_device_selector(self, selector, header):
        return self._evaluate_selector_on_object(selector, header, 'device')

    def _evaluate_sensor_selector(self, selector, header):
        return self._evaluate_selector_on_object(selector, header, 'sensor')

    def _evaluate_scalar_selector(self, selector, header):
        if selector.selection_type == 'devices':
            return self._evaluate_device_selector(selector, header)
        elif selector.selection_type == 'sensors':
            return self._evaluate_sensor_selector(selector, header)
        else:
            raise ValueError('Invalid selection type in selection')

    def _evaluate_selector(self, selector, header):
        if isinstance(selector, Compound):
            return self._evaluate_compound_clause(selector, header)
        elif isinstance(selector, ScalarSelector):
            return self._evaluate_scalar_selector(selector, header)
        else:
            raise ValueError('Invalid selector in selection')
