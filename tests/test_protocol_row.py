import unittest
import pytz
import datetime
from tempoiq.protocol.row import Row


class TestRow(unittest.TestCase):
    def test_row_timestamp_parsing_with_utc(self):
        data = {
            't': '2014-01-01T00:00:00Z',
            'data': {
                'device-1': {
                    'sensor-1': 1.0,
                    'sensor-2': 2.0
                },
                'device-2': {
                    'sensor-2': 3.0
                }
            }
        }

        row = Row(data)
        self.assertEquals(row.timestamp, datetime.datetime(2014, 1, 1,
                                                           tzinfo=pytz.utc))

    def test_row_timestamp_parsing_with_timezone(self):
        data = {
            't': '2014-01-01T00:00:00-05:00',
            'data': {
                'device-1': {
                    'sensor-1': 1.0,
                    'sensor-2': 2.0
                },
                'device-2': {
                    'sensor-2': 3.0
                }
            }
        }

        row = Row(data)
        tz = pytz.timezone("US/Eastern")
        self.assertEquals(row.timestamp, datetime.datetime(2014, 1, 1,
                                                           tzinfo=tz))

    def test_row_dict_like_access(self):
        data = {
            't': '2014-01-01T00:00:00Z',
            'data': {
                'device-1': {
                    'sensor-1': 1.0,
                    'sensor-2': 2.0
                },
                'device-2': {
                    'sensor-2': 3.0
                }
            }
        }

        row = Row(data)
        value1 = row['device-1']['sensor-1']
        value2 = row['device-1']['sensor-2']
        value3 = row['device-2']['sensor-2']
        self.assertEquals(value1, 1.0)
        self.assertEquals(value2, 2.0)
        self.assertEquals(value3, 3.0)

    def test_row_iterator(self):
        data = {
            't': '2014-01-01T00:00:00Z',
            'data': {
                'device-1': {
                    'sensor-1': 1.0,
                    'sensor-2': 2.0
                },
                'device-2': {
                    'sensor-2': 3.0
                }
            }
        }

        row = Row(data)
        values = [r for r in row]
        expected = [
            (('device-1', 'sensor-1'), 1.0),
            (('device-1', 'sensor-2'), 2.0),
            (('device-2', 'sensor-2'), 3.0)
        ]
        self.assertEquals(values, expected)
