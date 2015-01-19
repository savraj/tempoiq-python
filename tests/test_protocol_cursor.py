import mock
import unittest
from tempoiq.protocol.cursor import DataPointsCursor, DeviceCursor
from tempoiq.protocol.cursor import StreamResponseCursor, Page, StreamManager


class DummyType(object):
    def __init__(self, data, response, tz=None):
        self.data = data
        self.response = response
        self.tz = tz


class Dummy(object):
    pass


class DummyResponse(object):
    def __init__(self):
        self.resp = Dummy()
        self.resp.headers = {}
        self.headers = {}
        self.text = ''
        self.session = mock.Mock()
        self.status_code = 200
        self.reason = 'OK'
        self.links = {}


class TestProtocolCursor(unittest.TestCase):
    def test_datapoints_cursor_iteration_with_no_fetch(self):
        first_data = {
            'data': [
                {'t': '2014-01-01T00:00:00',
                 'data': {
                     'test1': {'temp': 1.0}
                 }
                 }
            ]
        }

        def fetcher(cursor):
            return {
                'data': [
                    {'t': '2014-01-02T00:00:00',
                     'data': {
                         'test1': {'temp': 2.0}
                     }
                     }
                ]
            }
        resp = DummyResponse()
        resp.data = first_data
        c = DataPointsCursor(resp, first_data, fetcher)
        results = [d for d in c]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['test1']['temp'], 1.0)

    def test_datapoints_cursor_iteration_with_extra_fetch(self):
        first_data = {
            'next_page': {'next_query': None},
            'data': [
                {'t': '2014-01-01T00:00:00',
                 'data': {
                     'test1': {'temp': 1.0}
                 }
                 }
            ]
        }

        def fetcher(cursor):
            return {
                'data': [
                    {'t': '2014-01-02T00:00:00',
                     'data': {
                         'test1': {'temp': 2.0}
                     }
                     }
                ]
            }
        resp = DummyResponse()
        resp.data = first_data
        c = DataPointsCursor(resp, first_data, fetcher)
        results = [d for d in c]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0]['test1']['temp'], 1.0)
        self.assertEquals(results[1]['test1']['temp'], 2.0)

    def test_device_cursor_iteration_with_no_fetch(self):
        first_data = {
            'data': [
                {'key': 'device1', 'attributes': {}, 'sensors': []}
            ]
        }

        def fetcher(cursor):
            return {
                'data': [
                    {'key': 'device2', 'attributes': {}, 'sensors': []}
                ]
            }
        resp = DummyResponse()
        resp.data = first_data
        c = DeviceCursor(resp, first_data, fetcher)
        results = [d for d in c]
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0].key, 'device1')

    def test_device_cursor_iteration_with_extra_fetch(self):
        first_data = {
            'next_page': {'next_query': None},
            'data': [
                {'key': 'device1', 'attributes': {}, 'sensors': []}
            ]
        }

        def fetcher(cursor):
            return {
                'data': [
                    {'key': 'device2', 'attributes': {},
                     'sensors': [{'key': 'sensor1', 'attributes': {}}]
                    }
                ]
            }
        resp = DummyResponse()
        resp.data = first_data
        c = DeviceCursor(resp, first_data, fetcher)
        results = [d for d in c]
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0].key, 'device1')
        self.assertEquals(results[1].key, 'device2')
        self.assertEquals(results[1].sensors[0].key, 'sensor1')


class TestProtocolPage(unittest.TestCase):
    def test_page_is_active(self):
        page = Page(None, None)
        self.assertFalse(page.is_active())
        page.data = 1
        self.assertTrue(page.is_active())

    def test_garbage_collect_and_reconstruct(self):
        dummy_cursor = Dummy()

        def dummy_fetcher(cursor):
            return {'data': 42}

        dummy_cursor.fetcher = dummy_fetcher
        page = Page(1, None)
        page.garbage_collect()
        self.assertEquals(page.data, None)
        page.reconstruct(dummy_cursor)
        self.assertEquals(page.data, 42)


class TestProtocolStreamManager(unittest.TestCase):
    def test_initial_state(self):
        cursor = Dummy()
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)
        self.assertEquals(ds.active_pages, 1)
        self.assertEquals(ds.page_size, 3)
        self.assertEquals(ds.pages[0].data, [1, 2, 3])

    def test_garbage_collection_with_nothing_to_collect_idempotent(self):
        StreamManager.MAX_PAGES = 2
        cursor = Dummy()
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)
        page2 = Page([4, 5, 6], None)
        ds.active_pages += 1
        ds.pages[1] = page2
        ds.active_pointers['key1'].add(0)
        ds._garbage_collect()
        self.assertEquals(ds.pages[0].data, [1, 2, 3])
        self.assertEquals(ds.pages[1].data, [4, 5, 6])

    def test_garbage_collection_with_one_page(self):
        StreamManager.MAX_PAGES = 1
        cursor = Dummy()
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)
        page2 = Page([4, 5, 6], None)
        ds.active_pages += 1
        ds.pages[1] = page2
        ds.active_pointers[0].add('key1')
        ds._garbage_collect()
        self.assertEquals(ds.pages[0].data, [1, 2, 3])
        self.assertEquals(ds.pages[1].data, None)

    def test_garbage_collection_collects_first_available(self):
        StreamManager.MAX_PAGES = 1
        cursor = Dummy()
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)
        page2 = Page([4, 5, 6], None)
        ds.active_pages += 1
        ds.pages[1] = page2
        ds.active_pointers[1].add('key1')
        ds._garbage_collect()
        self.assertEquals(ds.pages[0].data, None)
        self.assertEquals(ds.pages[1].data, [4, 5, 6])

    def test_garbage_collection_doesnt_loop_infinitely(self):
        StreamManager.MAX_PAGES = 1
        cursor = Dummy()
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)
        page2 = Page([4, 5, 6], None)
        ds.active_pages += 1
        ds.pages[1] = page2
        ds.active_pointers[0].add('key1')
        ds.active_pointers[1].add('key2')
        ds._garbage_collect()
        self.assertEquals(ds.pages[0].data, [1, 2, 3])
        self.assertEquals(ds.pages[1].data, [4, 5, 6])

    def test_data_fetches_by_stream_are_independent(self):
        StreamManager.MAX_PAGES = 1
        cursor = Dummy()
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)
        page2 = Page([4, 5, 6], None)
        ds.active_pages += 1
        ds.pages[1] = page2
        receiver1 = Dummy()
        receiver2 = Dummy()
        receiver1.key = 'key1'
        receiver2.key = 'key2'
        data1 = ds.next(receiver1)
        ds.next(receiver2)
        self.assertEquals(ds.active_pointers[0], set(['key1', 'key2']))
        ds.next(receiver2)
        ds.next(receiver2)
        data2 = ds.next(receiver2)
        self.assertEquals(ds.pages[0].data, [1, 2, 3])
        self.assertEquals(ds.pages[1].data, [4, 5, 6])
        self.assertEquals(data1, 1)
        self.assertEquals(data2, 4)
        self.assertEquals(ds.active_pointers[0], set(['key1']))
        self.assertEquals(ds.active_pointers[1], set(['key2']))

    def test_new_page_fetch_over_limit_forces_a_gc(self):
        StreamManager.MAX_PAGES = 1
        cursor = Dummy()

        def _fetch_next():
            return {'data': [4, 5, 6]}, None

        cursor._fetch_next = _fetch_next
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)

        receiver1 = Dummy()
        receiver1.key = 'key1'
        ds.next(receiver1)
        self.assertEquals(ds.active_pointers[0], set(['key1']))
        ds.next(receiver1)
        ds.next(receiver1)
        data = ds.next(receiver1)
        self.assertEquals(ds.active_pointers[1], set(['key1']))
        self.assertEquals(ds.pages[0].data, None)
        self.assertEquals(ds.pages[1].data, [4, 5, 6])
        self.assertEquals(data, 4)

    def test_new_page_fetch_over_limit_forces_a_gc_2(self):
        StreamManager.MAX_PAGES = 2
        cursor = Dummy()

        def _fetch_next():
            return {'data': [4, 5, 6]}, None

        cursor._fetch_next = _fetch_next
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)

        receiver1 = Dummy()
        receiver2 = Dummy()
        receiver1.key = 'key1'
        receiver2.key = 'key2'
        ds.next(receiver2)
        iters = 0
        while iters < 8:
            ds.next(receiver1)
            iters += 1
        data = ds.next(receiver1)
        self.assertEquals(ds.active_pointers[0], set(['key2']))
        self.assertEquals(ds.active_pointers[1], set([]))
        self.assertEquals(ds.active_pointers[2], set(['key1']))
        self.assertEquals(ds.pages[0].data, [1, 2, 3])
        self.assertEquals(ds.pages[1].data, None)
        self.assertEquals(ds.pages[2].data, [4, 5, 6])
        self.assertEquals(data, 6)

    def test_reconstruction_of_old_page(self):
        StreamManager.MAX_PAGES = 2
        cursor = Dummy()

        def _fetch_next():
            return {'data': [4, 5, 6]}, None

        def fetcher(cursor):
            return {'data': [7, 8, 9]}

        cursor._fetch_next = _fetch_next
        cursor.fetcher = fetcher
        data = {'data': [1, 2, 3], 'next_page': {'next_query': None}}
        ds = StreamManager(cursor, data, 3)

        receiver1 = Dummy()
        receiver2 = Dummy()
        receiver1.key = 'key1'
        receiver2.key = 'key2'
        iters = 0
        while iters < 8:
            ds.next(receiver1)
            iters += 1
        data1 = ds.next(receiver1)
        data2 = ds.next(receiver2)
        self.assertEquals(ds.active_pointers[0], set(['key2']))
        self.assertEquals(ds.active_pointers[1], set([]))
        self.assertEquals(ds.active_pointers[2], set(['key1']))
        self.assertEquals(ds.pages[0].data, [7, 8, 9])
        self.assertEquals(ds.pages[1].data, [4, 5, 6])
        self.assertEquals(ds.pages[2].data, [4, 5, 6])
        self.assertEquals(data1, 6)
        self.assertEquals(data2, 7)


class TestProtocolStreamResponseCursor(unittest.TestCase):
    def test_bind_one_stream_with_single_page(self):
        StreamManager.MAX_PAGES = 2

        def fetcher(cursor):
            raise StopIteration

        data = {'data': [{1: 1}, {1: 2}, {1: 3}],
                'streams': [{'device': {'key': 'foo'}, 'id': 1}],
                'next_page': {'next_query': None}}

        cursor = StreamResponseCursor(None, data, fetcher)
        stream = cursor.bind_stream(device_key='foo')
        data = [s for s in stream]
        self.assertEquals(data, [1, 2, 3])

    def test_bind_one_stream_with_missing_data(self):
        StreamManager.MAX_PAGES = 2

        def fetcher(cursor):
            raise StopIteration

        data = {'data': [{1: 1}, {2: 2}, {1: 3}],
                'streams': [{'device': {'key': 'foo'}, 'id': 1}],
                'next_page': {'next_query': None}}

        cursor = StreamResponseCursor(None, data, fetcher)
        stream = cursor.bind_stream(device_key='foo')
        data = [s for s in stream]
        self.assertEquals(data, [1, 3])

    def test_bind_one_stream_with_multiple_pages(self):
        StreamManager.MAX_PAGES = 2

        class Fetcher(object):
            def __init__(self):
                self.iters = 0

            def __call__(self, cursor):
                if self.iters == 1:
                    raise StopIteration
                else:
                    data = {'data': [{1: 4}, {1: 5}, {1: 6}],
                            'streams': [{'device': {'key': 'foo'}, 'id': 1}],
                            'next_page': {'next_query': None}}
                    self.iters += 1
                    return data

        data = {'data': [{1: 1}, {1: 2}, {1: 3}],
                'streams': [{'device': {'key': 'foo'}, 'id': 1}],
                'next_page': {'next_query': None}}

        cursor = StreamResponseCursor(None, data, Fetcher())
        stream = cursor.bind_stream(device_key='foo')
        data = [s for s in stream]
        self.assertEquals(data, [1, 2, 3, 4, 5, 6])

    def test_bind_multiple_streams_with_single_page(self):
        StreamManager.MAX_PAGES = 2

        def fetcher(cursor):
            raise StopIteration

        data = {'data': [{1: 1, 2: 1},
                         {1: 2, 2: 2},
                         {1: 3, 2: 3}],
                'streams': [
                    {'device': {'key': 'foo'}, 'id': 1},
                    {'device': {'key': 'bar'}, 'id': 2}
                ],
                'next_page': {'next_query': None}}

        cursor = StreamResponseCursor(None, data, fetcher)
        stream1 = cursor.bind_stream(device_key='foo')
        stream2 = cursor.bind_stream(device_key='bar')
        data1 = [s for s in stream1]
        data2 = [s for s in stream2]
        self.assertEquals(data1, [1, 2, 3])
        self.assertEquals(data2, [1, 2, 3])

    def test_bind_multiple_streams_with_multiple_pages(self):
        StreamManager.MAX_PAGES = 2

        class Fetcher(object):
            def __init__(self):
                self.iters = 0

            def __call__(self, cursor):
                if self.iters == 1:
                    raise StopIteration
                else:
                    data = {'data': [{1: 4, 2: 4},
                                     {1: 5},
                                     {1: 6, 2: 6}],
                            'streams': [
                                {'device': {'key': 'foo'}, 'id': 1},
                                {'device': {'key': 'bar'}, 'id': 2}
                            ],
                            'next_page': {'next_query': None}}
                    self.iters += 1
                    return data

        data = {'data': [{1: 1, 2: 1},
                         {1: 2, 2: 2},
                         {1: 3, 2: 3}],
                'streams': [
                    {'device': {'key': 'foo'}, 'id': 1},
                    {'device': {'key': 'bar'}, 'id': 2}
                ],
                'next_page': {'next_query': None}}

        cursor = StreamResponseCursor(None, data, Fetcher())
        stream1 = cursor.bind_stream(device_key='foo')
        stream2 = cursor.bind_stream(device_key='bar')
        data1 = [s for s in stream1]
        data2 = [s for s in stream2]
        self.assertEquals(data1, [1, 2, 3, 4, 5, 6])
        self.assertEquals(data2, [1, 2, 3, 4, 6])

    def test_bind_multiple_streams_with_gc(self):
        StreamManager.MAX_PAGES = 1

        class Fetcher(object):
            def __init__(self):
                self.iters = 0

            def __call__(self, cursor):
                if self.iters >= 2:
                    raise StopIteration
                else:
                    data = {'data': [{1: 1, 2: 4},
                                     {1: 2},
                                     {1: 3, 2: 6}],
                            'streams': [
                                {'device': {'key': 'foo'}, 'id': 1},
                                {'device': {'key': 'bar'}, 'id': 2}
                            ],
                            'next_page': {'next_query': None}}
                    self.iters += 1
                    return data

        data = {'data': [{1: 1, 2: 4},
                         {1: 2},
                         {1: 3, 2: 6}],
                'streams': [
                    {'device': {'key': 'foo'}, 'id': 1},
                    {'device': {'key': 'bar'}, 'id': 2}
                ],
                'next_page': {'next_query': None}}

        cursor = StreamResponseCursor(None, data, Fetcher())
        stream1 = cursor.bind_stream(device_key='foo')
        stream2 = cursor.bind_stream(device_key='bar')
        data1 = []
        iters = 0
        while iters < 6:
            iterator = iter(stream1)
            data1.append(iterator.next())
            iters += 1
        self.assertEquals(cursor.manager.pages[0].data, None)
        data2 = [s for s in stream2]
        self.assertEquals(data1, [1, 2, 3, 1, 2, 3])
        self.assertEquals(data2, [4, 6, 4, 6])

    def test_two_dimensional_iteration(self):
        StreamManager.MAX_PAGES = 2

        class Fetcher(object):
            def __init__(self):
                self.iters = 0

            def __call__(self, cursor):
                if self.iters == 1:
                    raise StopIteration
                else:
                    data = {'data': [{1: 4, 2: 4},
                                     {1: 5},
                                     {1: 6, 2: 6}],
                            'streams': [
                                {'device': {'key': 'foo'}, 'id': 1},
                                {'device': {'key': 'bar'}, 'id': 2}
                            ],
                            'next_page': {'next_query': None}}
                    self.iters += 1
                    return data

        data = {'data': [{1: 1, 2: 1},
                         {1: 2, 2: 2},
                         {1: 3, 2: 3}],
                'streams': [
                    {'device': {'key': 'foo'}, 'id': 1},
                    {'device': {'key': 'bar'}, 'id': 2}
                ],
                'next_page': {'next_query': None}}

        cursor = StreamResponseCursor(None, data, Fetcher())
        streams = cursor.streams
        stream_data = []
        for stream in streams:
            stream_data.append([s for s in stream])
        self.assertEquals(stream_data[0], [1, 2, 3, 4, 5, 6])
        self.assertEquals(stream_data[1], [1, 2, 3, 4, 6])
