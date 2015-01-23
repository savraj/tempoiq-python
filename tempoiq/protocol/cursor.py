from collections import defaultdict
from row import Row, StreamInfo, PointStream
from device import Device
from sensor import Sensor


def make_row_generator(rows):
    """"Utility function for converting a list to a generator.

    :param list d: the list to convert
    :rtype: generator"""

    for r in rows:
        yield Row(r)


def make_device_generator(devices):
    for d in devices:
        sensors = []
        for s in d['sensors']:
            sensors.append(Sensor(s['key'], s.get('name', ''),
                                  s['attributes']))
        yield Device(d['key'], d.get('name', ''), d['attributes'], sensors)


def check_response(resp):
    """Utility function for checking the status of a cursor increment.  Raises
    an exception if the call to the paginated link returns anything other than
    a 200.

    :param resp: the response to check
    :type resp: :class:`tempodb.response.Response` object
    :raises ValueError: if the response is not a 200
    :rtype: None"""

    s = resp.status
    if s != 200:
        raise ValueError(
            'TempoIQ API returned %d as status when 200 was expected' % s)


class Cursor(object):
    """An iterable cursor over data retrieved from the TempoDB API.  The
    cursor will make network requests to fetch more data as needed, until
    the API returns no more data.  It can be used with the standard
    iterable interface::

        >>> data = [d for d in response.data]"""

    def __init__(self, data,  response):
        self.response = response
        self.data = make_row_generator(data)

    def __iter__(self):
        while True:
            try:
                x = self.data.next()
                yield x
            except StopIteration:
                self._fetch_next()

    def _fetch_next(self):
        raise StopIteration


class DeviceCursor(Cursor):
    """The data attribute holds the actual data from the request.

    Additionally, the raw response object is available as the response
    attribute of the cursor.

    :param response: the raw response object
    :type response: :class:`tempodb.response.Response"""

    def __init__(self, response, data, fetcher):
        self.response = response
        self.fetcher = fetcher
        self._raw_data = data
        self.data = make_device_generator(data['data'])

    def _fetch_next(self):
        try:
            cursor_obj = self._raw_data['next_page']['next_query']
            new_data = self.fetcher(cursor_obj)
            self._raw_data = new_data
            self.response.data = new_data
            self.data = make_device_generator(new_data['data'])
        except KeyError:
            raise StopIteration


class DataPointsCursor(Cursor):
    """The data attribute holds the actual data from the request.

    Additionally, the raw response object is available as the response
    attribute of the cursor.

    :param response: the raw response object
    :type response: :class:`tempodb.response.Response"""

    def __init__(self, response, data, fetcher):
        self.response = response
        self.fetcher = fetcher
        self._raw_data = data
        self.data = make_row_generator(data['data'])

    def _fetch_next(self):
        try:
            cursor_obj = self._raw_data['next_page']['next_query']
            new_data = self.fetcher(cursor_obj)
            self._raw_data = new_data
            self.response.data = new_data
            self.data = make_row_generator(new_data['data'])
        except KeyError:
            raise StopIteration
        except Exception:
            raise


class Page(object):
    def __init__(self, data, cursor_obj, collectible=True):
        self.data = data
        self.cursor_obj = cursor_obj
        self.is_collectible = collectible

    def garbage_collect(self):
        self.data = None

    def is_active(self):
        if self.data is not None:
            return True
        return False

    def reconstruct(self, cursor):
        try:
            self.data = cursor.fetcher(self.cursor_obj)['data']
        except Exception:
            raise


class StreamManager(object):
    MAX_PAGES = 100

    def __init__(self, cursor, starting_data, page_size):
        if starting_data.get('next_page') is not None:
            cursor_obj = starting_data['next_page']['next_query']
        else:
            cursor_obj = None
        self.pages = {0: Page(starting_data['data'], None, False)}
        self.active_pages = 1
        self.receiver_pointers = {}
        self.active_pointers = defaultdict(set)
        self.cursor = cursor
        self.page_size = page_size

    def _garbage_collect(self):
        to_collect = self.active_pages - StreamManager.MAX_PAGES
        max_iters = 2
        current_iters = 0
        while to_collect > 0 and current_iters < max_iters:
            for k in self.pages:
                if len(self.active_pointers[k]) == 0:
                    page = self.pages[k]
                    if page.is_collectible:
                        page.garbage_collect()
                        self.active_pages -= 1
                        to_collect -= 1
                        if to_collect == 0:
                            break
                    else:
                        continue
                    #dont want to be too aggressive here, we are guaranteeing
                    #max memory used and trying to keep network traffic
                    #to a minimum
            current_iters += 1

    def _update_active_pointers(self, key, page_num):
        if page_num == 0:
            self.active_pointers[page_num].add(key)
        elif key not in self.active_pointers[page_num]:
            old_page = page_num - 1
            self.active_pointers[old_page].remove(key)
            self.active_pointers[page_num].add(key)

    def fetch_next(self):
        next_idx = self.pages.keys()[-1] + 1
        new_data, cursor_obj = self.cursor._fetch_next()
        page = Page(new_data['data'], cursor_obj)
        self.pages[next_idx] = page
        self.active_pages += 1
        return page

    def next(self, receiver):
        if not self.receiver_pointers.get(receiver.key):
            self.receiver_pointers[receiver.key] = 0
            self.active_pointers[0].add(receiver.key)

        current_idx = self.receiver_pointers[receiver.key]
        page_num = current_idx / self.page_size
        item = current_idx % self.page_size
        self.receiver_pointers[receiver.key] = current_idx + 1
        self._update_active_pointers(receiver.key, page_num)

        page = self.pages.get(page_num)
        if page is None:
            page = self.fetch_next()
            self._garbage_collect()
        if not page.is_active():
            self.reconstruct_page(page)
        return page.data[item]

    def reconstruct_page(self, page):
        page.reconstruct(self.cursor)
        self.active_pages += 1


class StreamResponseCursor(Cursor):
    def __init__(self, response, data, fetcher):
        self.response = response
        self.fetcher = fetcher
        self._raw_data = data
        self.page_size = len(data['data'])
        self.stream_info = StreamInfo(data['streams'])
        self.manager = StreamManager(self, data, self.page_size)

    def __iter__(self):
        streams = self.streams
        iterators = [(s, iter(s)) for s in streams]
        for stream, it in iterators:
            yield ((stream.device, stream.sensor), it.next())

    def _fetch_next(self):
        try:
            cursor_obj = self._raw_data['next_page']['next_query']
            new_data = self.fetcher(cursor_obj)
            self._raw_data = new_data
            if len(new_data['data']) == 0:
                raise StopIteration
            return new_data, cursor_obj
        except KeyError:
            raise StopIteration
        except Exception:
            raise

    def bind_stream(self, **kwargs):
        stream_info = self.stream_info.get_one(**kwargs)
        return PointStream(stream_info, self.manager)

    @property
    def streams(self):
        return [PointStream(si, self.manager) for
                si in self.stream_info.headers]
