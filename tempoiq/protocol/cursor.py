from row import Row


def make_generator(d):
    """"Utility function for converting a list to a generator.

    :param list d: the list to convert
    :rtype: generator"""

    for i in d:
        yield Row(i)


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
        self.data = make_generator(data)

    def __iter__(self):
        while True:
            try:
                x = self.data.next()
                yield x
            except StopIteration:
                self._fetch_next()

    def _fetch_next(self):
        raise StopIteration


class TempoIQCursor(Cursor):
    """The data attribute holds the actual data from the request.

    Additionally, the raw response object is available as the response
    attribute of the cursor.

    :param response: the raw response object
    :type response: :class:`tempodb.response.Response"""

    def __init__(self, response, data, fetcher):
        self.response = response
        self.fetcher = fetcher
        self.data = make_generator(data.data)

    def _fetch_next(self):
        try:
            cursor_obj = self.response.data['cursor']['next_query']
            new_data = self.fetcher(cursor_obj)
            self.data = make_generator(new_data['data'])
        except KeyError:
            raise StopIteration
