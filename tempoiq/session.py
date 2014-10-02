from client import Client
from endpoint import HTTPEndpoint


def get_session(url, key, secret):
    """Get a :class:`tempoiq.client.Client` instance with the given session
    information.

    :param String url: Backend's base URL, in the form
                       "https://your-url.backend.tempoiq.com"
    :param String key: API key
    :param String secret: API secret
    :rtype: :class:`tempoiq.client.Client`"""
    endpoint = HTTPEndpoint(url, key, secret)
    return Client(endpoint)
