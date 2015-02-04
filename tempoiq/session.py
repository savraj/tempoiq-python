from client import Client
from endpoint import HTTPEndpoint


def get_session(host, key, secret, secure=True, port=None, read_version='v2'):
    """Get a :class:`tempoiq.client.Client` instance with the given session
    information.

    :param String host: Backend's base URL, in the form
                       "your-host.backend.tempoiq.com". For legacy reasons,
                       it is also possible to prepend the URL schema, but this
                       will be deprecated in the future.
    :param String key: API key
    :param String secret: API secret
    :rtype: :class:`tempoiq.client.Client`"""
    endpoint = HTTPEndpoint(host, key, secret, secure, port)
    return Client(endpoint, read_version=read_version)
