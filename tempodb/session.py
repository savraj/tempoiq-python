from client import Client
from endpoint import HTTPEndpoint


def get_session(url, key, secret):
    endpoint = HTTPEndpoint(url, key, secret)
    return Client(endpoint)
