# TempoIQ Python Library
# Adapted to work with google appengine, by savraj

Removed dependence on requests, swapped in appengine's urlfetch, made a few other changes, like using pytz.gae, see diff

To use, copy paste the tempoiq folder into your project and import tempoiq

===

The TempoIQ Python API library makes calls to the [TempoIQ API](https://tempoiq.com/). The module is available on PyPI as [tempoiq](https://pypi.python.org/pypi/tempoiq/):

    pip install tempoiq

You can also check out this repository and install from source:

    git clone https://github.com/TempoIQ/tempoiq-python.git
    cd tempoiq-python
    python setup.py install


Run unit tests:

    cd path/to/tempoiq-python
    python setup.py nosetests


Build documentation:

    cd path/to/tempoiq-python/docs
    make html
    cd build
    firefox index.html

Examples can be found in the HTML documentation.
