#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/setup.py

Copyright (c) 2012 TempoDB Inc. All rights reserved.
"""

from setuptools import setup


install_requires = [
    'python-dateutil',
    'requests>=1.0',
    'simplejson',
    'pytz',
    'sphinx'
]

tests_require = [
    'mock',
    'unittest2',
]

setup(
    name="tempoiq",
    version="1.0.0",
    author="TempoIQ Inc",
    author_email="aaron.brenzel@tempoiq.com",
    url="http://github.com/tempoiq/tempoiq-python/",
    description="A client for the TempoDB API",
    packages=["tempoiq", "tempoiq.temporal", "tempoiq.protocol",
              "tempoiq.protocol.query"],
    long_description="A Python client for the TempoIQ API.",
    dependency_links=[
    ],
    setup_requires=['nose>=1.0'],
    install_requires=install_requires,
    tests_require=tests_require,
)
