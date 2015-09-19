#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='AerospikeClientMock',
    version='1.0.0',
    description='Aerospike client mock',
    long_description=
    """
        Aerospike client mock based on dict used for unit testing
    """,
    url='https://github.com/tivvit/aerospike-client-mock-python',
    author='Vit Listik',
    author_email='tivvit@seznam.cz',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Testing',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    keywords='Aerospike client test testing mock',
    packages=["AerospikeClientMock"],
)