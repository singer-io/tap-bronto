#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-bronto',
    version='1.0.1',
    description='Singer.io tap for extracting data from the Bronto API',
    author='Stitch',
    url='https://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_bronto'],
    install_requires=[
        'zeep==2.5.0',
        'singer-python>=3.5.0',
        'funcy==1.10',
        'voluptuous==0.10.5',
    ],
    entry_points='''
    [console_scripts]
    tap-bronto=tap_bronto:main
    ''',
    packages=['tap_bronto', 'tap_bronto.endpoints']
)
