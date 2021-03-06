#!/usr/bin/env python

from setuptools import setup

setup(name='yahoo_quote_download',
	version='0.1',
	description='Yahoo Quote Downloader',
	author='c0redumb',
	packages=['yahoo_quote_download'],
	install_requires=[
		'requests',
	],
	zip_safe=False,
	entry_points = {
		'console_scripts': [ 'yqd=yahoo_quote_download.__main__:main' ],
	},
)
