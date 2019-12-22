#!/usr/bin/env python

from setuptools import setup

setup(name='target-bigquery',
      version='0.0.1',
      description='Singer.io target for writing data to Google BigQuery',
      author='Said Tezel',
      url='https://github.com/saidtezel/target-google-bigquery',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_bigquery'],
      install_requires=[
          'jsonschema==2.6.0',
          'singer-python>=1.5.0',
          'google-api-python-client>=1.6.2',
          'google-cloud>=0.34.0',
          'google-cloud-bigquery>=1.9.0',
          'oauth2client',
      ],
      entry_points='''
          [console_scripts]
          target-bigquery=target_bigquery:main
      ''',
)
