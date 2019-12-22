#!/usr/bin/env python3

import argparse
import io
import sys
import json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources

import singer

from .client import BigQuery

REQUIRED_CONFIG_KEYS = [
    "project_id",
    "dateset_id",
    "service_account_file"
]

LOGGER = singer.get_logger()

def process_args():
    # Parse command line arguments
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Check for errors on the provided config params that utils.parse_args is letting through
    if not args.config.get('project_id):
        LOGGER.critical("target-bigquery: a valid project_id must be provided.")
        sys.exit(1)

    if not args.config.get('dataset_id'):
        LOGGER.critical("target-bigquery: a valid dateset_id must be provided.")
        sys.exit(1)

    if not args.config.get('service_account_file'):
        LOGGER.critical("target-bigquery: a valid service_account_file string must be provided.")
        sys.exit(1)

    args.config['validate_records'] = args.config.get('validate_records', True)
    args.config['full_replication'] = args.config.get('full_replication', False)
    args.config['stream_data'] = args.config.get('stream_data', False)

    return args

def main():
    # Parse command line arguments
    args = process_args()

    client = BigQuery(args.config)
    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
