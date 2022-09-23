#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import logging
import sys

from sqlalchemy import select

from web3 import Web3
from web3.middleware import geth_poa_middleware

import xquery.cache
import xquery.db
import xquery.db.orm as orm
from xquery.provider import BatchHTTPProvider
from xquery.middleware import http_backoff_retry_request_middleware

from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)

MIN_PYTHON = (3, 8)
if sys.version_info < MIN_PYTHON:
    sys.exit("Python {}.{} or later is required!".format(*MIN_PYTHON))


@timeit
def main() -> int:
    """
    Simple testing script to ensure the environment is working.

    :return:
    """
    logging.basicConfig(level=C["LOG_LEVEL"], format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    # check rpc node
    w3 = Web3(BatchHTTPProvider(endpoint_uri=C["API_URL"]))
    w3.middleware_onion.clear()
    w3.middleware_onion.add(http_backoff_retry_request_middleware, "http_backoff_retry_request")
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    w3.eth.get_block_number()

    # check database
    db = xquery.db.FusionSQL(
        conn=xquery.db.build_url(
            driver=C["DB_DRIVER"],
            host=C["DB_HOST"],
            port=C["DB_PORT"],
            username=C["DB_USERNAME"],
            password=C["DB_PASSWORD"],
            database=C["DB_DATABASE"],
        ),
        verbose=C["DB_DEBUG"],
    )

    # load any table to ensure the models were migrated
    with db.session() as session:
        session.execute(
            select(orm.IndexerState)
        ).scalar()

    # check cache
    cache = xquery.cache.Cache_Redis(
        host=C["REDIS_HOST"],
        port=C["REDIS_PORT"],
        password=C["REDIS_PASSWORD"],
        db=C["REDIS_DATABASE"],
    )

    cache.ping()

    return 0


if __name__ == "__main__":
    sys.exit(main())
