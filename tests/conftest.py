#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import pytest

from web3 import Web3
from web3.middleware import geth_poa_middleware

import xquery.db
import xquery.cache
import xquery.provider
from xquery.config import CONFIG as C


@pytest.fixture(scope="session")
def c():
    return xquery.cache.Cache_Redis(
        host=C["REDIS_HOST"],
        port=C["REDIS_PORT"],
        password=C["REDIS_PASSWORD"],
        db=C["REDIS_DATABASE"],
    )


@pytest.fixture(scope="session")
def w3():
    w = Web3(Web3.HTTPProvider(endpoint_uri=None))
    w.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w


@pytest.fixture(scope="session")
def db():
    return xquery.db.FusionPGSQL(
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


@pytest.fixture(scope="session")
def dbm():
    # TODO memory only database for testing
    # TODO should also init the tables with alembic
    # https://docs.pytest.org/en/6.2.x/fixture.html#scope-sharing-fixtures-across-classes-modules-packages-or-session
    raise NotImplementedError
