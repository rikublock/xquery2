#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import logging
import sys

from web3 import Web3
from web3.middleware import geth_poa_middleware

from sqlalchemy import (
    delete,
    select,
)

import xquery.cache
import xquery.contract
import xquery.controller
import xquery.db
import xquery.db.orm as orm
import xquery.event.filter
import xquery.event.indexer
from xquery.provider import BatchHTTPProvider
from xquery.middleware import http_backoff_retry_request_middleware

from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger("main")

MIN_PYTHON = (3, 8)
if sys.version_info < MIN_PYTHON:
    sys.exit("Python {}.{} or later is required!".format(*MIN_PYTHON))


@timeit
def main() -> int:
    """
    Example XQuery configuration for the Pangolin exchange.

    :return:
    """
    logging.basicConfig(level=C["LOG_LEVEL"], format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    chain = orm.Chain.AVAX

    # Note: remove the default `http_retry_request_middleware` retry middleware as it cannot properly
    # handle `eth_getLogs` throttle errors
    try:
        w3 = Web3(BatchHTTPProvider(endpoint_uri=C["API_URL"]))
        w3.middleware_onion.clear()
        w3.middleware_onion.add(http_backoff_retry_request_middleware, "http_backoff_retry_request")
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        log.error(e)
        return 1

    assert w3.eth.chain_id == int(chain)

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

    cache = xquery.cache.Cache_Redis(
        host=C["REDIS_HOST"],
        port=C["REDIS_PORT"],
        password=C["REDIS_PASSWORD"],
        db=C["REDIS_DATABASE"],
    )

    # ensure the service is running
    cache.ping()
    cache.flush()

    # create event list
    png_router = xquery.contract.png_router
    contract_address = Web3.toChecksumAddress(png_router.address)

    # load the indexer state
    with db.session() as session:
        state = session.execute(
            select(orm.IndexerState)
                .filter(orm.IndexerState.name == "default")
        ).scalar()

        # default
        if state is None:
            log.info(f"Creating new indexer state for contract '{contract_address}' running on {chain}")
            state = orm.IndexerState(
                name="default",
                block_number=0,
                block_hash=None,
                discarded=False,
            )
            session.add(state)
            session.commit()

        # Note: we generally want 'expire_on_commit' except in this very specific case
        # see: https://stackoverflow.com/questions/16907337/sqlalchemy-eager-loading-on-object-refresh
        session.refresh(state)
        session.expunge(state)

    # Discard entries from possibly invalid/changed blocks
    # In case we indexed all the way to the latest block in the previous run,
    # we should consider that the chain might underwent minor reorganisations.
    # As a first, simple strategy we just remove the N latest indexed blocks.
    safety_blocks = 20
    if not state.discarded and state.block_number > 0:
        with db.session() as session:
            session.execute(
                delete(orm.XQuery)
                    .filter(orm.XQuery.block_height > state.block_number - safety_blocks)
            )

            state.block_number = max(0, state.block_number - safety_blocks)
            state.block_hash = None
            state.discarded = True
            session.add(state)

            session.commit()
            session.refresh(state)
            session.expunge(state)

    # select the event indexer class/type
    # Note: will be instantiated in the worker process and therefore needs to be passed as type
    indexer_cls = xquery.event.EventIndexerRouterPangolin

    # create an event filter
    event_filter = xquery.event.EventFilterRouterPangolin(
        w3=w3,
    )

    with xquery.controller.Controller(w3=w3, db=db, indexer_cls=indexer_cls, num_workers=16) as c:
        start_block = max(png_router.from_block, state.block_number + 1)

        # Run the scan
        c.scan(
            start_block=start_block,
            end_block="latest",
            num_safety_blocks=0,
            filter_=event_filter,
            chunk_size=2048,
            max_chunk_size=2048,  # public AVAX node limit
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
