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
import xquery.provider

from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger("main")

MIN_PYTHON = (3, 8)
if sys.version_info < MIN_PYTHON:
    sys.exit("Python {}.{} or later is required!".format(*MIN_PYTHON))


# Basic XQuery 2.0 program flow
# 0) Controller: manage concurrent elements (see bellow), init scan
# 1) EventFilter: generate/fetch a list of event log entries
# 2) EventIndexer: process filtered event list, prepare orm objects for the database
# 3) DBHandler: safely write objects to the database
# 4) EventProcessor: post-process event log entries in the database
#
# Has several concurrent elements:
# a) Main process:
#    - MainThread:
#       - runs the controller (manages threads, workers)
#       - runs scan function (includes the EventFilter, adds jobs to shared queue)
#    - DBHandlerThread:
#       - gets job results from shared queue
#       - sorts job results
#       - writes job results to the database ("atomically" per block)
# b) Worker processes, each:
#    - runs the EventIndexer
#    - gets jobs from shared queue
#    - adds results to shared queue
# c) Worker processes, each: -> NOT implemented
#    - runs the EventProcessor
#    - reads and writes to database
#
# Note: The indexer state should only ever be updated/changed in the main process!

@timeit
def main() -> int:
    """
    Example XQuery configuration for the Pangolin exchange.

    :return:
    """
    logging.basicConfig(level=logging.INFO, format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    chain = orm.Chains.AVAX
    endpoint_uri = C["API_URL"]["AVAX"]

    # Note: remove the default `http_retry_request_middleware` retry middleware as it cannot properly
    # handle `eth_getLogs` throttle errors
    try:
        w3 = Web3(xquery.provider.BatchHTTPProvider(endpoint_uri=endpoint_uri))
        w3.middleware_onion.clear()
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        log.error(e)
        return 1

    db = xquery.db.FusionPGSQL(
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
    png_pair = xquery.contract.png_pair
    png_rc20 = xquery.contract.png_rc20
    png_wavax = xquery.contract.png_wavax

    contract_router = w3.eth.contract(address=Web3.toChecksumAddress(png_router.address), abi=png_router.abi)
    contract_pair = w3.eth.contract(abi=png_pair.abi)
    contract_rc20 = w3.eth.contract(abi=png_rc20.abi)
    contract_wavax = w3.eth.contract(address=Web3.toChecksumAddress(png_wavax.address), abi=png_wavax.abi)

    events = [
        contract_rc20.events.Approval,  # 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925
        contract_rc20.events.Transfer,  # 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
        contract_pair.events.Burn,   # 0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496
        contract_pair.events.Mint,  # 0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f
        contract_pair.events.Swap,  # 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
        contract_pair.events.Sync,  # 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1
        contract_wavax.events.Deposit,  # 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c
        contract_wavax.events.Withdrawal,  # 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65
    ]

    contract_address = Web3.toChecksumAddress(png_router.address)

    # load the indexer state
    with db.session() as session:
        state = session.execute(
            select(orm.IndexerState)
        ).one_or_none()
        state = state[0] if state else None

        # default
        if state is None:
            log.info(f"Creating new indexer state for contract '{contract_address}' running on {chain}")
            state = orm.IndexerState(
                contract_address=contract_address,
                block_height=0,
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
    if not state.discarded and state.block_height > 0:
        with db.session() as session:
            session.execute(
                delete(orm.XQuery)
                    .filter(orm.XQuery.block_height > state.block_height - safety_blocks)
            )

            state.block_height = max(0, state.block_height - safety_blocks)
            state.block_hash = None
            state.discarded = True
            session.add(state)

            session.commit()
            session.refresh(state)
            session.expunge(state)

    # select the event indexer class/type
    # Note: will be instantiated in the worker process and therefore needs to be passed as type
    indexer_cls = xquery.event.EventIndexer_Pangolin

    # create an event filter
    event_filter = xquery.event.EventFilter_Pangolin(
        w3=w3,
        contract=contract_router,
        events=events,
    )

    with xquery.controller.Controller(w3=w3, db=db, indexer_cls=indexer_cls, num_workers=16) as c:
        start_block = max(png_router.from_block, state.block_height + 1)

        # Run the scan
        c.scan(
            start_block=start_block,
            end_block="latest",
            filter_=event_filter,
            chunk_size=2048,
            max_chunk_size=2048,  # public AVAX node limit
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
