#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import logging
import sys
import time

import pidfile

from web3 import Web3
from web3.middleware import geth_poa_middleware

from sqlalchemy import select

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
    Example XQuery configuration for the Pegasys exchange.
    """
    logging.basicConfig(
        level=C["LOG_LEVEL"],
        format=C["LOG_FORMAT"],
        datefmt=C["LOG_DATE_FORMAT"],
        handlers=[
            # logging.FileHandler(filename="run.log", mode="w"),
            logging.StreamHandler(),
        ]
    )

    w3 = Web3(BatchHTTPProvider(endpoint_uri=C["API_URL"], request_kwargs={"timeout": 120}))
    w3.middleware_onion.clear()
    w3.middleware_onion.add(http_backoff_retry_request_middleware, "http_backoff_retry_request")
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    # TODO
    # assert w3.eth.chain_id == int(orm.Chain.SYS)

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

    # factory contract
    psys_factory = xquery.contract.psys_factory
    contract_address = Web3.toChecksumAddress(psys_factory.address)

    # load the indexer state
    with db.session() as session:
        state = session.execute(
            select(orm.IndexerState)
                .filter(orm.IndexerState.name == "default")
        ).scalar()

        # default
        if state is None:
            log.info(f"Creating new default indexer state for contract '{contract_address}'.")
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

    # load pair addresses
    with db.session() as session:
        pairs = session.execute(
            select(orm.Pair)
        ).scalars().all()

        pair_addresses = set([pair.address for pair in pairs])

    # select the event indexer class/type
    # Note: will be instantiated in the worker process and therefore needs to be passed as type
    indexer_cls = xquery.event.EventIndexerExchangePegasys

    # create an event filter
    event_filter = xquery.event.EventFilterExchangePegasys(
        w3=w3,
        pair_addresses=pair_addresses,
    )

    with xquery.controller.Controller(w3=w3, db=db, indexer_cls=indexer_cls, num_workers=int(C["XQ_NUM_WORKERS"])) as c:
        start_block = max(psys_factory.from_block, state.block_number + 1)

        # Run the scan
        c.scan(
            start_block=start_block,
            end_block="latest",
            num_safety_blocks=5,
            filter_=event_filter,
            chunk_size=2048,
            max_chunk_size=2048,
        )

        # FIXME: workaround to give all worker processes enough time to fully initialize in the case of a short scan
        time.sleep(5.0)

    return 0


if __name__ == "__main__":
    try:
        with pidfile.PIDFile("xquery.pid"):
            #sys.exit(main())
            while True:
                main()
                # Rerun the indexer every 30 secs to keep indexed db in sync w/ blockchain
                log.info("Restarting indexing after 60 secs...")
                time.sleep(60)
    except pidfile.AlreadyRunningError:
        print("Already running. Exiting.")
        sys.exit(1)
