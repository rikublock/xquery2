#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import logging
import pprint
import sys

from web3 import Web3
from web3.middleware import geth_poa_middleware
from web3.types import RPCEndpoint

import xquery.provider
from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)

# Note: On the public api node, the maximum number of items is currently 40 for batched requests!
# see https://docs.avax.network/apis/avalanchego/apis/c-chain


@timeit
def bench_get_block_batched(from_block: int = 1600000, num: int = 20) -> int:
    logging.basicConfig(level=logging.DEBUG, format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    api_url = C["API_URL"]["AVAX"]

    try:
        w3 = Web3(xquery.provider.BatchHTTPProvider(api_url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        log.error(e)
        return 1

    @timeit
    def get_block_batched(full_transactions: bool = False):
        calls = []
        for i in range(num):
            c = xquery.provider.BatchHTTPProvider.build_entry(
                method=RPCEndpoint("eth_getBlockByNumber"),
                params=[hex(from_block + i), full_transactions],
                request_id=i,
            )
            calls.append(c)

        result = w3.provider.make_batch_request(calls)
        log.debug(pprint.pformat(result))

    get_block_batched()

    return 0


if __name__ == "__main__":
    sys.exit(bench_get_block_batched())
