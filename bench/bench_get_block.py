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

from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)


@timeit
def bench_get_block(from_block: int = 1600000, num: int = 20) -> int:
    logging.basicConfig(level=logging.DEBUG, format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    api_url = C["API_URL"]["AVAX"]

    try:
        w3 = Web3(Web3.HTTPProvider(api_url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        log.error(e)
        return 1

    @timeit
    def get_block(index: int):
        result = w3.eth.get_block(from_block + index)
        log.debug(pprint.pformat(result))

    for i in range(num):
        get_block(i)

    return 0


if __name__ == "__main__":
    sys.exit(bench_get_block())
