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

import xquery.provider
import xquery.contract

from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)


# Pangolin (PNG) Token
ADDRESS = "0x60781C2586D68229fde47564546784ab3fACA982"


@timeit
def batch_fetch_token() -> int:
    logging.basicConfig(level=logging.DEBUG, format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    api_url = C["API_URL"]["AVAX"]

    try:
        w3 = Web3(Web3.HTTPProvider(api_url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        log.error(e)
        return 1

    rc20 = xquery.contract.png_rc20
    contract = w3.eth.contract(address=Web3.toChecksumAddress(ADDRESS), abi=rc20.abi)

    @timeit
    def get_name():
        name = contract.functions.name().call()
        log.info(name)

    @timeit
    def get_symbol():
        symbol = contract.functions.symbol().call()
        log.info(symbol)

    @timeit
    def get_decimals():
        decimals = contract.functions.decimals().call()
        log.info(decimals)

    get_name()
    get_symbol()
    get_decimals()

    return 0


if __name__ == "__main__":
    sys.exit(batch_fetch_token())
