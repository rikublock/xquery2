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

# Note: not officially exposed methods
from web3._utils.contracts import (
    encode_transaction_data,
    find_matching_fn_abi,
)
from web3._utils.abi import get_abi_output_types

import xquery.provider
import xquery.contract

from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)


# Pangolin (PNG) Token
ADDRESS = "0x60781C2586D68229fde47564546784ab3fACA982"


@timeit
def batch_fetch_token_batched() -> int:
    logging.basicConfig(level=logging.DEBUG, format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    api_url = C["API_URL"]["AVAX"]

    try:
        w3 = Web3(xquery.provider.BatchHTTPProvider(api_url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        log.error(e)
        return 1

    rc20 = xquery.contract.png_rc20

    # Note: None of these functions have any args
    fns = [
        "name",
        "symbol",
        "decimals"
    ]

    # Prepare batch request
    calls = []
    for i, fn_identifier in enumerate(fns):
        data = encode_transaction_data(
            w3,
            fn_identifier=fn_identifier,
            contract_abi=rc20.abi,
        )

        calls.append(xquery.provider.BatchHTTPProvider.build_entry(
            method=RPCEndpoint("eth_call"),
            params=[
                {
                    "to": Web3.toChecksumAddress(ADDRESS),
                    "data": data,
                },
                "latest",
            ],
            request_id=i,
        ))

    # make batch request
    result = w3.provider.make_batch_request(calls)
    log.debug(pprint.pformat(result))

    # decode result
    values = []
    for i, fn_identifier in enumerate(fns):
        assert result[i]["id"] == i

        fn_abi = find_matching_fn_abi(rc20.abi, w3.codec, fn_identifier)
        output_types = get_abi_output_types(fn_abi)

        result_data = bytearray.fromhex(result[i]["result"][2:])
        value = w3.codec.decode_abi(output_types, result_data)
        values.append(value[0])

    log.info(pprint.pformat(values))

    return 0


if __name__ == "__main__":
    sys.exit(batch_fetch_token_batched())
