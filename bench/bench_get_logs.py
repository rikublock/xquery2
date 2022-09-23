#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import json
import logging
import sys

from web3 import Web3
from web3.middleware import geth_poa_middleware

from xquery.event import (
    EventFilterExchangePangolin,
    EventFilterRouterPangolin,
)
from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)

# Note: The event filtering is contract specific
# Note: The same filter should be used when grabbing recent blocks via `eth_getFilterChanges()`

# Note: Regarding filtering in `get_logs()`
# For more details see: https://docs.alchemy.com/alchemy/apis/ethereum/eth-newfilter
# Generally, we would like to apply a strict filter to `get_logs()` in order to receive a list of
# only relevant events (server side filtering). This will drastically speed up the retrieval of data.


@timeit
def bench_get_logs_router(w3: Web3, from_block: int, chunk_size: int, chunks: int) -> None:
    filter_ = EventFilterRouterPangolin(
        w3=w3,
    )

    logs = []
    for i in range(chunks):
        logs.extend(filter_.get_logs(from_block + i * chunk_size, chunk_size))

    log.info(f"Processed {chunk_size * chunks} blocks")


@timeit
def bench_get_logs_factory(w3: Web3, from_block: int, chunk_size: int, chunks: int) -> None:
    # all Pangolin pairs up to block 212161
    pair_addresses = [
        "0xa37cd29A87975f44b83F06F9BA4D51879a99d378",
        "0x1aCf1583bEBdCA21C8025E172D8E8f2817343d65",
        "0x9EE0a4E21bd333a6bb2ab298194320b8DaA26516",
        "0x7a6131110B82dAcBb5872C7D352BfE071eA6A17C",
        "0xbbC7fFF833D27264AaC8806389E02F717A5506c9",
        "0x17a2E8275792b4616bEFb02EB9AE699aa0DCb94b",
        "0x92dC558cB9f8d0473391283EaD77b79b416877cA",
        "0xd8B262C0676E13100B33590F10564b46eeF652AD",
        "0x5F233A14e1315955f48C5750083D9A44b0DF8B50",
        "0x7A886B5b2F24eD0Ec0B3C4a17b930E16d160BD17",
        "0xd7538cABBf8605BdE1f4901B47B8D42c61DE0367",
        "0xe6C5e55c12de2e59eBB5f9b0A19bC3FD71500Db3",
        "0x359059Bdbf2B9DCc534D20912D3e82Df2111B620",
        "0x27Eef94E479CB4774B050530cFc45E4A6ccc7E5F",
        "0x18C8E1346D26824063706242AdB391DDB16C293E",
        "0x795ab2504C01426a31C8bD9F58c024dBa86bA80a",
        "0x862C96397fe2C80F4011e90029e1eDeBc8206605",
        "0xC005F8320Dc4cD5Ba32Aa441B708C83Eef8f64e9",
        "0x757c99FCD02da951582b47146F7bD75ae11f6F43",
    ]

    filter_ = EventFilterExchangePangolin(
        w3=w3,
        pair_addresses=set(pair_addresses),
    )

    logs = []
    for i in range(chunks):
        logs.extend(filter_.get_logs(from_block + i * chunk_size, chunk_size))

    log.info(f"Processed {chunk_size * chunks} blocks")


@timeit
def bench_get_logs_factory_all(w3: Web3, from_block: int, chunk_size: int, chunks: int) -> None:
    with open("tests/data/AVAX_Pangolin_pairs.json", "r") as f:
        data = json.load(f)

    filter_ = EventFilterExchangePangolin(
        w3=w3,
        pair_addresses=set(data["pairs"]),
    )

    logs = []
    for i in range(chunks):
        logs.extend(filter_.get_logs(from_block + i * chunk_size, chunk_size))

    log.info(f"Processed {chunk_size * chunks} blocks")


@timeit
def main() -> int:
    logging.basicConfig(level=C["LOG_LEVEL"], format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    w3 = Web3(Web3.HTTPProvider(endpoint_uri=C["API_URL"], request_kwargs={"timeout": 100}))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    from_block = 185400
    chunk_size = 2048
    chunks = 5

    bench_get_logs_router(w3, from_block, chunk_size, chunks)
    bench_get_logs_factory(w3, from_block, chunk_size, chunks)
    bench_get_logs_factory_all(w3, from_block, chunk_size, chunks)

    return 0


if __name__ == "__main__":
    sys.exit(main())
