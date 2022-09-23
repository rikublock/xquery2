#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import logging
import sys

import xquery.cache
from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)


@timeit
def bench_cache_redis(num: int = 10000) -> int:
    logging.basicConfig(level=C["LOG_LEVEL"], format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    c = xquery.cache.Cache_Redis(
        host=C["REDIS_HOST"],
        port=C["REDIS_PORT"],
        password=C["REDIS_PASSWORD"],
        db=C["REDIS_DATABASE"],
    )

    # ensure the service is running
    c.ping()

    data_complex = {
        "address": "0xd7538cABBf8605BdE1f4901B47B8D42c61DE0367",
        "blockHash": "0x2544fe8d16e56008130750149d13552b1e85eab65c638bbba951b31bb506fa53",
        "blockNumber": 16955456,
        "data": "0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000017b600a58db2f12000000000000000000000000000000000000000000000002737fccbc55068e290000000000000000000000000000000000000000000000000000000000000000",
        "dataDecoded": {
            "amount0In": 0,
            "amount0Out": 45216083893,
            "amount1In": 1067846,
            "amount1Out": 0,
            "sender": "0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106",
            "to": "0xC33Ac18900b2f63DFb60B554B1F53Cd5b474d4cd"
        },
        "logIndex": 14,
        "name": "Swap",
        "removed": False,
        "topics": [
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
            "0x000000000000000000000000e54ca86531e17ef3616d22ca28b0d458b6c89106",
            "0x000000000000000000000000c33ac18900b2f63dfb60b554b1f53cd5b474d4cd"
        ],
        "transactionHash": "0x250f403ba38cc46bef098b8cbcd85e2af3b57db71e8603112419a66f006a21a2",
        "transactionIndex": 1
    }

    data_large = [(i, n) for i, n in enumerate(range(1000, 1000 ** 2))]

    @timeit
    def set_simple():
        for i in range(num):
            key = f"_test_simple_{i}"
            data = f"Test Data {i:06}"
            c.set(key, data)

    @timeit
    def get_simple():
        for i in range(num):
            key = f"_test_simple_{i}"
            data = c.get(key)
            assert data

    @timeit
    def set_complex():
        for i in range(num):
            key = f"_test_complex_{i}"
            c.set(key, data_complex)

    @timeit
    def get_complex():
        for i in range(num):
            key = f"_test_complex_{i}"
            data = c.get(key)
            assert data

    @timeit
    def set_complex_different():
        for i in range(num):
            key = f"_test_complex_diff_{i}"
            data_complex["name"] = f"Name {i:06}"
            c.set(key, data_complex)

    @timeit
    def get_complex_different():
        for i in range(num):
            key = f"_test_complex_diff_{i}"
            name = f"Name {i:06}"
            data = c.get(key)
            assert data and data["name"] == name

    @timeit
    def set_large():
        for i in range(10):
            key = f"_test_large_{i}"
            c.set(key, data_large)

    @timeit
    def get_large():
        for i in range(10):
            key = f"_test_large_{i}"
            data = c.get(key)
            assert data

    c.flush()
    set_simple()
    get_simple()

    c.flush()
    set_complex()
    get_complex()

    c.flush()
    set_complex_different()
    get_complex_different()

    c.flush()
    set_large()
    get_large()

    return 0


if __name__ == "__main__":
    sys.exit(bench_cache_redis())
