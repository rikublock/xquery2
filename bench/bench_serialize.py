#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import json
import logging
import pickle
import sys

import orjson
import ujson

from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)

# Note: Unfortunately, both ujson and orjson don't support integers that exceed 64-bit.


@timeit
def bench_serialize(num: int = 100000) -> int:
    logging.basicConfig(level=logging.DEBUG, format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    data = {
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

    data_dump_pickle = pickle.dumps(data)
    data_dump_json = json.dumps(data)
    data_dump_ujson = ujson.dumps(data)
    data_dump_orjson = orjson.dumps(data)

    @timeit
    def dump_pickle():
        for _ in range(num):
            pickle.dumps(data)

    @timeit
    def load_pickle():
        for _ in range(num):
            pickle.loads(data_dump_pickle)

    @timeit
    def dump_json():
        for _ in range(num):
            json.dumps(data)

    @timeit
    def load_json():
        for _ in range(num):
            json.loads(data_dump_json)

    @timeit
    def dump_ujson():
        for _ in range(num):
            ujson.dumps(data)

    @timeit
    def load_ujson():
        for _ in range(num):
            ujson.loads(data_dump_ujson)

    @timeit
    def dump_orjson():
        for _ in range(num):
            orjson.dumps(data)

    @timeit
    def load_orjson():
        for _ in range(num):
            orjson.loads(data_dump_orjson)

    dump_pickle()
    load_pickle()
    dump_json()
    load_json()
    dump_ujson()
    load_ujson()
    dump_orjson()
    load_orjson()

    return 0


if __name__ == "__main__":
    sys.exit(bench_serialize())
