#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import Any

import hashlib
import itertools
import json
import logging
import time

from eth_utils import add_0x_prefix

from web3 import Web3
from web3.types import LogReceipt

log = logging.getLogger(__name__)


def timeit(func: callable) -> callable:
    """
    Decorator for measuring a function's running time

    :param func: function
    :return:
    """
    def measure_time(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        log.info(f"Processing time of '{func.__qualname__}()': {elapsed:.4f} seconds.")
        return result

    return measure_time


def bundled(a: list, key: callable = lambda x: x) -> list:
    """
    Group consecutive list elements with the same key

    Note: the source list needs be sorted on the same key function

    Example:
    [1, 1, 2, 3, 3] -> [[1, 1], [2], [3, 3]]

    :param a: source list
    :param key: function to extract comparison key
    :return:
    """
    return [list(g) for k, g in itertools.groupby(a, key=key)]


def batched(a: list, size: int = 8) -> list:
    """
    Yield successive evenly-sized chunks from a list

    :param a: source list
    :param size: chunk size
    :return:
    """
    for i in range(0, len(a), size):
        yield a[i:i + size]


def convert(value: Any) -> Any:
    """
    Recursively replace lists with tuples

    :param value: source object
    :return:
    """
    if isinstance(value, (list, tuple)):
        return tuple(convert(x) for x in value)
    elif isinstance(value, dict):
        return dict((k, convert(v)) for k, v in value.items())
    else:
        return value


def compute_xhash(entry: LogReceipt) -> str:
    """
    Compute the sha256 hash of an event log entry in order to create a unique identifier.

    Example of hashed data:
    {
        'address': '0xd7538cABBf8605BdE1f4901B47B8D42c61DE0367',
        'blockHash': '0x2544fe8d16e56008130750149d13552b1e85eab65c638bbba951b31bb506fa53',
        'logIndex': 14,
        'transactionHash': '0x250f403ba38cc46bef098b8cbcd85e2af3b57db71e8603112419a66f006a21a2',
    }

    Result:
    0x7ceefa1adb70dd9145753925d66d980e212a2f40de6a46ab40986363049d4dff

    :param entry: event log entry
    :return:
    """
    keys = ["address", "blockHash", "logIndex", "transactionHash"]
    data = json.loads(Web3.toJSON(entry))
    data = {k: v for k, v in data.items() if k in keys}

    m = hashlib.sha256()
    m.update(json.dumps(data, sort_keys=True, ensure_ascii=True).encode("utf-8"))
    return add_0x_prefix(m.hexdigest())
