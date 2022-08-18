#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import List

import json
import logging
import operator
import pprint
import sys

from eth_utils import add_0x_prefix

from web3 import Web3
from web3.middleware import geth_poa_middleware
from web3.datastructures import AttributeDict

# Currently this method is not exposed over official web3 API,
# but we need it to construct eth_getLogs parameters
from web3._utils.filters import construct_event_topic_set
from web3._utils.events import get_event_data

import xquery.contract

from xquery.config import CONFIG as C
from xquery.util.misc import (
    convert,
    timeit,
)

log = logging.getLogger(__name__)

# Note: The event filtering is contract specific
# Note: The same filter should be used when grabbing recent blocks via `eth_getFilterChanges()`

# Note: Regarding filtering in `get_logs()`
# For more details see: https://docs.alchemy.com/alchemy/apis/ethereum/eth-newfilter
# Generally, we would like to apply a strict filter to `get_logs()` in order to receive a list of
# only relevant events (server side filtering). This will drastically speed up the retrieval of data.


# Note: There is currently a bug in web3.py which makes AttributDict objects un-hashable
# This can be removed once the library is fixed
def fix(attrdicts: List[AttributeDict]) -> List[AttributeDict]:
    for a in attrdicts:
        a.__dict__ = convert(a.__dict__)
    return attrdicts


@timeit
def bench_get_logs(from_block: int = 16955420, chunk_size: int = 2048, chunks: int = 5) -> int:
    logging.basicConfig(level=logging.DEBUG, format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    api_url = C["API_URL"]["AVAX"]

    w3 = Web3(Web3.HTTPProvider(api_url))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

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
        contract_rc20.events.Approval,
        contract_rc20.events.Transfer,
        contract_pair.events.Burn,
        contract_pair.events.Mint,
        contract_pair.events.Swap,
        contract_pair.events.Sync,
        contract_wavax.events.Deposit,
        contract_wavax.events.Withdrawal,
    ]

    # generate topics from events
    topics = []
    abis = {}
    for event in events:
        abi = event._get_event_abi()
        topic = construct_event_topic_set(
            event_abi=abi,
            abi_codec=w3.codec,
        )

        assert len(topic) == 1
        topics.extend(topic)
        abis[topic[0]] = abi

    # encode router address
    value = w3.codec.encode_single(typ="address", arg=contract_router.address)
    address = add_0x_prefix(value.hex())

    # Note: not all events will be captured by this filter
    @timeit
    def get_logs(i: int) -> list:
        logs = set()

        entries = w3.eth.get_logs({
            "fromBlock": hex(from_block + chunk_size * i),
            "toBlock": hex(from_block + chunk_size * (i + 1)),
            "topics": [
                topics,
                address,
            ],
        })
        logs.update(fix(entries))

        entries = w3.eth.get_logs({
            "fromBlock": hex(from_block + chunk_size * i),
            "toBlock": hex(from_block + chunk_size * (i + 1)),
            "topics": [
                topics,
                None,
                address,
            ],
        })
        logs.update(fix(entries))

        return sorted(logs, key=operator.itemgetter("blockNumber", "logIndex"))

    logs = []
    for i in range(chunks):
        logs.extend(get_logs(i))

    # get event data
    for l in logs:
        log.debug(pprint.pformat(json.loads(Web3.toJSON(l))))
        topic = l.topics[0].hex()
        data = get_event_data(w3.codec, abis[topic], l)
        log.debug(pprint.pformat(json.loads(Web3.toJSON(data.args))))

    log.info(f"Processed {chunk_size * chunks} blocks")

    return 0


if __name__ == "__main__":
    sys.exit(bench_get_logs())
