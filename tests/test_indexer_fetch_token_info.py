#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from web3 import Web3

import xquery.cache
import xquery.contract
from xquery.event import EventIndexerExchangePangolin


# TODO add token symbol bytes32
# see https://etherscan.io/address/0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2

def test_indexer_fetch_token_info(w3: Web3) -> None:
    indexer = EventIndexerExchangePangolin(
        w3=w3,
        db=None,
        cache=xquery.cache.Cache_Dummy(),
    )

    addresses = [
        "0x60781C2586D68229fde47564546784ab3fACA982",  # PNG
        "0xc7198437980c041c805A1EDcbA50c1Ce5db95118",  # USDT.e
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",  # WAVAX
        "0x6Fe55D097FC9C1d08B64f4b1c94ac9453B1c9abB",  # bad
        "0xd00ae08403B9bbb9124bB305C09058E32C39A48c",  # bad
    ]

    # Note: the total supply can easily change, skip when comparing
    results = [
        ("PNG", "Pangolin", 18),
        ("USDT.e", "Tether USD", 6),
        ("WAVAX", "Wrapped AVAX", 18),
        ("unknown", "unknown", 0),
        ("unknown", "unknown", 0),
    ]

    for i, address in enumerate(addresses):
        result = indexer._fetch_token_info(address)
        assert result[:3] == results[i]
