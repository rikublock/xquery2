#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import Set

import json
import logging
import pytest

from pathlib import Path

from web3 import Web3

from xquery.event import EventFilterExchangePangolin
from xquery.util.misc import convert

from .load import load_logs

log = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def addresses_pair() -> Set[str]:
    with open("tests/data/AVAX_Pangolin_pairs.json", "r") as f:
        data = json.load(f)
    return set(data["pairs"])


def test_filter_empty(w3: Web3) -> None:
    """
    Empty case (valid blocks, but no pairs that are being tracked)
    """
    filter_ = EventFilterExchangePangolin(
        w3=w3,
        pair_addresses=set(),
    )

    logs = filter_.get_logs(
        from_block=64611,
        chunk_size=2048,
    )

    assert len(logs) == 0


def test_filter_other_exchange(w3: Web3, addresses_pair: Set[str]) -> None:
    """
    Ensure we are not picking up events from other exchanges (e.g. Yetiswap) even though
    function signatures (topics) might match.

    Example:
      - https://snowtrace.io/tx/0x8a4f1bc44754e48c5c88d5c80526e97b5a9c42e7b4991d7613d9b52a40c58fbd#eventlog
    """
    filter_ = EventFilterExchangePangolin(
        w3=w3,
        pair_addresses=addresses_pair,
    )

    logs = filter_.get_logs(
        from_block=185402,
        chunk_size=1,
    )

    assert len(logs) == 0


def test_filter_all_events(w3: Web3, addresses_pair: Set[str]) -> None:
    """
    Test each of the exchange events
    """
    filter_ = EventFilterExchangePangolin(
        w3=w3,
        pair_addresses=addresses_pair,
    )

    cases = [
        (61499, None),  # PairCreated
        (65299, None),  # Burn
        (64638, None),  # Mint
        (65267, None),  # Swap
        (18787806, None)  # Multiple
    ]

    for block, txids in cases:
        logs = filter_.get_logs(
            from_block=block,
            chunk_size=1,
        )

        logs_file = load_logs(file=Path(f"tests/data/AVAX_{block}_filtered.json"), txids=txids)

        # TODO remove once web3 lib is fixed
        # fix attribute dicts
        for entry in logs_file:
            entry.__dict__ = convert(entry.__dict__)

        assert len(logs) == len(logs_file)
        assert logs == logs_file
