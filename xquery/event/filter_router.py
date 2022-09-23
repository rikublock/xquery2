#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import List

import logging
import operator

from eth_utils import add_0x_prefix

from web3 import Web3
from web3.contract import Contract
from web3.datastructures import AttributeDict
from web3.types import LogReceipt

import xquery.contract
from xquery.types import ExtendedLogReceipt
from xquery.util import convert
from .filter import EventFilter

log = logging.getLogger(__name__)


class EventFilterRouter(EventFilter):

    def __init__(self, w3: Web3, contract: Contract, events: list) -> None:
        """
        Deprecated filter used only in classic XQuery.

        :param w3: web3 provider
        :param contract: router contract
        :param events: find and filter for these events
        """
        super().__init__(w3, events)

        self._contract = contract

        # encode router address
        value = self.w3.codec.encode_single(typ="address", arg=self._contract.address)
        self._address = add_0x_prefix(value.hex())

    @staticmethod
    def _fix_attrdict(attrdicts: List[AttributeDict]) -> List[AttributeDict]:
        """
        Recursively replace tuples with lists in ``AttributDict`` objects.

        Note: There is currently a bug in web3.py which makes ``AttributDict`` objects un-hashable.
        This can be removed once the library is fixed.
        """
        for a in attrdicts:
            a.__dict__ = convert(a.__dict__)
        return attrdicts

    def _get_logs(self, from_block: int, chunk_size: int) -> List[LogReceipt]:
        assert chunk_size > 0

        # Note: trim duplicated log entries by using sets
        logs = set()

        entries = self.w3.eth.get_logs({
            "fromBlock": hex(from_block),
            "toBlock": hex(from_block + chunk_size - 1),
            "topics": [
                self._topics,
                self._address,
            ],
        })
        logs.update(self.__class__._fix_attrdict(entries))

        entries = self.w3.eth.get_logs({
            "fromBlock": hex(from_block),
            "toBlock": hex(from_block + chunk_size - 1),
            "topics": [
                self._topics,
                None,
                self._address,
            ],
        })
        logs.update(self.__class__._fix_attrdict(entries))

        return sorted(logs, key=operator.itemgetter("blockNumber", "logIndex"))  # type: ignore

    def get_logs(self, from_block: int, chunk_size: int) -> List[ExtendedLogReceipt]:
        logs = self._get_logs(from_block, chunk_size)
        self._decode_event_data(logs)
        self._add_event_name(logs)
        return logs  # type: ignore


class EventFilterRouterPangolin(EventFilterRouter):

    def __init__(self, w3: Web3) -> None:
        png_router = xquery.contract.png_router
        png_pair = xquery.contract.png_pair
        png_rc20 = xquery.contract.png_rc20
        wavax = xquery.contract.wavax

        contract_router = w3.eth.contract(address=Web3.toChecksumAddress(png_router.address), abi=png_router.abi)
        contract_pair = w3.eth.contract(abi=png_pair.abi)
        contract_rc20 = w3.eth.contract(abi=png_rc20.abi)
        contract_wavax = w3.eth.contract(address=Web3.toChecksumAddress(wavax.address), abi=wavax.abi)

        super().__init__(
            w3=w3,
            contract=contract_router,
            events=[
                contract_rc20.events.Approval,  # 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925
                contract_rc20.events.Transfer,  # 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
                contract_pair.events.Burn,  # 0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496
                contract_pair.events.Mint,  # 0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f
                contract_pair.events.Swap,  # 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
                contract_pair.events.Sync,  # 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1
                contract_wavax.events.Deposit,  # 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c
                contract_wavax.events.Withdrawal,  # 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65
            ],
        )
