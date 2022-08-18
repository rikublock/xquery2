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

from xquery.types import ExtendedLogReceipt
from xquery.util.misc import convert
from .filter import EventFilter

log = logging.getLogger(__name__)


class EventFilter_Router(EventFilter):

    def __init__(self, w3: Web3, contract: Contract, events: list) -> None:
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


class EventFilter_Pangolin(EventFilter_Router):
    pass
