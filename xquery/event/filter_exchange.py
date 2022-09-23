#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    List,
    Set,
)

import logging
import operator

from web3 import Web3
from web3.contract import Contract
from web3.datastructures import AttributeDict
from web3.types import LogReceipt

# Currently this method is not exposed over official web3 API,
# but we need it to construct eth_getLogs parameters
from web3._utils.events import get_event_data
from web3._utils.filters import construct_event_topic_set

import xquery.contract
import xquery.db.orm as orm
from xquery.types import ExtendedLogReceipt
from xquery.util import convert

from .filter import EventFilter

log = logging.getLogger(__name__)


class EventFilterExchange(EventFilter):

    def __init__(self, w3: Web3, contract_factory: Contract, addresses_pair: Set[str], events: list) -> None:
        """
        Event filter for Uniswap like exchanges.

        :param w3: web3 provider
        :param contract_factory: exchange factory contract
        :param addresses_pair: pair contract checksum addresses associated with the factory
        :param events: filter for these events
        """
        super().__init__(w3, events + [contract_factory.events.PairCreated])

        self._contract_factory = contract_factory
        self._addresses_pair = set(addresses_pair)

        # factory contract topics
        # topic: 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
        self._abi_pair_created = self._contract_factory.events.PairCreated._get_event_abi()
        self._topic_pair_created = construct_event_topic_set(
            event_abi=self._abi_pair_created,
            abi_codec=self.w3.codec,
        )

        # pair contract topics
        self._topics_pair = []
        for event in events:
            abi = event._get_event_abi()
            topic = construct_event_topic_set(
                event_abi=abi,
                abi_codec=self.w3.codec,
            )
            assert len(topic) == 1
            self._topics_pair.extend(topic)

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

    def _get_logs(self, from_block: int, to_block: int) -> List[LogReceipt]:
        assert from_block <= to_block

        # Note: trim duplicated log entries by making use of set()
        logs = set()

        # Look for newly created pairs and start tracking them
        # Note: the PairCreated event will be processed by the indexer and will add an entry
        # to the database that can be loaded at start up.
        entries = self.w3.eth.get_logs({
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": self._contract_factory.address,
            "topics": [
                self._topic_pair_created,
            ],
        })
        logs.update(self.__class__._fix_attrdict(entries))

        for entry in entries:
            data = get_event_data(
                abi_codec=self.w3.codec,
                event_abi=self._abi_pair_created,
                log_entry=entry,
            )
            address_pair = Web3.toChecksumAddress(data.args.pair)
            log.info(f"Found new pair contract address '{address_pair}'")
            self._addresses_pair.add(address_pair)

        # Pair contract events
        if len(self._addresses_pair) > 0:
            entries = self.w3.eth.get_logs({
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": self._addresses_pair,
                "topics": [
                    self._topics_pair,
                ],
            })
            logs.update(self.__class__._fix_attrdict(entries))

        return sorted(logs, key=operator.itemgetter("blockNumber", "logIndex"))  # type: ignore

    def get_logs(self, from_block: int, chunk_size: int) -> List[ExtendedLogReceipt]:
        assert chunk_size > 0
        logs = self._get_logs(from_block, from_block + chunk_size - 1)
        self._decode_event_data(logs)
        self._add_event_name(logs)
        return logs  # type: ignore


class EventFilterExchangePangolin(EventFilterExchange):

    def __init__(self, w3: Web3, pair_addresses: Set[str]) -> None:
        """
        Event filter for the Pangolin Exchange (on AVAX)
        """
        # TODO
        # assert w3.eth.chain_id == int(orm.Chain.AVAX)

        png_factory = xquery.contract.png_factory
        png_pair = xquery.contract.png_pair
        contract_factory = w3.eth.contract(address=Web3.toChecksumAddress(png_factory.address), abi=png_factory.abi)
        contract_pair = w3.eth.contract(abi=png_pair.abi)

        super().__init__(
            w3=w3,
            contract_factory=contract_factory,
            addresses_pair=pair_addresses,
            events=[
                contract_pair.events.Transfer,  # 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
                contract_pair.events.Burn,   # 0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496
                contract_pair.events.Mint,  # 0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f
                contract_pair.events.Swap,  # 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
                contract_pair.events.Sync,  # 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1
            ],
        )


class EventFilterExchangePegasys(EventFilterExchange):

    def __init__(self, w3: Web3, pair_addresses: Set[str]) -> None:
        """
        Event filter for the Pegasys Exchange (on SYS)
        """
        # TODO
        # assert w3.eth.chain_id == int(orm.Chain.SYS)

        psys_factory = xquery.contract.psys_factory
        psys_pair = xquery.contract.psys_pair
        contract_factory = w3.eth.contract(address=Web3.toChecksumAddress(psys_factory.address), abi=psys_factory.abi)
        contract_pair = w3.eth.contract(abi=psys_pair.abi)

        super().__init__(
            w3=w3,
            contract_factory=contract_factory,
            addresses_pair=pair_addresses,
            events=[
                contract_pair.events.Transfer,  # 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
                contract_pair.events.Burn,   # 0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496
                contract_pair.events.Mint,  # 0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f
                contract_pair.events.Swap,  # 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
                contract_pair.events.Sync,  # 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1
            ],
        )
