#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import List

import abc
import logging

from web3 import Web3
from web3.types import LogReceipt

# Currently these methods are not exposed over the official web3 API,
# but we need it to construct eth_getLogs parameters
from web3._utils.events import get_event_data
from web3._utils.filters import construct_event_topic_set

from xquery.types import ExtendedLogReceipt

log = logging.getLogger(__name__)

# Note: see https://docs.alchemy.com/alchemy/guides/eth_getlogs


class EventFilter(abc.ABC):
    """
    Event filter base class

    Responsible for:
    - generate/fetch filtered event logs for a range of blocks
    - decode potential event data
    - determine event name (based on abi)

    The goal of the filter step is to swiftly collect only necessary event log entries.
    """

    def __init__(self, w3: Web3, events: list) -> None:
        """
        Create filter and determine event topics.

        :param w3: web3 provider
        :param events: find and filter for these events
        """
        self.w3 = w3
        self.events = events

        # generate topics from events
        self._topics = []
        self._abis = {}
        self._event_names = {}
        for event in self.events:
            abi = event._get_event_abi()
            topic = construct_event_topic_set(
                event_abi=abi,
                abi_codec=self.w3.codec,
            )

            assert len(topic) == 1
            self._topics.extend(topic)
            self._abis[topic[0]] = abi
            self._event_names[topic[0]] = event.event_name

            log.debug(f"Event(name={event.event_name}, topic={topic[0]})")

    def _decode_event_data(self, logs: List[LogReceipt]) -> None:
        """
        Decode the event data and add a ``dataDecoded`` item to the ``AttributeDict``.

        TODO not very clean approach, should be refactored

        :param logs: fetched event log entries
        :return:
        """
        for entry in logs:
            topic = entry.topics[0].hex()
            data = get_event_data(self.w3.codec, self._abis[topic], entry)
            entry.__dict__["dataDecoded"] = data.args

    def _add_event_name(self, logs: List[LogReceipt]) -> None:
        """
        Add the event ``name`` to the ``AttributeDict``.

        TODO not very clean approach, should be refactored

        :param logs: fetched event log entries
        :return:
        """
        for entry in logs:
            topic = entry.topics[0].hex()
            entry.__dict__["name"] = self._event_names[topic]

    @abc.abstractmethod
    def get_logs(self, from_block: int, chunk_size: int) -> List[ExtendedLogReceipt]:
        """
        Retrieve all relevant event logs for ``chunk_size`` blocks starting from ``from_block`` and
        filter them according to predefined rules.

        Subclasses are expected to:
        - implement the generator and filter (best based on eth_getlogs)
        - only return filtered events
        - only return sorted events (blockNumber, logIndex)
        - populate the ``dataDecoded`` and ``name`` keys to return an 'extended' event log

        Example basic log entry:
        {
            'address': '0xd7538cABBf8605BdE1f4901B47B8D42c61DE0367',
            'blockHash': '0x2544fe8d16e56008130750149d13552b1e85eab65c638bbba951b31bb506fa53',
            'blockNumber': 16955456,
            'data': '0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000017b600a58db2f12000000000000000000000000000000000000000000000002737fccbc55068e290000000000000000000000000000000000000000000000000000000000000000',
            'dataDecoded': {
                'amount0In': 0,
                'amount0Out': 45216083893075480105,
                'amount1In': 106784613730037522,
                'amount1Out': 0,
                'sender': '0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106',
                'to': '0xC33Ac18900b2f63DFb60B554B1F53Cd5b474d4cd'
            },
            'logIndex': 14,
            'name': 'Swap',
            'removed': False,
            'topics': [
                '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
                '0x000000000000000000000000e54ca86531e17ef3616d22ca28b0d458b6c89106',
                '0x000000000000000000000000c33ac18900b2f63dfb60b554b1f53cd5b474d4cd'
            ],
            'transactionHash': '0x250f403ba38cc46bef098b8cbcd85e2af3b57db71e8603112419a66f006a21a2',
            'transactionIndex': 1
        }

        :param from_block: starting height
        :param chunk_size: number of blocks that should be requested at once
        :return:
        """
        raise NotImplementedError
