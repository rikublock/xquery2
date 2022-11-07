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

import xquery.cache
import xquery.db
import xquery.db.orm as orm
from xquery.types import ExtendedLogReceipt

log = logging.getLogger(__name__)


class EventIndexer(abc.ABC):
    """
    Event indexer base class

    Responsible for:
    - extract static event log data
    - generate/fetch complementary data (e.g. block info, tx info, contract info, etc.)
    - prepare database orm objects (without committing)

    The goal of the indexing step is to collect all "external" data and move it to the database for processing.

    Note: This runs in a worker process.
    Note: Worker processes primarily have read access to the database, but may commit very distinct/specific data.
    """

    def __init__(self, w3: Web3, db: xquery.db.FusionSQL, cache: xquery.cache.Cache) -> None:
        """
        Create event indexer

        :param w3: web3 provider
        :param db: database service
        :param cache: cache service
        """
        self._w3 = w3
        self._db = db
        self._cache = cache

    def reset(self) -> None:
        """
        Reset the indexer after processing a job.

        :return:
        """
        pass

    @classmethod
    @abc.abstractmethod
    def setup(cls, w3: Web3, db: xquery.db.FusionSQL, start_block: int) -> List[orm.Base]:
        """
        Initialize the indexer before workers start processing events

        Subclasses are expected to:
        - perform global initializations

        Note: Should only be called once from the main thread

        :param w3: web3 provider
        :param db: database service
        :param start_block: the earliest block this indexer will ever process
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def process(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        """
        Index an event log entry.

        Subclasses are expected to:
        - implement the indexing part that maps event log data to orm objects
        - fetch and prepare additional data (e.g. block info, tx info, contract info, etc.)
        - add relationships between orm objects (load from db or cache if necessary)
        - return a list of database orm objects

        :param entry: event log entry
        :return:
        """
        raise NotImplementedError


class EventIndexerDummy(EventIndexer):
    """
    Dummy event indexer that does nothing. Used for debugging.
    """

    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def setup(cls, w3: Web3, db: xquery.db.FusionSQL, start_block: int) -> List[orm.Base]:
        return []

    def process(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        return []
