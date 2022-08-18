#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    Dict,
)

import abc
import logging

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
    Note: Worker processes intentionally only have read access to the database.
    """

    @abc.abstractmethod
    def process(self, entry: ExtendedLogReceipt) -> Dict[str, Any]:
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
