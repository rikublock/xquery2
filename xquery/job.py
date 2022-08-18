#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    Dict,
    List,
)

import multiprocessing as mp
from dataclasses import dataclass

from xquery.types import ExtendedLogReceipt


@dataclass
class Job(object):
    """
    The unique job id is used to sort results and ensure all jobs have been processed.

    Note: all event logs from one block are expected to be "packaged" together
    """
    id: int
    entries: List[List[ExtendedLogReceipt]]
    canceled: mp.Event = None

    def __repr__(self) -> str:
        return f"Job(id={self.id} bundles={len(self.entries)} entries={sum([len(package) for package in self.entries])})"


@dataclass
class JobResult(object):
    """
    The unique result id has to always match the id from the corresponding job.

    Note: all event logs from one block are expected to be "packaged" together
    """
    id: int
    results: List[List[Dict[str, Any]]]
