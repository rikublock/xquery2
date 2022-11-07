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
    Tuple,
    Type,
    Union,
)

import enum
from dataclasses import dataclass

import xquery.db.orm as orm
from xquery.event.processor import ComputeInterval
from xquery.types import ExtendedLogReceipt


@dataclass
class DataBundle(object):
    """
    Used to efficiently package data for workers in a coherent way.
    The structure is used to transport data in both ``Job`` and ``JobResult``.

    Attributes:
        objects: data being transported between processes
        meta: optional metadata detailing additional properties of ``objects``
    """
    objects: List[Union[ExtendedLogReceipt, ComputeInterval, List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]]
    meta: Dict[str, Any] = None


class JobType(enum.Enum):
    Index = 0
    Process = enum.auto()


@dataclass
class Job(object):
    """
    Contains the necessary job data for a worker.

    Should contain bundled objects of type ``ExtendedLogReceipt`` and ``ComputeInterval``
    in case of an indexer and processor worker, respectively.

    Note: The unique job id is used to sort results and ensure all jobs have been processed.
    Note: All event log entries from one block are expected to be "bundled" together.
    Note: Objects are expected to be sorted in ascending order.

    Example:
        DataBundle(
            objects=[
                event1,
                event2,
                event3,
            ],
            meta={
                "state_name": "indexer",
                "block_number": 57347,
                "block_hash": "0x8ed42786cb8fa0aa8ef0121cfc50b7e23277d513b5f4486078141a9f540d982b",
            }
        )

    Attributes:
        id: unique, consecutive identifier
        type: job type
        data: bundled job data
    """
    id: int
    type: JobType
    data: List[DataBundle]

    def __repr__(self) -> str:
        return f"Job(id={self.id} bundles={len(self.data)} entries={sum(len(bundle.objects) for bundle in self.data)})"


@dataclass
class JobResult(object):
    """
    Contains the result data of a job that was fully processed by a worker.

    Should contain bundled objects of type ``List[orm.Base]`` or ``List[Tuple[Type[orm.Base], List[dict]]]``.

    Note: The unique result id has to always match the id of the corresponding job.
    Note: All event log entries from a block are expected to be "bundled" together.
    Note: A single event can generate a list (multiple) of orm objects.
    Note: Objects are expected to follow the same order found in the corresponding job.

    Example:
        DataBundle(
            objects=[
                [orm_obj1, orm_obj2],            # from event1
                [orm_obj3],                      # from event2
                [orm_obj4, orm_obj5, orm_obj6],  # from event3
            ],
            meta={
                "block_number": 57347,
                "block_hash": "0x8ed42786cb8fa0aa8ef0121cfc50b7e23277d513b5f4486078141a9f540d982b",
            }
        )

    Attributes:
        id: unique, consecutive identifier
        type: job type
        data: bundled result data
    """
    id: int
    type: JobType
    data: List[DataBundle]

    def __repr__(self) -> str:
        return f"JobResult(id={self.id} type={self.type})"
