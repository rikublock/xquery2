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
    Union,
)

from dataclasses import dataclass

import xquery.db.orm as orm
from xquery.types import ExtendedLogReceipt


@dataclass
class DataBundle(object):
    """
    Used to efficiently package data for indexer workers in a coherent way.
    The structure is used transport data in ``Job`` and ``JobResult``.

    Attributes:
        objects: data being transported between processes
        meta: optional metadata detailing additional properties of ``objects``
    """
    objects: List[Union[ExtendedLogReceipt, List[orm.BaseModel]]]
    meta: Dict[str, Any] = None


@dataclass
class Job(object):
    """
    Contains the necessary job data for an indexer worker.

    Should contain bundled objects of type ``ExtendedLogReceipt``.

    Note: The unique job id is used to sort results and ensure all jobs have been processed.
    Note: All event log entries from one block are expected to be "bundled" together.

    Example:
        DataBundle(
            objects=[
                event1,
                event2,
                event3,
            ],
            meta={
                "block_number": 57347,
                "block_hash": "0x8ed42786cb8fa0aa8ef0121cfc50b7e23277d513b5f4486078141a9f540d982b",
            }
        )

    Attributes:
        id: unique, consecutive identifier
        data: bundled job data
    """
    id: int
    data: List[DataBundle]

    def __repr__(self) -> str:
        return f"Job(id={self.id} bundles={len(self.data)} entries={sum(len(bundle.objects) for bundle in self.data)})"


@dataclass
class JobResult(object):
    """
    Contains the result data of a job from an indexer worker.

    Should contain bundled objects of type ``List[orm.BaseModel]``.

    Note: The unique result id has to always match the id from the corresponding job.
    Note: All event log entries from a block are expected to be "bundled" together.
    Note: A single event can generate a list (multiple) of orm objects.

    Example:
        DataBundle(
            objects=[
                [orm_obj1, orm_obj2],  # from event1
                [orm_obj3],
                [orm_obj4, orm_obj5, orm_obj6],
            ],
            meta={
                "block_number": 57347,
                "block_hash": "0x8ed42786cb8fa0aa8ef0121cfc50b7e23277d513b5f4486078141a9f540d982b",
            }
        )

    Attributes:
        id: unique, consecutive identifier
        data: bundled result data
    """
    id: int
    data: List[DataBundle]
