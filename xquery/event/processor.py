#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import abc
import logging

from dataclasses import dataclass

import xquery.cache
import xquery.db
import xquery.db.orm as orm

log = logging.getLogger(__name__)


class EventProcessorStage(abc.ABC):
    """
    Event processor stage base class

    Responsible for:
    - post-process previously indexed event data already present in the database
    - compute and aggregate complex data from database entries
    - prepare/update database orm objects (without committing)

    Note: This runs in a worker process.
    Note: Worker processes primarily have read access to the database, but may commit very distinct/specific data.
    """

    def __init__(self, db: xquery.db.FusionSQL, cache: xquery.cache.Cache) -> None:
        """
        Create event processor stage

        :param db: database service
        :param cache: cache service
        """
        self._db = db
        self._cache = cache

    @classmethod
    @abc.abstractmethod
    def setup(cls, db: xquery.db.FusionSQL, start_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        """
        Initialize the stage before workers start processing data

        Subclasses are expected to:
        - perform global initializations

        Note: Should only be called once from the main thread

        :param db: database service
        :param start_block: the earliest block this stage will ever process
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def process(self, start_block: int, end_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        """
        Compute data for a given interval of blocks.

        Subclasses are expected to:
        - implement the processing part that calculates/computes/aggregates complex data from existing orm objects
        - load and filter orm objects from the database required for the computation
        - return a list of new/updated database orm objects

        :param start_block: first block (start of interval)
        :param end_block: last block (included in the computation)
        :return:
        """
        raise NotImplementedError


class EventProcessorStageDummy(EventProcessorStage):
    """
    Dummy event processor stage that does nothing. Used for debugging.
    """

    def __init__(self, *args, **kwargs) -> None:
        pass

    @classmethod
    def setup(cls, db: xquery.db.FusionSQL, start_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        return []

    def process(self, start_block: int, end_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        return []


@dataclass
class StageInfo(object):
    """
    Used to configure the stages of an event processor.

    Attributes:
        name: unique identifier within a processor (primarily used to load the corresponding db state)
        cls: event processor stage class
        batch_size: number of blocks per job or ``None`` if everything should be packed into a single job
    """
    name: str
    cls: Type[EventProcessorStage]
    batch_size: Optional[int] = None


@dataclass
class ComputeInterval(object):
    """
    Used to specify an integer interval (range) of blocks that should be processed by a processor stage.

    Attributes:
        stage_cls: event processor stage class
        a: start value (included in the interval)
        b: end value (included in the interval)
    """
    stage_cls: Type[EventProcessorStage]
    a: int
    b: int


class EventProcessor(abc.ABC):
    """
    Event processor base class

    Responsible for:
    - collection of stages that incorporate the real functionality
    - ensure that everything being computed has no dependencies outside of the compute interval.
    - ensure that job sizes are adequate and that only a reasonable number of orm objects is being returned

    The goal of the processing step is calculate/aggregate supplementary information from indexed data.
    """

    def __init__(self, stages: List[StageInfo]) -> None:
        """
        Acts as an iterator for processor stages

        :param stages: ordered list of processor stages
        """
        self._stages = list(stages)

    def __iter__(self):
        for stage in self._stages:
            yield stage


class EventProcessorDummy(EventProcessor):
    """
    Dummy event processor that does nothing. Used for debugging.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__([])
