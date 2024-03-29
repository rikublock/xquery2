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

import xquery.db.orm as orm
from xquery.cache import Cache
from xquery.db import FusionSQL

log = logging.getLogger(__name__)


class EventProcessorStage(abc.ABC):
    """
    Event processor stage base class

    Responsible for:
    - post-process previously indexed event data already present in the database
    - compute and aggregate complex data from database entries
    - prepare/update database orm objects

    Note: This runs in a worker process.
    Note: Worker processes primarily have read access to the database, but may commit very distinct/specific data.
    """

    def __init__(self, db: FusionSQL, cache: Cache) -> None:
        """
        Create event processor stage

        :param db: database service
        :param cache: cache service
        """
        self._db = db
        self._cache = cache

    @classmethod
    @abc.abstractmethod
    def setup(cls, db: FusionSQL, first_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        """
        One-time setup/configuration of a stage

        Subclasses are expected to:
        - perform any initial stage configuration

        Note: Runs on the main thread (main process)
        Note: Should be called exactly once before first running a stage

        :param db: database service
        :param first_block: the earliest block this stage will ever process
        :return:
        """
        raise NotImplementedError

    @classmethod
    def pre_process(cls, db: FusionSQL, cache: Cache, start_block: int, end_block: int) -> None:
        """
        Initialize the stage before workers start processing data

        Subclasses are expected to:
        - perform any global stage initializations
        - prepare global cache

        Note: Runs on the main thread (main process)
        Note: Should be called exactly once per run before processing compute intervals
        Note: Should not make any changes to the database (read only)

        :param db: database service
        :param cache: cache service
        :param start_block: first block (start of interval)
        :param end_block: last block (included in the computation)
        :return:
        """
        pass

    @abc.abstractmethod
    def process(self, start_block: int, end_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        """
        Compute data for a given interval of blocks.

        Subclasses are expected to:
        - implement the processing part that calculates/computes/aggregates complex data from existing orm objects
        - load and filter orm objects from the database required for the computation
        - return a list of new/updated database orm objects

        Note: Runs in a worker process

        :param start_block: first block (start of interval)
        :param end_block: last block (included in the computation)
        :return:
        """
        raise NotImplementedError

    @classmethod
    def post_process(cls, db: FusionSQL, cache: Cache, first_block: int, end_block: int, state: orm.State) -> None:
        """
        Finalize the stage after all workers have completed processing data

        Subclasses are expected to:
        - do any sequential bulk orm updates (bypass DBHandler)
        - perform any global stage finalizations
        - update the stage ``finalized`` field

        Note: Runs on the main thread (main process)
        Note: Should be called exactly once per run after processing compute intervals
        Note: Can adjust the ``first_block`` internally depending on the state

        :param db: database service
        :param cache: cache service
        :param first_block: the earliest block this stage will ever process
        :param end_block: last block (included in the computation)
        :param state: database state associated with this stage
        :return:
        """
        with db.session() as session:
            state.finalized = end_block
            assert state.block_number >= state.finalized

            session.merge(state, load=True)
            session.commit()


class EventProcessorStageDummy(EventProcessorStage):
    """
    Dummy event processor stage that does nothing. Used for debugging.
    """

    def __init__(self, *args, **kwargs) -> None:
        pass

    @classmethod
    def setup(cls, db: FusionSQL, first_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
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
