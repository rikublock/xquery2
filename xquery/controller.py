#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.
import pprint
from typing import Type

import enum
import logging
import multiprocessing as mp
import operator
import os
import queue
import threading

from sqlalchemy import select

from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.types import BlockIdentifier

import xquery.db.orm as orm

from xquery.event.filter import EventFilter
from xquery.event.indexer import EventIndexer
from xquery.job import Job
from xquery.util.misc import (
    batched,
    bundled,
)
from xquery.worker import worker

log = logging.getLogger(__name__)


class ControllerState(enum.Enum):
    UNKNOWN = 0
    INIT = enum.auto()
    RUNNING = enum.auto()
    TERMINATING = enum.auto()


class Controller(object):
    """
    Manages threads and worker processes
    """

    MAX_RESULT_STORAGE_SIZE = 1000

    def __init__(self, w3: Web3, db, indexer_cls: Type[EventIndexer], num_workers: int = None) -> None:
        """
        The core of XQuery

        :param w3: web3 provider
        :param db: database
        :param indexer_cls: event indexer class used to process event log entries
        :param num_workers: Number of worker processes to use. If None, the number returned by os.cpu_count() is used.
        """
        self.w3 = w3
        self.db = db

        self._indexer_cls = indexer_cls

        self._queue_jobs = mp.JoinableQueue(maxsize=1000)
        self._queue_results = mp.JoinableQueue(maxsize=1000)

        self._job_counter = 0
        self._result_counter = 0

        self._state = ControllerState.INIT
        self._terminating = mp.Event()

        self._db_handler = threading.Thread(
            name="DBHandler",
            target=self._handle_db,
            args=(),
            daemon=False,
        )

        self._num_workers = num_workers if num_workers is not None else os.cpu_count()
        self._workers = []
        for i in range(self._num_workers):
            w = mp.Process(
                name=f"Worker-{i}",
                target=worker,
                args=(
                    self.w3.provider.endpoint_uri,
                    self._indexer_cls,
                    self._queue_jobs,
                    self._queue_results,
                    self._terminating,
                ),
                daemon=False,
            )
            self._workers.append(w)

    def __del__(self):
        if self._state != ControllerState.TERMINATING:
            try:
                self.stop()
            except:
                pass

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def start(self):
        log.info("Starting Controller")
        self._db_handler.start()
        for w in self._workers:
            w.start()
        self._state = ControllerState.RUNNING

    def stop(self):
        log.info("Terminating Controller")
        self._terminating.set()
        self._queue_results.join()
        self._db_handler.join()
        for w in self._workers:
            w.join()
        self._state = ControllerState.TERMINATING

    @staticmethod
    def _find_non_consecutive(a: list, key: callable = None) -> int:
        """
        Find the index of the first non-consecutive element in a list

        Note: list has to contain only unique elements

        :param a: target list
        :param key: function to extract comparison key
        :return:
        """
        if len(a) == 0:
            return 0
        elif len(a) == 1:
            return 1
        else:  # len(a) > 1
            for i, j in enumerate(a):
                if key(a[0]) + i != key(j):
                    return i
            # all elements consecutive
            return len(a)

    def _handle_db(self):
        """
        Continuously get processed data elements from the ``queue_results``, sort them and finally
        commit them to the database (update the indexer state).

        Responsibilities:
        - sort job results
        - write job results to database
        - update indexer state
        - maintain database integrity at all costs

        Algorithm:
        - keep a local, sorted cache 'storage' of job results that were removed from the result_queue,
        but could not yet be added to the db, because they're out of order
        - first look in the cache 'storage', if the job result with the next id is found, write all elements with
        consecutive id to the database
        - next look in the queue, if job result with the next id is found, write it to the database, else write
        to the cache 'storage' for later processing/sorting
        - track job id counter to ensure no jobs are lost

        General rules:
        - only commit state changes on per block basis
        - minimum change is: all event log entries from a single block

        Assumptions:
        - the job id is unique, continuous and ascending
        - every job will be processed in finite time and hence will eventually be added
          to the result_queue (this is currently not necessarily the case, will need to implement a
          proper WorkerPool that can better manage workers and jobs in case of errors)
        - processed data in a job result is sorted

        Note: This will run in a separate thread in the main process
        """
        log.info("Starting database handler thread")

        # temporary list of out of order job results
        storage = []
        count_consecutive = 0

        with self.db.session() as session:
            # load the indexer state
            state = session.execute(
                select(orm.IndexerState)
            ).one_or_none()
            state = state[0] if state else None
            assert state is not None

            # trigger block deletion when the indexer restarts
            state.discarded = False

            while not self._terminating.is_set() or self._result_counter < self._job_counter:
                # sanity check to crash the indexer in case a job result cannot be found for a very long time
                # this can be removed once the WorkerPool class is added
                assert len(storage) < self.__class__.MAX_RESULT_STORAGE_SIZE

                storage_id = storage[0].id if len(storage) > 0 else None

                # log.info(pprint.pformat({
                #     "terminating": self._terminating.is_set(),
                #     "result_counter": self._result_counter,
                #     "job_counter": self._job_counter,
                #     "storage_id": storage_id,
                #     "len_storage": len(storage),
                #     "queue_results_size": self._queue_results.qsize(),
                # }))

                # a) process elements in the cached 'storage' first
                # check if the first element in the storage matches the next job result id
                if self._result_counter == storage_id:
                    # find the position of last non-consecutive element in the storage list
                    # Example:
                    # [5, 6, 7, 10, 8, ....]
                    # would return position 3, which belongs to element 10
                    pos = Controller._find_non_consecutive(storage, key=operator.attrgetter("id"))

                    # prepare database entries
                    for job_result in list(storage[:pos]):
                        queries = []
                        for bundle in job_result.results:
                            for i, result in enumerate(bundle):
                                if i == 0:
                                    state.block_height = result["block_height"]
                                    state.block_hash = result["block_hash"]
                                query = orm.XQuery(**result)
                                queries.append(query)

                        session.bulk_save_objects(queries)
                        session.add(state)

                        self._result_counter += 1

                    # remove processed entries
                    del storage[:pos]

                # b) process elements in the queue
                # get() until we encounter the first non-consecutive element
                # Note: we forcibly break the loop after N jobs to ensure data is regularly committed to the database
                while count_consecutive < 50:
                    count_consecutive += 1

                    try:
                        job_result = self._queue_results.get(timeout=1.0)
                    except queue.Empty:
                        # continue main loop
                        break

                    if self._result_counter == job_result.id:
                        queries = []
                        for bundle in job_result.results:
                            for i, result in enumerate(bundle):
                                if i == 0:
                                    state.block_height = result["block_height"]
                                    state.block_hash = result["block_hash"]
                                query = orm.XQuery(**result)
                                queries.append(query)

                        session.bulk_save_objects(queries)
                        session.add(state)

                        self._result_counter += 1
                        self._queue_results.task_done()

                        # find more consecutive job results
                        continue

                    else:
                        assert job_result.id > self._result_counter

                        # TODO use bisect.insort() once we switch to 3.10
                        storage.append(job_result)
                        storage = sorted(storage, key=operator.attrgetter("id"))

                        self._queue_results.task_done()

                        # continue main loop
                        break

                count_consecutive = 0

                # c) update database state
                session.commit()

        # sanity check to ensure all jobs have been processed
        assert len(storage) == 0

        log.info("Terminating database handler thread")

    # TODO
    # def load_state(self) -> orm.IndexerState:
    #     address = Web3.toChecksumAddress(address)
    #
    #     # load the indexer state
    #     with self.db.session() as session:
    #         state = session.execute(
    #             select(orm.IndexerState)
    #                 .filter(orm.IndexerState.contract_address == address)
    #         ).one_or_none()
    #         state = state[0] if state else None
    #
    #         # default
    #         if state is None:
    #             log.info(f"Creating new indexer state for contract '{address}' running on {chain}")
    #             state = orm.IndexerState(
    #                 contract_address=address,
    #                 block_height=0,
    #                 block_hash=None,
    #                 discarded=False,
    #             )
    #             session.add(state)
    #             session.commit()

    def scan(self, start_block: BlockIdentifier, end_block: BlockIdentifier, filter_: EventFilter, chunk_size: int, max_chunk_size: int) -> None:
        """
        Index data in the given range

        Note: runs on the main thread

        :param start_block: first block
        :param end_block: last block (included in the scan)
        :param filter_: event filter instance
        :param chunk_size: number of blocks fetched at once
        :param max_chunk_size: maximum number of blocks that should be fetched at once
        :return:
        """
        assert self._state == ControllerState.RUNNING

        if not isinstance(start_block, int):
            try:
                block_info = self.w3.eth.get_block(start_block)
                start_block = block_info.number
            except BlockNotFound:
                log.error(f"Failed to fetch block '{start_block}'")
                return

        if not isinstance(end_block, int):
            try:
                block_info = self.w3.eth.get_block(end_block)
                end_block = block_info.number
            except BlockNotFound:
                log.error(f"Failed to fetch block '{end_block}'")
                return

        assert start_block <= end_block
        log.info(f"Starting scan ({start_block} to {end_block})")

        current_block = start_block
        current_chunk_size = min(chunk_size, end_block - current_block)

        while current_block < end_block:
            logs = filter_.get_logs(
                from_block=current_block,
                chunk_size=current_chunk_size,
            )

            log.info(f"Fetched {len(logs)} log entries from {current_chunk_size} blocks ({current_block} to {current_block + current_chunk_size - 1})")

            # TODO dynamically change chunk_size depending on previously returned results to minimize API calls
            current_block += current_chunk_size
            current_chunk_size = min(chunk_size, end_block - current_block)

            # group/bundle by block height
            # Note: will later be used to ensure a consistent database state (commit all logs per block at once)
            bundles = bundled(logs, key=operator.attrgetter("blockNumber"))

            for batch in batched(bundles, size=16):
                # TODO add put() timeout, so we could exit if necessary
                try:
                    self._queue_jobs.put(Job(id=self._job_counter, entries=batch))
                except queue.Full:
                    # TODO
                    raise

                self._job_counter += 1

        # wait (block) for all jobs to be picked up by a worker
        self._queue_jobs.join()
        log.info("Finished scan")

    def scan_chunk(self, start_block: int, end_block: int) -> bool:
        # TODO move parts from scan() to this function
        raise NotImplementedError
