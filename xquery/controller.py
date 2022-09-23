#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import Type

import enum
import json
import logging
import multiprocessing as mp
import operator
import os
import pprint
import queue
import threading
import traceback

from sqlalchemy import select

from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.types import BlockIdentifier

import xquery.db
import xquery.db.orm as orm
from xquery.event.filter import EventFilter
from xquery.event.indexer import EventIndexer
from xquery.worker import (
    DataBundle,
    Job,
    JobResult,
    worker,
)
from xquery.util import (
    batched,
    bundled,
    init_decimal_context,
)

log = logging.getLogger(__name__)

# FIXME: There is currently a race condition in the controller. If the controller context is exited "too quickly",
# some workers might deadlock. A termination signal might be emitted before the worker is fully initialized.


class ControllerState(enum.Enum):
    UNKNOWN = 0
    INIT = enum.auto()
    RUNNING = enum.auto()
    TERMINATING = enum.auto()


class Controller(object):
    """
    Basic XQuery 2.0 program flow:
    0) Controller: manage concurrent elements (see bellow), init scan
    1) EventFilter: generate/fetch a list of event log entries
    2) EventIndexer: process filtered event list, prepare orm objects for the database
    3) DBHandler: safely write objects to the database
    4) EventProcessor: post-process event log entries in the database

    Has several concurrent elements:
    a) Main process:
       - MainThread:
          - runs the controller (manages threads, workers)
          - runs scan function (includes the EventFilter, adds jobs to shared queue)
       - DBHandlerThread:
          - gets job results from shared queue
          - sorts job results
          - writes job results to the database ("atomically" per block)
    b) Worker processes, each:
       - runs the EventIndexer
       - gets jobs from shared queue
       - adds results to shared queue
       - can write non-essential data to database
    c) Worker processes, each: -> NOT implemented
       - runs the EventProcessor
       - reads and writes to database

    Note: The indexer state should only ever be updated/changed in the main process!
    """

    MAX_RESULT_STORAGE_SIZE = 1000

    def __init__(self, w3: Web3, db: xquery.db.FusionSQL, indexer_cls: Type[EventIndexer], num_workers: int = None) -> None:
        """
        The core of XQuery. Manages threads and worker processes.

        :param w3: web3 provider
        :param db: database
        :param indexer_cls: event indexer class used to process event log entries
        :param num_workers: Number of worker processes to use. If None, the number returned by os.cpu_count() is used.
        """
        self.w3 = w3

        # Note: Currently shared between MainThread and DBHandler without any real protection in place
        self.db = db

        self._indexer_cls = indexer_cls

        self._queue_jobs = mp.JoinableQueue(maxsize=100)
        self._queue_results = mp.JoinableQueue(maxsize=100)

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
                name=f"Worker-I{i:02}",
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

        # rename main thread
        t = threading.current_thread()
        t.name = "Controller"

        init_decimal_context()
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

        # restore thread name
        t = threading.current_thread()
        t.name = "MainThread"

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

    def _commit_job(self, job_result: JobResult):
        """
        Finalize a job result and add associated orm objects to the database.

        Assumption: All events from a single block are always bundled in only one job.

        Note: Regularly refreshes the db connection,
              see https://docs.sqlalchemy.org/en/13/orm/session_basics.html#session-faq-whentocreate

        :param job_result: job result that should be added to the database
        :return:
        """

        # TODO possibly use .populate_existing()
        with self.db.session() as session:
            # load the indexer state
            state = session.execute(
                select(orm.IndexerState)
                    .filter(orm.IndexerState.name == "default")
                    .with_for_update()
            ).scalar()
            assert state is not None

            for bundle in job_result.data:
                state.block_number = bundle.meta["block_number"]
                state.block_hash = bundle.meta["block_hash"]
                session.add(state)

                for result in bundle.objects:
                    for obj in result:
                        log.debug(f"Merging object '{obj}'")
                        # Note: will add the object to the session
                        session.merge(obj, load=True)

            session.commit()

        # report progress
        if self._result_counter % 20 == 0:
            log.info(f"Committed events up to block {bundle.meta['block_number']}")

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

        init_decimal_context()

        # temporary list of out of order job results
        storage = []
        count_consecutive = 0

        try:
            while not self._terminating.is_set() or self._result_counter < self._job_counter:
                # sanity check to crash the indexer in case a job result cannot be found for a very long time
                # this can be removed once the WorkerPool class is added
                assert len(storage) < Controller.MAX_RESULT_STORAGE_SIZE

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
                        self._commit_job(job_result)
                        self._result_counter += 1

                    # remove processed entries
                    del storage[:pos]

                # b) process elements in the queue
                # get() until we encounter the first non-consecutive element
                # Note: we forcibly break the loop after N jobs to check the terminating event
                while count_consecutive < 20:
                    count_consecutive += 1

                    try:
                        job_result = self._queue_results.get(timeout=1.0)
                    except queue.Empty:
                        # continue main loop
                        break

                    if self._result_counter == job_result.id:
                        self._commit_job(job_result)
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

        except Exception:
            log.critical("Encountered unexpected error in database handler thread. Terminating!")
            log.error(traceback.format_exc())
            self._terminating.set()
            raise

        # sanity check to ensure all jobs have been processed
        assert len(storage) == 0

        log.info("Terminating database handler thread")

    def scan(
        self,
        start_block: BlockIdentifier,
        end_block: BlockIdentifier,
        num_safety_blocks: int,
        filter_: EventFilter,
        chunk_size: int,
        max_chunk_size: int,
    ) -> None:
        """
        Index data in the given block range

        Note: Runs on the main thread

        :param start_block: first block
        :param end_block: last block (included in the scan)
        :param num_safety_blocks: number of most recent blocks that should be skipped when indexing the full chain
            (ensure only finalized blocks are indexed)
        :param filter_: event filter instance
        :param chunk_size: number of blocks fetched at once
        :param max_chunk_size: maximum number of blocks that should be fetched at once
        :return:
        """
        assert self._state == ControllerState.RUNNING

        try:
            block_info = self.w3.eth.get_block("latest")
            latest_block = block_info.number
        except BlockNotFound:
            log.error("Failed to fetch block 'latest'")
            return

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
        end_block = min(end_block, latest_block - num_safety_blocks)

        log.info(f"Starting scan ({start_block} to {end_block} with {num_safety_blocks} safety blocks)")

        # we are already done
        if start_block > end_block:
            return

        current_block = start_block
        current_chunk_size = min(chunk_size, end_block - current_block)

        while current_block < end_block:
            # TODO needs logic to handle 'eth_getLogs' throttle errors
            logs = filter_.get_logs(
                from_block=current_block,
                chunk_size=current_chunk_size,
            )

            log.info(f"Fetched {len(logs)} log entries from {current_chunk_size} blocks ({current_block} to {current_block + current_chunk_size - 1})")
            log.debug(pprint.pformat([json.loads(Web3.toJSON(entry)) for entry in logs]))

            # TODO dynamically change chunk_size depending on previously returned results to minimize API calls
            current_block += current_chunk_size
            current_chunk_size = min(chunk_size, end_block - current_block)

            # group/bundle by block height
            # Note: will later be used to ensure a consistent database state (commit all logs per block at once)
            basic_bundles = bundled(logs, key=operator.attrgetter("blockNumber"))

            # determine metadata and convert to DataBundle objects
            bundles = []
            for bundle in basic_bundles:
                bundles.append(
                    DataBundle(
                        objects=bundle,
                        meta={
                            "block_number": bundle[0].blockNumber,
                            "block_hash": bundle[0].blockHash.hex(),
                        },
                    )
                )

            for batch in batched(bundles, size=16):
                # TODO add put() timeout, so we could exit if necessary
                try:
                    self._queue_jobs.put(Job(id=self._job_counter, data=batch))
                except queue.Full:
                    # TODO
                    raise

                self._job_counter += 1

        # wait (blocking) for all jobs to be picked up by a worker
        self._queue_jobs.join()
        log.info("Finished scan")

    def scan_chunk(self, start_block: int, end_block: int) -> None:
        # TODO move parts from scan() to this function
        raise NotImplementedError

    def run(self, start_block) -> bool:
        """
        Stay up to date after an initial scan. Use a 'web3.eth.filter' to continuously check
        for new/changed entries.

        :param start_block:
        :return:
        """
        raise NotImplementedError
