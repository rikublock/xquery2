#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Callable,
    List,
    Type,
)

import enum
import json
import logging
import multiprocessing as mp
import operator
import os
import pprint
import queue
import signal
import threading
import time

from requests.exceptions import (
    HTTPError,
    Timeout,
)

from sqlalchemy import select

from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.types import BlockIdentifier

import xquery.cache
import xquery.db
import xquery.db.orm as orm
from xquery.event import (
    ComputeInterval,
    EventFilter,
    EventIndexer,
    EventProcessor,
)
from xquery.util import (
    batched,
    bundled,
    init_decimal_context,
    intervaled,
)
from xquery.worker import (
    DataBundle,
    Job,
    JobResult,
    JobType,
    WorkerIndexer,
    WorkerProcessor,
)

log = logging.getLogger(__name__)

# FIXME: There is currently a race condition in the controller. If the controller context is exited "too quickly",
# some workers might deadlock. A termination signal might be emitted before the worker is fully initialized.


class SignalContext(object):

    def __init__(self, signals: List[signal.Signals], handler: Callable):
        self._signals = set(signals)
        self._handler = handler
        self._cache = {}

    def __enter__(self):
        # register signal handlers
        for sig in self._signals:
            self._cache[sig] = signal.signal(sig, self._handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # restore default signal handlers
        for k, v in self._cache.items():
            signal.signal(k, v)
        self._cache = {}


class ControllerState(enum.Enum):
    UNKNOWN = 0
    INIT = enum.auto()
    RUNNING = enum.auto()
    TERMINATING = enum.auto()


class Controller(object):
    """
    Basic XQuery 2.0 program flow:
    0) Controller: manage concurrent elements (see bellow), init scan and compute (create jobs)
    1) EventFilter: generate/fetch a list of event log entries
    2) EventIndexer: process filtered event list, prepare orm objects for the database
    3) EventProcessor: generate a list of processor stages
    4) EventProcessorStage: post-process event log entries from the database, prepare new/updated orm objects
    5) DBHandler: safely write orm objects to the database, update state

    Has several concurrent elements:
    a) Main process:
       - MainThread:
          - runs the controller (manages threads, workers)
          - runs scan function (includes the EventFilter, adds jobs to shared indexer queue)
          - runs compute function (includes the EventProcessor, adds jobs to shared processor queue)
       - DBHandlerThread:
          - gets job results from shared queues
          - sorts job results
          - writes job results to the database ("atomically" per block)
       - SignalHandler:
          - handle posix signals (primarily used to shutdown XQuery)
    b) Worker (indexer) processes, each:
       - runs the EventIndexer
       - gets jobs from a shared queue
       - adds results to a shared queue
       - can write non-essential data to database
    c) Worker (processor) processes, each:
       - runs the EventProcessorStage
       - exclusively gets (reads) data from the database
       - gets jobs from a separate shared queue
       - adds results to same shared queue
       - can write non-essential data to database

    Note: The indexer/processor state should only ever be updated/changed in the main process!
    """

    MAX_RESULT_STORAGE_SIZE = 1000

    def __init__(self, w3: Web3, db: xquery.db.FusionSQL, indexer_cls: Type[EventIndexer], num_workers: int = None) -> None:
        """
        The core of XQuery. Manages threads and worker processes.

        :param w3: web3 provider
        :param db: database service
        :param indexer_cls: event indexer class used to process event log entries
        :param num_workers: Number of worker processes to use. If None, the number returned by os.cpu_count() is used.
        """
        self._w3 = w3

        # Note: Currently shared between MainThread and DBHandler without any real protection in place
        self._db = db

        self._indexer_cls = indexer_cls

        self._local_cache = xquery.cache.Cache_Memory()

        self._queue_jobs_index = mp.JoinableQueue(maxsize=100)
        self._queue_jobs_process = mp.JoinableQueue(maxsize=100)
        self._queue_results = mp.JoinableQueue(maxsize=100)

        self._job_counter = 0
        self._result_counter = 0

        self._state = ControllerState.INIT
        self._terminating = mp.Event()
        self._terminating_local = threading.Event()

        self._db_handler = threading.Thread(
            name="DBHandler",
            target=self._handle_db,
            args=(),
            daemon=False,
        )

        self._num_workers = num_workers if num_workers is not None else os.cpu_count()
        assert self._num_workers > 0

        self._workers_index = []
        for i in range(self._num_workers):
            w = WorkerIndexer(
                name=f"Worker-I{i:02}",
                daemon=False,
                indexer_cls=self._indexer_cls,
                queue_jobs=self._queue_jobs_index,
                queue_results=self._queue_results,
                terminating=self._terminating,
            )
            self._workers_index.append(w)

        self._workers_process = []
        for i in range(self._num_workers):
            w = WorkerProcessor(
                name=f"Worker-P{i:02}",
                daemon=False,
                queue_jobs=self._queue_jobs_process,
                queue_results=self._queue_results,
                terminating=self._terminating,
            )
            self._workers_process.append(w)

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

    def start(self) -> None:
        """
        Start the controller

        :return:
        """
        log.info("Starting Controller")

        # rename main thread
        t = threading.current_thread()
        t.name = "Controller"

        init_decimal_context()
        self._db_handler.start()
        for w in self._workers_index:
            w.start()
        for w in self._workers_process:
            w.start()
        self._state = ControllerState.RUNNING

    def stop(self) -> None:
        """
        Terminate the controller

        :return:
        """
        log.info("Terminating Controller")

        self._terminating.set()
        self._queue_results.join()
        self._db_handler.join()
        for w in self._workers_index:
            w.join()
        for w in self._workers_process:
            w.join()

        if self._job_counter != self._result_counter:
            log.warning(f"Number of jobs does not match results ({self._job_counter} != {self._result_counter})!")

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

    def _get_state(self, name: str) -> orm.State:
        """
        Get a state object

        Create a new state entry, if it doesn't already exist.

        :param name: state identifier
        :return:
        """
        log.debug(f"Getting state '{name}'")

        key = f"_state_{name}"
        state = self._local_cache.get(key)

        if not state:
            with self._db.session() as session:
                state = session.execute(
                    select(orm.State)
                        .filter(orm.State.name == name)
                ).scalar()

                if state is None:
                    state = orm.State(
                        name=name,
                        block_number=None,
                        block_hash=None,
                    )

                    session.add(state)
                    session.commit()

            self._local_cache.set(key, state)

        # ensure only persistent/detached objects get loaded from the cache
        assert state.id is not None

        return state

    def _handle_signal(self, signum, frame) -> None:
        log.critical(f"Received {signal.Signals(signum).name} ({signum}) '{signal.strsignal(signum)}'. Terminating!")
        self._terminating_local.set()

    def _commit_job(self, job_result: JobResult) -> None:
        """
        Finalize a job result and add/update associated orm objects to/in the database.

        Assumption: All events from a single block are always "bundled" in only one (single) job.

        Note: Regularly refreshes the db connection, see
              https://docs.sqlalchemy.org/en/14/orm/session_basics.html#session-faq-whentocreate

        :param job_result: job result that should be added to the database
        :return:
        """
        with self._db.session() as session:
            for i, bundle in enumerate(job_result.data):
                # Note: Only need to update the state once (last element) as we can assume that objects are sorted
                #       and that all objects from a block are always bundled together in a single job result.
                if i == len(job_result.data) - 1:
                    name = bundle.meta["state_name"]
                    state = self._get_state(name)
                    state.block_number = int(bundle.meta["block_number"])
                    state.block_hash = bundle.meta["block_hash"]
                    session.merge(state, load=True)

                    # update cached state
                    key = f"_state_{name}"
                    self._local_cache.set(key, state)

                for result in bundle.objects:
                    for obj in result:
                        if isinstance(obj, orm.Base):
                            log.debug(f"Merging object '{obj}'")
                            session.merge(obj, load=True)
                        elif isinstance(obj, tuple) and len(obj) == 2:
                            log.debug(f"Bulk inserting {len(obj[1])} '{(obj[0]).__name__}' objects")
                            session.bulk_insert_mappings(*obj)
                        else:
                            raise TypeError(obj)

            session.commit()

        # report progress
        log.info(f"Committed {job_result} for state '{name}' up to block {bundle.meta['block_number']}")

    def _handle_db(self) -> None:
        """
        Continuously get processed data elements from the ``queue_results``, sort them and finally
        commit them to the database (update the indexer and processor state).

        Responsibilities:
        - sort job results
        - write job results to database
        - update indexer and processor state
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
                        self._queue_results.task_done()

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

                        # continue main loop
                        break

                count_consecutive = 0

        except Exception:
            log.critical("Encountered unexpected error in database handler thread. Terminating!", stack_info=True, exc_info=True)
            self._terminating.set()
            raise

        # sanity check to ensure all jobs have been processed
        assert len(storage) == 0

        log.info("Terminating Database Handler")

    def _estimate_next_chunk_size(self, current_chuck_size: int, count_logs: int) -> int:
        """
        Dynamically adjust the chunk_size depending on log entry density in the current
        block chain section in order to minimize the number of API calls.

        :param current_chuck_size: range of blocks scanned in the previous filter call
        :param count_logs: number of event log entries found in the previous filter call
        :return:
        """
        # TODO implement
        return current_chuck_size

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
        Index event data in the given block range.

        Note: Runs on the main thread
        Note: Can adjust the ``start_block`` internally depending on the indexer state

        TODO make use of the "safe" or "finalized" block identifier once it's part of the specification

        :param start_block: first block
        :param end_block: last block (included in the scan)
        :param num_safety_blocks: number of most recent blocks that should be skipped when indexing the full chain
            (ensure only finalized blocks are indexed)
        :param filter_: event filter instance
        :param chunk_size: number of blocks fetched at once
        :param max_chunk_size: maximum number of blocks that should be fetched at once (currently ignored)
        :return:
        """
        assert self._state == ControllerState.RUNNING

        # sanity check: ensure all previous jobs have been fully processed and committed
        self._queue_results.join()
        assert self._job_counter == self._result_counter

        try:
            block_info = self._w3.eth.get_block("latest")
            latest_block = block_info.number
        except BlockNotFound:
            log.error("Failed to fetch block 'latest'")
            return

        if not isinstance(start_block, int):
            try:
                block_info = self._w3.eth.get_block(start_block)
                start_block = block_info.number
            except BlockNotFound:
                log.error(f"Failed to fetch block '{start_block}'")
                return

        if not isinstance(end_block, int):
            try:
                block_info = self._w3.eth.get_block(end_block)
                end_block = block_info.number
            except BlockNotFound:
                log.error(f"Failed to fetch block '{end_block}'")
                return

        assert start_block <= end_block

        # load the indexer state
        state_name = "indexer"
        state_indexer = self._get_state(state_name)

        # initialize the indexer
        if state_indexer.block_number is None:
            log.info(f"Initializing indexer")

            # running the setup job on the main thread, hence we directly create a job result
            result = JobResult(id=self._job_counter, type=JobType.Index, data=[])
            self._job_counter += 1

            objects = self._indexer_cls.setup(self._w3, self._db, start_block)

            result.data.append(
                DataBundle(
                    objects=[objects],
                    meta={
                        "state_name": state_name,
                        "block_number": start_block,
                        "block_hash": None,
                    },
                )
            )

            # TODO add put() timeout, so we could exit if necessary
            try:
                self._queue_results.put(result)
            except queue.Full:
                raise

            # wait for indexer setup to complete
            self._queue_results.join()
            assert self._job_counter == self._result_counter

            # reload state
            state_indexer = self._get_state(state_name)
            assert state_indexer.block_number is not None

        start_block = max(start_block, state_indexer.block_number + 1)
        end_block = min(end_block, latest_block - num_safety_blocks)

        # we are already done
        if start_block > end_block:
            log.info(f"Skipping obsolete scan ({start_block} to {end_block})")
            return

        log.info(f"Starting scan ({start_block} to {end_block} with {num_safety_blocks} safety blocks)")

        # Regarding chunk size:
        # - a range of blocks always includes both the start and the end block
        # - the range 4 to 6 (start 4, end 6) would scan a total of 3 blocks (4, 5 and 6)
        # - the same range would translate to a chunk_size = end - start + 1 = 3 with starting block 4
        current_block = start_block
        current_chunk_size = min(chunk_size, end_block - current_block + 1)

        while current_block <= end_block:
            if self._terminating_local.is_set():
                break

            # handle possible 'eth_getLogs' throttle errors
            retries = 5
            delay = 3.0
            for i in range(retries):
                try:
                    logs = filter_.get_logs(
                        from_block=current_block,
                        chunk_size=current_chunk_size,
                    )
                except (HTTPError, Timeout):
                    if i < retries - 1:
                        current_chunk_size = max(1, current_chunk_size // 2)
                        log.warning(f"Failed to fetch log entries. Reducing number of blocks to {current_chunk_size} and retrying in {delay:.2f}s.")
                        time.sleep(delay)
                    else:
                        raise

            log.info(f"Fetched {len(logs)} log entries from {current_chunk_size} blocks ({current_block} to {current_block + current_chunk_size - 1})")
            log.debug(pprint.pformat([json.loads(Web3.toJSON(entry)) for entry in logs]))

            current_block += current_chunk_size
            current_chunk_size = self._estimate_next_chunk_size(current_chunk_size, len(logs))
            current_chunk_size = min(current_chunk_size, end_block - current_block + 1)

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
                            "state_name": state_name,
                            "block_number": bundle[0].blockNumber,
                            "block_hash": bundle[0].blockHash.hex(),
                        },
                    )
                )

            for batch in batched(bundles, size=16):
                if self._terminating_local.is_set():
                    break

                # TODO add put() timeout, so we could exit if necessary
                try:
                    self._queue_jobs_index.put(Job(id=self._job_counter, type=JobType.Index, data=batch))
                except queue.Full:
                    raise

                self._job_counter += 1

        # wait (blocking) for all jobs to be picked up by an indexer worker
        self._queue_jobs_index.join()
        log.info("Finished scan")

    def compute(self, start_block: BlockIdentifier, end_block: BlockIdentifier, processor: EventProcessor) -> None:
        """
        Post-process indexed event data from the database for a given block range.

        Note: Runs on the main thread
        Note: Can adjust the ``end_block`` internally depending on the indexer state
        Note: Can adjust the ``start_block`` internally depending on the processor stage state

        :param start_block: first block
        :param end_block: last block (included in the computation)
        :param processor: event processor instance
        :return:
        """
        assert self._state == ControllerState.RUNNING

        # sanity check: ensure all previous jobs have been fully processed and committed
        self._queue_results.join()
        assert self._job_counter == self._result_counter

        if not isinstance(start_block, int):
            try:
                block_info = self._w3.eth.get_block(start_block)
                start_block = block_info.number
            except BlockNotFound:
                log.error(f"Failed to fetch block '{start_block}'")
                return

        if not isinstance(end_block, int):
            try:
                block_info = self._w3.eth.get_block(end_block)
                end_block = block_info.number
            except BlockNotFound:
                log.error(f"Failed to fetch block '{end_block}'")
                return

        assert start_block <= end_block

        # load the indexer state
        # Note: This ensures the processor is aligned with the indexer progress
        state_indexer = self._get_state("indexer")
        assert state_indexer.block_number is not None

        end_block = min(end_block, state_indexer.block_number)

        log.info("Starting compute")

        for stage in processor:
            if self._terminating_local.is_set():
                break

            # load the processor stage state
            state_name = f"processor_{stage.name}"
            state_processor = self._get_state(state_name)

            # initialize the stage
            if state_processor.block_number is None:
                log.info(f"Initializing stage '{stage.name}'")

                # running the setup job on the main thread, hence we directly create a job result
                result = JobResult(id=self._job_counter, type=JobType.Process, data=[])
                self._job_counter += 1

                objects = stage.cls.setup(self._db, start_block)

                result.data.append(
                    DataBundle(
                        objects=[objects],
                        meta={
                            "state_name": state_name,
                            "block_number": start_block,
                            "block_hash": None,
                        },
                    )
                )

                # TODO add put() timeout, so we could exit if necessary
                try:
                    self._queue_results.put(result)
                except queue.Full:
                    raise

                # wait for stage setup to complete
                self._queue_results.join()
                assert self._job_counter == self._result_counter

                # reload state
                state_processor = self._get_state(state_name)
                assert state_processor.block_number is not None

            adjust_start_block = max(start_block, state_processor.block_number + 1)

            # we are already done
            if adjust_start_block > end_block:
                log.info(f"Skipping up-to-date stage '{stage.name}' ({adjust_start_block} to {end_block})")
                continue

            log.info(f"Processing stage '{stage.name}' ({adjust_start_block} to {end_block})")

            batch_size = stage.batch_size if stage.batch_size else (end_block - adjust_start_block + 1)
            for a, b in intervaled(adjust_start_block, end_block, batch_size):
                if self._terminating_local.is_set():
                    break

                data = DataBundle(
                    objects=[
                        ComputeInterval(stage.cls, a, b),
                    ],
                    meta={
                        "state_name": state_name,
                        "block_number": b,
                        "block_hash": None,
                    },
                )

                # TODO add put() timeout, so we could exit if necessary
                try:
                    self._queue_jobs_process.put(Job(id=self._job_counter, type=JobType.Process, data=[data]))
                except queue.Full:
                    raise

                self._job_counter += 1

            # ensure stage has been fully computed
            self._queue_jobs_process.join()
            self._queue_results.join()
            assert self._job_counter == self._result_counter

        # wait (blocking) for all jobs to be picked up by a processor worker
        self._queue_jobs_process.join()
        log.info("Finished compute")

    def run(
        self,
        start_block: BlockIdentifier,
        end_block: BlockIdentifier,
        num_safety_blocks: int,
        filter_: EventFilter,
        processor: EventProcessor,
        chunk_size: int,
        target_sleep_time: int,
    ) -> None:
        """
        Continuously scan and compute event data in the given block range.

        Note: This method should only be used in conjunction with dynamic block identifiers (e.g. "latest").

        For more details see the ``scan()`` and ``compute()`` documentation.

        :param start_block: first block
        :param end_block: last block (included in the scan and computation)
        :param num_safety_blocks: number of most recent blocks that should be skipped when indexing the full chain
            (ensure only finalized blocks are indexed)
        :param filter_: event filter instance
        :param processor: event processor instance
        :param chunk_size: number of blocks fetched at once
        :param target_sleep_time: target number of seconds before resuming the main loop
        :return:
        """
        with SignalContext([signal.SIGHUP, signal.SIGINT, signal.SIGTERM], self._handle_signal):
            while not (self._terminating.is_set() or self._terminating_local.is_set()):
                current = time.time()

                # run the indexer
                self.scan(
                    start_block=start_block,
                    end_block=end_block,
                    num_safety_blocks=num_safety_blocks,
                    filter_=filter_,
                    chunk_size=chunk_size,
                    max_chunk_size=2048,
                )

                # run the processor
                self.compute(
                    start_block=start_block,
                    end_block=end_block,
                    processor=processor,
                )

                elapsed = time.time() - current
                log.info(f"Executed main loop in {elapsed:.4f}s")

                delay = target_sleep_time - elapsed
                if delay > 0:
                    log.info(f"Resuming main loop in {delay:.2f}s")
                    self._terminating_local.wait(delay)

            # FIXME: workaround to give all worker processes enough time to fully initialize in the case of a short scan
            time.sleep(5.0)
