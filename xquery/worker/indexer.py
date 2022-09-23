#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import Type

import logging
import multiprocessing as mp
import os
import queue
import signal
import threading
import traceback

from web3 import Web3
from web3.middleware import geth_poa_middleware

import xquery.db
import xquery.cache
import xquery.event.indexer
from xquery.config import CONFIG as C
from xquery.middleware import http_backoff_retry_request_middleware
from xquery.provider import BatchHTTPProvider
from .base import WorkerBase
from .job import (
    DataBundle,
    JobResult,
)
from xquery.util import init_decimal_context

log = logging.getLogger(__name__)


# TODO convert to a class (would allow to create different workers that can inherit base functionality)
class WorkerIndexer(WorkerBase):

    def __init__(self):
        super().__init__()

    def run(self):
        # Note: Only code inside run() executes in the new process!
        raise NotImplementedError


# TODO workers should probably use threads/async to offset some of the web request delays
def worker(
    api_url: str,
    indexer_cls: Type[xquery.event.indexer.EventIndexer],
    queue_jobs: mp.JoinableQueue,
    queue_results: mp.JoinableQueue,
    terminating: mp.Event,
) -> int:
    """
    Body of a worker process

    :param api_url: web3 provider url
    :param indexer_cls: event indexer class used to process event entries
    :param queue_jobs: in queue
    :param queue_results: out queue
    :param terminating: event to trigger shutdown
    :return: exit status
    """
    log.info(f"Starting worker process ({os.getpid()})")

    # rename MainThread
    thread = threading.current_thread()
    thread.name = mp.current_process().name

    terminating_local = threading.Event()

    # handle OS signals
    def _signal_handler(signum, frame):
        log.critical("Received SIGINT or SIGTERM")
        terminating_local.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # prepare web3 provider
    try:
        w3 = Web3(BatchHTTPProvider(endpoint_uri=api_url, request_kwargs={"timeout": 30}))
        w3.middleware_onion.clear()
        w3.middleware_onion.add(http_backoff_retry_request_middleware, "http_backoff_retry_request")
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        terminating.set()
        raise

    # prepare database
    try:
        db = xquery.db.FusionSQL(
            conn=xquery.db.build_url(
                driver=C["DB_DRIVER"],
                host=C["DB_HOST"],
                port=C["DB_PORT"],
                username=C["DB_USERNAME"],
                password=C["DB_PASSWORD"],
                database=C["DB_DATABASE"],
            ),
            verbose=C["DB_DEBUG"],
        )
    except Exception as e:
        terminating.set()
        raise

    # prepare cache
    try:
        cache = xquery.cache.Cache_Redis(
            host=C["REDIS_HOST"],
            port=C["REDIS_PORT"],
            password=C["REDIS_PASSWORD"],
            db=C["REDIS_DATABASE"],
        )
    except Exception as e:
        terminating.set()
        raise

    # prepare event indexer
    try:
        event_indexer = indexer_cls(
            w3=w3,
            db=db,
            cache=cache,
        )
    except Exception as e:
        terminating.set()
        raise

    init_decimal_context()

    # worker main loop
    try:
        while not (terminating.is_set() or terminating_local.is_set()):
            try:
                job = queue_jobs.get(timeout=1.0)
            except queue.Empty:
                continue

            log.info(f"Processing {job}")
            result = JobResult(id=job.id, data=[])
            for bundle in job.data:
                sub_result = []
                for entry in bundle.objects:
                    r = event_indexer.process(entry)
                    sub_result.append(r)

                result.data.append(
                    DataBundle(
                        objects=sub_result,
                        meta=bundle.meta,
                    )
                )

            queue_results.put(result)
            queue_jobs.task_done()

            # reset the indexer after processing a job
            event_indexer.reset()

            log.info(f"Completed {job}")

    except Exception:
        log.critical("Encountered unexpected error in worker. Terminating!")
        log.error(traceback.format_exc())
        terminating.set()
        raise

    log.info(f"Terminating worker process ({os.getpid()})")
    return 0
