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

from web3 import Web3
from web3.middleware import geth_poa_middleware

import xquery.cache
import xquery.event.indexer
import xquery.provider
from xquery.config import CONFIG as C
from xquery.job import JobResult

log = logging.getLogger(__name__)

# TODO workers should probably use threads/async to offset some of the web request delays
# TODO convert to a class (would allow to create different workers that can inherit base functionality)


def worker(
        api_url: str,
        indexer_cls: Type[xquery.event.indexer.EventIndexer],
        queue_jobs: mp.JoinableQueue,
        queue_results: mp.JoinableQueue,
        terminating: mp.Event
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

    terminating_local = threading.Event()

    # handle OS signals
    def _signal_handler(signum, frame):
        log.critical("Received SIGINT or SIGTERM")
        terminating_local.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # prepare web3 provider
    try:
        w3 = Web3(xquery.provider.BatchHTTPProvider(api_url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        log.error(e)
        return 1

    # prepare cache
    try:
        cache = xquery.cache.Cache_Redis(
            host=C["REDIS_HOST"],
            port=C["REDIS_PORT"],
            password=C["REDIS_PASSWORD"],
            db=C["REDIS_DATABASE"],
        )
    except Exception as e:
        log.error(e)
        return 1

    # prepare event indexer
    try:
        event_indexer = indexer_cls(
            w3=w3,
            cache=cache,
        )
    except Exception as e:
        log.error(e)
        return 1

    # worker main loop
    while not (terminating.is_set() or terminating_local.is_set()):
        try:
            job = queue_jobs.get(timeout=1.0)
        except queue.Empty:
            continue

        log.info(f"Processing {job}")
        result = JobResult(id=job.id, results=[])
        for bundle in job.entries:
            result_bundle = []
            for entry in bundle:
                r = event_indexer.process(entry)
                result_bundle.append(r)
            result.results.append(result_bundle)

        queue_results.put(result)
        queue_jobs.task_done()

        log.info(f"Completed {job}")

    log.info(f"Terminating worker process ({os.getpid()})")
    return 0
