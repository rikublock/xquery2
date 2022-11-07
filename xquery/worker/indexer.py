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

from web3 import Web3
from web3.middleware import geth_poa_middleware

import xquery.event.indexer
from xquery.config import CONFIG as C
from xquery.middleware import http_backoff_retry_request_middleware
from xquery.provider import BatchHTTPProvider
from .base import WorkerBase
from .job import (
    DataBundle,
    JobResult,
    JobType,
)

log = logging.getLogger(__name__)


class WorkerIndexer(WorkerBase):

    def __init__(
        self,
        indexer_cls: Type[xquery.event.indexer.EventIndexer],
        queue_jobs: mp.JoinableQueue,
        queue_results: mp.JoinableQueue,
        terminating: mp.Event,
        *args,
        **kwargs,
    ) -> None:
        """
        Indexer worker process

        :param indexer_cls: event indexer class used to process event entries
        :param queue_jobs: in queue
        :param queue_results: out queue
        :param terminating: event to trigger shutdown
        """
        super().__init__(queue_jobs, queue_results, terminating, *args, **kwargs)

        self.indexer_cls = indexer_cls

    def run(self) -> None:
        log.info(f"Starting worker process ({os.getpid()})")
        self._init_process()

        # prepare web3 provider
        try:
            w3 = Web3(BatchHTTPProvider(endpoint_uri=C["API_URL"], request_kwargs={"timeout": 30}))
            w3.middleware_onion.clear()
            w3.middleware_onion.add(http_backoff_retry_request_middleware, "http_backoff_retry_request")
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        except Exception:
            self.terminating.set()
            raise

        # prepare event indexer
        try:
            event_indexer = self.indexer_cls(
                w3=w3,
                db=self.db,
                cache=self.cache,
            )
        except Exception:
            self.terminating.set()
            raise

        # worker main loop
        try:
            while not (self.terminating.is_set() or self.terminating_local.is_set()):
                try:
                    job = self.queue_jobs.get(timeout=1.0)
                except queue.Empty:
                    continue

                log.info(f"Processing {job}")
                assert job.type == JobType.Index

                result = JobResult(id=job.id, type=job.type, data=[])
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

                # reset the indexer after processing a job
                event_indexer.reset()

                self.queue_results.put(result)
                self.queue_jobs.task_done()

                log.info(f"Completed {job}")

        except Exception:
            log.critical("Encountered unexpected error in worker. Terminating!", stack_info=True, exc_info=True)
            self.terminating.set()
            raise

        log.info(f"Terminating worker process ({os.getpid()})")
