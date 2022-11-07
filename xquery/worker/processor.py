#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import logging
import multiprocessing as mp
import os
import queue

from .base import WorkerBase
from .job import (
    DataBundle,
    JobResult,
    JobType,
)

log = logging.getLogger(__name__)


class WorkerProcessor(WorkerBase):

    def __init__(
        self,
        queue_jobs: mp.JoinableQueue,
        queue_results: mp.JoinableQueue,
        terminating: mp.Event,
        *args,
        **kwargs
    ) -> None:
        """
        Processor worker process

        :param queue_jobs: in queue
        :param queue_results: out queue
        :param terminating: event to trigger shutdown
        """
        super().__init__(queue_jobs, queue_results, terminating, *args, **kwargs)

    def run(self) -> None:
        log.info(f"Starting worker process ({os.getpid()})")
        self._init_process()

        # worker main loop
        try:
            while not (self.terminating.is_set() or self.terminating_local.is_set()):
                try:
                    job = self.queue_jobs.get(timeout=1.0)
                except queue.Empty:
                    continue

                log.info(f"Processing {job}")
                assert job.type == JobType.Process

                result = JobResult(id=job.id, type=job.type, data=[])

                for bundle in job.data:
                    sub_result = []
                    for entry in bundle.objects:
                        stage = entry.stage_cls(db=self.db, cache=self.cache)
                        r = stage.process(entry.a, entry.b)
                        sub_result.append(r)

                    result.data.append(
                        DataBundle(
                            objects=sub_result,
                            meta=bundle.meta,
                        )
                    )

                self.queue_results.put(result)
                self.queue_jobs.task_done()

                log.info(f"Completed {job}")

        except Exception:
            log.critical("Encountered unexpected error in worker. Terminating!", stack_info=True, exc_info=True)
            self.terminating.set()
            raise

        log.info(f"Terminating worker process ({os.getpid()})")

