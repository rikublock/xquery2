#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import logging
import multiprocessing as mp
import signal
import threading

import xquery.db
import xquery.cache
import xquery.event.indexer
from xquery.config import CONFIG as C
from xquery.util import init_decimal_context

log = logging.getLogger(__name__)


class WorkerBase(mp.Process):

    def __init__(
        self,
        queue_jobs: mp.JoinableQueue,
        queue_results: mp.JoinableQueue,
        terminating: mp.Event,
        *args,
        **kwargs
    ) -> None:
        """
        Base worker process

        :param queue_jobs: in queue
        :param queue_results: out queue
        :param terminating: event to trigger shutdown
        """
        super().__init__(*args, **kwargs)

        self.queue_jobs = queue_jobs
        self.queue_results = queue_results
        self.terminating = terminating
        self.terminating_local = None
        self.started = mp.Event()

        self.db = None
        self.cache = None

    def _init_process(self) -> None:
        """
        Initialize a new worker process

        :return:
        """
        # rename process MainThread
        thread = threading.current_thread()
        thread.name = mp.current_process().name

        self.terminating_local = threading.Event()

        # handle OS signals
        def _signal_handler(signum, frame):
            log.critical(f"Received {signal.Signals(signum).name} ({signum}) '{signal.strsignal(signum)}'. Terminating!")
            self.terminating_local.set()

        signal.signal(signal.SIGHUP, _signal_handler)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, _signal_handler)

        # prepare database
        try:
            self.db = xquery.db.FusionSQL(
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
        except Exception:
            self.terminating.set()
            raise

        # prepare cache
        try:
            self.cache = xquery.cache.Cache_Redis(
                host=C["REDIS_HOST"],
                port=C["REDIS_PORT"],
                password=C["REDIS_PASSWORD"],
                db=C["REDIS_DATABASE"],
            )
        except Exception:
            self.terminating.set()
            raise

        init_decimal_context()

        self.started.set()

    def run(self) -> None:
        """
        Body of a worker process

        Note: Only code inside run() executes in the new process!
        Note: Should invoke _init_process()

        :return:
        """
        raise NotImplementedError
