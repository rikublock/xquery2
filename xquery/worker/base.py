#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import multiprocessing as mp


# TODO
class WorkerBase(mp.Process):

    def __init__(self):
        super().__init__()

    def _init_process(self):
        pass

    def run(self):
        # Note: Only code inside run() executes in the new process!
        self._init_process()
        raise NotImplementedError
