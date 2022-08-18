#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import abc
import logging

log = logging.getLogger(__name__)


class EventProcessor(abc.ABC):
    """
    Event processor base class

    Responsible for:
    - post-process previously indexed event data already present in the database
    - TODO

    The goal of the processing step is calculate/aggregate supplementary information from indexed event log entries.
    """

    @abc.abstractmethod
    def process(self) -> None:
        """
        Process previously indexed event log data from the database.

        Subclasses are expected to:
        - TODO

        :return:
        """
        raise NotImplementedError
