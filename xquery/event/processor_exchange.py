#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import List

from .processor import (
    EventProcessor,
    StageInfo,
)
from .processor_exchange_bundle import (
    EventProcessorStageExchangePangolin_Bundle,
    EventProcessorStageExchangePegasys_Bundle,
)
from .processor_exchange_count import (
    EventProcessorStageExchangePangolin_Count,
    EventProcessorStageExchangePegasys_Count,
)


class EventProcessorExchange(EventProcessor):

    def __init__(self, stages: List[StageInfo]) -> None:
        """
        Basic exchange event processor

        :param stages: ordered list of processor stages
        """
        super().__init__(stages)


class EventProcessorExchangePangolin(EventProcessorExchange):

    def __init__(self) -> None:
        """
        Event processor for the Pangolin Exchange (on AVAX)
        """
        super().__init__(
            stages=[
                StageInfo("bundle", EventProcessorStageExchangePangolin_Bundle, 1024 * 20),
                StageInfo("count", EventProcessorStageExchangePangolin_Count, None),
            ],
        )


class EventProcessorExchangePegasys(EventProcessorExchange):

    def __init__(self) -> None:
        """
        Event processor for the Pegasys Exchange (on SYS)
        """
        super().__init__(
            stages=[
                StageInfo("bundle", EventProcessorStageExchangePegasys_Bundle, 1024 * 10),
                StageInfo("count", EventProcessorStageExchangePegasys_Count, None),
            ],
        )
