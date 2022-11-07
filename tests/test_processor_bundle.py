#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from decimal import Decimal

from xquery.event.processor_exchange_bundle import (
    EventProcessorStageExchange_Bundle,
    PriceInfo,
)


def test_processor_bundle_price():
    values = [
        (Decimal("1.0"), Decimal("1.0"), 0),
        (Decimal("1.0"), Decimal("1.0"), 1),
        (1, 1, 0),
        (1, 1, 1),

        (Decimal("5.0"), Decimal("2.0"), 0),
        (Decimal("5.0"), Decimal("2.0"), 1),
        (5, 2, 0),
        (5, 2, 1),
    ]

    results = [
        PriceInfo(Decimal("1.0"), Decimal("1.0")),
        PriceInfo(Decimal("1.0"), Decimal("1.0")),
        PriceInfo(Decimal("1.0"), Decimal("1.0")),
        PriceInfo(Decimal("1.0"), Decimal("1.0")),

        PriceInfo(Decimal("2.5"), Decimal("2.0")),
        PriceInfo(Decimal("0.4"), Decimal("5.0")),
        PriceInfo(Decimal("2.5"), Decimal("2.0")),
        PriceInfo(Decimal("0.4"), Decimal("5.0")),
    ]

    for i, value in enumerate(values):
        result = EventProcessorStageExchange_Bundle.calc_price(*value)
        assert results[i] == result, result


def test_processor_bundle_weighted_average():
    values = [
        [
            PriceInfo(Decimal("1.0"), Decimal("1.0")),
            PriceInfo(Decimal("2.0"), Decimal("1.0")),
            PriceInfo(Decimal("3.0"), Decimal("1.0")),
        ],
        [
            PriceInfo(Decimal("1.0"), Decimal("5.0")),
            PriceInfo(Decimal("2.0"), Decimal("2.0")),
            PriceInfo(Decimal("3.0"), Decimal("1.0")),
        ],
        [
            PriceInfo(Decimal("1.07"), Decimal("3.2")),
            PriceInfo(Decimal("2.05"), Decimal("2.8")),
            PriceInfo(Decimal("3.03"), Decimal("1.5")),
        ],
    ]

    results = [
        Decimal("2.0"),
        Decimal("1.5"),
        Decimal("1.827866666666666667"),
    ]

    for i, value in enumerate(values):
        result = EventProcessorStageExchange_Bundle.calc_weighted_average({j: v for j, v in enumerate(value)})
        assert results[i] == result
