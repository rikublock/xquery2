#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from decimal import (
    Decimal,
    ROUND_HALF_UP,
    getcontext,
    Clamped,
    DivisionByZero,
    FloatOperation,
    InvalidOperation,
    Overflow,
    Subnormal,
    Underflow,
)

from xquery.util import token_to_decimal


def test_decimal_context() -> None:
    c = getcontext()

    assert c.prec == 78
    assert c.rounding == ROUND_HALF_UP

    traps = [k for k, v in c.traps.items() if v]
    assert set(traps) == {Clamped, DivisionByZero, FloatOperation, InvalidOperation, Overflow, Subnormal, Underflow}


def test_decimal_quantize() -> None:
    value = 111009028044333631034

    r1 = Decimal(value) / Decimal(1000)
    assert r1 == Decimal("111009028044333631.034")

    r1_q1 = r1.quantize(Decimal(f"0.{2 * '0'}"), rounding=ROUND_HALF_UP)
    assert r1_q1 == Decimal("111009028044333631.03")

    r1_q2 = r1.quantize(Decimal(f"0.{5 * '0'}"), rounding=ROUND_HALF_UP)
    assert r1_q2 == Decimal("111009028044333631.03400")

    r1_q3 = r1.quantize(Decimal(f"0.{18 * '0'}"), rounding=ROUND_HALF_UP)
    assert r1_q3 == Decimal("111009028044333631.034000000000000000")

    r2 = Decimal(value) / (Decimal("10") ** Decimal(18))
    assert r2 == Decimal("111.009028044333631034")

    r2_q1 = r2.quantize(Decimal(f"0.{5 * '0'}"), rounding=ROUND_HALF_UP)
    assert r2_q1 == Decimal("111.00903")

    r2_q2 = r2.quantize(Decimal(f"0.{6 * '0'}"), rounding=ROUND_HALF_UP)
    assert r2_q2 == Decimal("111.009028")

    r2_q3 = r2.quantize(Decimal(f"0.{18 * '0'}"), rounding=ROUND_HALF_UP)
    assert r2_q3 == Decimal("111.009028044333631034")

    r2_q4 = r2.quantize(Decimal(f"0.{20 * '0'}"), rounding=ROUND_HALF_UP)
    assert r2_q4 == Decimal("111.00902804433363103400")


def test_decimal_token() -> None:
    values = [
        111009028044333631034,
        27515117030179501658,
        1922293486939334725,
        138047854643653001,
    ]

    results = [
        Decimal("111.009028044333631034"),
        Decimal("27.515117030179501658"),
        Decimal("1.922293486939334725"),
        Decimal("0.138047854643653001"),
    ]

    for i, value in enumerate(values):
        result = token_to_decimal(value, 18)
        assert results[i] == result
