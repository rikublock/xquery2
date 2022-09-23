#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import Union

import hashlib
import itertools
import json
import logging
import time

from decimal import (
    Context,
    Decimal,
    ROUND_HALF_UP,
    setcontext,
    Clamped,
    DivisionByZero,
    FloatOperation,
    InvalidOperation,
    Overflow,
    Subnormal,
    Underflow,
)

log = logging.getLogger(__name__)


def init_decimal_context() -> None:
    """
    Configure the decimal module context

    Note: Needs to be called from every thread
    """
    setcontext(
        Context(
            prec=78,
            rounding=ROUND_HALF_UP,
            traps=[
                Clamped,
                DivisionByZero,
                FloatOperation,
                # Inexact,
                InvalidOperation,
                Overflow,
                # Rounded,
                Subnormal,
                Underflow
            ],
            flags=[],
        )
    )


def token_to_decimal(value: Union[int, Decimal], exp: Union[int, Decimal]) -> Decimal:
    """
    Convert a RC20 token value to a decimal number

    :param value: integer value of token amount
    :param exp: number of significant digits (commonly known as "decimals" in the context of tokens)
    :return:
    """
    v = Decimal(value) / (Decimal("10") ** Decimal(exp))
    return v.quantize(Decimal(f"0.{18 * '0'}"), rounding=ROUND_HALF_UP)
