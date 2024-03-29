#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    Dict,
)

from web3.types import LogReceipt


class ExtendedLogReceipt(LogReceipt):
    name: str
    dataDecoded: Dict[str, Any]
