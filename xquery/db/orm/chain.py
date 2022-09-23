#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import enum


@enum.unique
class Chain(enum.IntEnum):
    UNKNOWN = 0
    ETH = 1
    AVAX = 43114
    SYS = 57
