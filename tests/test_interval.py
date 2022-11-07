#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from xquery.util import split_interval


def test_interval_split() -> None:
    assert split_interval(1, 8, [5, 5, 3]) == [(1, 3), (4, 5), (6, 8)]
    assert split_interval(1, 8, [5, 3]) == [(1, 3), (4, 5), (6, 8)]

    assert split_interval(1, 8, [-5]) == [(1, 8)]
    assert split_interval(1, 8, [0]) == [(1, 8)]
    assert split_interval(1, 8, [0, 4]) == [(1, 4), (5, 8)]
    assert split_interval(1, 8, [9]) == [(1, 8)]
    assert split_interval(1, 8, [3, 9]) == [(1, 3), (4, 8)]

    assert split_interval(1, 8, [1]) == [(1, 1), (2, 8)]
    assert split_interval(1, 8, [8]) == [(1, 8)]

    assert split_interval(1, 8, [4]) == [(1, 4), (5, 8)]
    assert split_interval(1, 8, [7]) == [(1, 7), (8, 8)]
    assert split_interval(1, 8, [3, 4]) == [(1, 3), (4, 4), (5, 8)]
    assert split_interval(1, 8, [4, 7]) == [(1, 4), (5, 7), (8, 8)]
    assert split_interval(1, 8, [4, 7, 8]) == [(1, 4), (5, 7), (8, 8)]
