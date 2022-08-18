#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from xquery.util.misc import convert


def test_convert() -> None:
    assert convert([1, 3, "abc", [3, 4, [], 5]]) == (1, 3, "abc", (3, 4, (), 5))
    assert convert((1, 3, "abc", [3, 4, [], 5])) == (1, 3, "abc", (3, 4, (), 5))
    assert convert(sorted({"b": 2, "a": [2, 3]}.items())) == (('a', (2, 3)), ('b', 2))
    assert convert({
        "a": "any",
        "b": ["1", [1, 2, 3], "2", "3",],
        "c": "0x2",
        "d": 52,
        "e": False,
        "f": {"x": 1, "y": [3, 4, [], 5]},
     }) == {
        "a": "any",
        "b": ("1", (1, 2, 3), "2", "3"),
        "c": "0x2",
        "d": 52,
        "e": False,
        "f": {"x": 1, "y": (3, 4, (), 5)}
    }
