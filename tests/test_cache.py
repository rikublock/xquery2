#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import time

import xquery.cache


def test_cache(c: xquery.cache.Cache) -> None:
    # check service availability
    c.ping()

    # check set/get
    key = "_test_cache"
    values = [
        None,
        b"test",
        False,
        "test",
        1234,
        1234.5,
        ["a", 2, "xyz", True],
        ("a", 2, "xyz", True),
        {"a", 2, "xyz", True},
        {"a": 1, "b": "xyz", "c": True},
    ]

    for value in values:
        c.set(key, value)
        assert c.get(key) == value

    # check entry removal
    c.remove(key)
    assert c.get(key) is None

    # check ttl
    value = "test_value"
    c.set(key, value, ttl=2)
    assert c.get(key) == value
    time.sleep(3.0)
    assert c.get(key) is None
