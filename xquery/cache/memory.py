#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    Optional,
)

from .base import (
    Cache,
    TKey,
    TValue,
)


class Cache_Memory(Cache):
    """
    Simple in-memory cache service
    """

    def __init__(self):
        self._cache = {}

    def set(self, name: TKey, value: TValue, ttl: Optional[int] = None) -> Any:
        self._cache[name] = value

    def get(self, name: TKey) -> Any:
        try:
            return self._cache[name]
        except KeyError:
            return None

    def remove(self, name: TKey) -> Any:
        try:
            del self._cache[name]
        except KeyError:
            pass

    def ping(self) -> Any:
        return True

    def flush(self) -> Any:
        self._cache = {}
