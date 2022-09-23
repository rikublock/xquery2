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

import pickle
import redis

from .base import (
    Cache,
    TKey,
    TValue,
)


class Cache_Redis(Cache):
    """
    Simple wrapper around a redis instance

    Note: Currently uses ``pickle`` to convert any python value/object to bytes
    """

    def __init__(self, host: str, port: int, password: Optional[str], db: int) -> None:
        self._redis = redis.Redis(
            host=host,
            port=int(port),
            password=password,
            db=int(db),
        )

    def set(self, name: TKey, value: TValue, ttl: Optional[int] = None) -> Any:
        self._redis.set(name, pickle.dumps(value, protocol=5), ex=ttl)

    def get(self, name: TKey) -> Any:
        try:
            return pickle.loads(self._redis.get(name))
        except TypeError:
            return None

    def remove(self, name: TKey) -> Any:
        self._redis.delete(name)

    def ping(self) -> Any:
        self._redis.ping()

    def flush(self):
        self._redis.flushdb()
