#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    Optional,
    Union,
)

import abc
import logging

log = logging.getLogger(__name__)

TKey = Union[bytes, str]
TValue = Union[bytes, bool, str, int, float, list, tuple, set, dict]


class Cache(abc.ABC):
    """
    The goal is to add a layer of abstraction in case the underlying cache service
    has to be replaced at some point.
    """

    def __contains__(self, key: TKey) -> bool:
        return self.get(key) is not None

    @abc.abstractmethod
    def set(self, name: TKey, value: TValue, ttl: Optional[int] = None) -> Any:
        """
        Set the value at key ``name`` to ``value``

        :param name:
        :param value:
        :param ttl: sets an expire flag on key ``name`` for ``ttl`` seconds
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, name: TKey) -> Any:
        """
        Return the value at key ``name``, or None if the key doesn't exist

        :param name: key
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def remove(self, name: TKey) -> Any:
        """
        Delete entry with key ``name``

        :param name: key
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def ping(self) -> Any:
        """
        Ping the underlying cache service

        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def flush(self) -> Any:
        """
        Delete all keys in the current database of the cache service

        :return:
        """
        raise NotImplementedError


class Cache_Dummy(Cache):
    """
    Dummy cache service that does nothing
    """

    def __init__(self, *args, **kwargs) -> None:
        pass

    def set(self, name: TKey, value: TValue, ttl: Optional[int] = None) -> Any:
        return True

    def get(self, name: TKey) -> Any:
        return None

    def remove(self, name: TKey) -> Any:
        return True

    def ping(self) -> Any:
        return True

    def flush(self) -> Any:
        return True
