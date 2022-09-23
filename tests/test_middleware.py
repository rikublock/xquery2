#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import email.utils
import time

import pytest

import requests
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    Timeout,
    TooManyRedirects,
)

from xquery.middleware import (
    _backoff,
    _parse_retry_after,
    http_backoff_retry_request_middleware,
)

# Note: web3.py uses the requests module internally

# TODO fix: unit tests should no depend on an external http server (https://httpbin.org/),


def test_middleware_parse_retry_after() -> None:
    assert _parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == int(time.time() - 1445405280)
    assert _parse_retry_after("Wed, 1 Oct 2025 21:41:59 GMT") == 0  # int(time.time() - 1759347719) < 0
    assert _parse_retry_after(email.utils.formatdate(localtime=True, usegmt=True)) == 0
    assert _parse_retry_after("68") == 68
    assert _parse_retry_after("invalid date") == 0
    assert _parse_retry_after(68) == 68
    assert _parse_retry_after(-68) == 0


def test_middleware_backoff() -> None:
    # default backoff
    b0 = _backoff(
        base=2,
        factor=1,
        max_value=None,
    )

    for v in [1, 2, 4, 8, 16]:
        assert next(b0) == v

    # default backoff with max value
    b1 = _backoff(
        base=2,
        factor=1,
        max_value=10,
    )

    for v in [1, 2, 4, 8, 10, 10, 10]:
        assert next(b1) == v

    # default backoff with factor
    b2 = _backoff(
        base=2,
        factor=3,
        max_value=None,
    )

    for v in [3, 6, 12, 24, 48]:
        assert next(b2) == v

    # custom base backoff
    b3 = _backoff(
        base=7,
        factor=1,
        max_value=None,
    )

    for v in [1, 7, 49, 343, 2401]:
        assert next(b3) == v


def test_middleware_error_connection() -> None:
    def make_request(*args, **kwargs) -> None:
        r = requests.get("https://invalid_url.com/")
        r.raise_for_status()

    mw = http_backoff_retry_request_middleware(
        make_request=make_request,
        w3=None,
        retries=2,
        max_delay=60,
    )

    with pytest.raises(ConnectionError):
        # ensure the method is whitelisted for retries
        mw(method="eth_call", params={})  # type: ignore


def test_middleware_error_http_400() -> None:
    def make_request(*args, **kwargs) -> None:
        r = requests.get("https://httpbin.org/status/400")
        r.raise_for_status()

    mw = http_backoff_retry_request_middleware(
        make_request=make_request,
        w3=None,
        retries=2,
        max_delay=60,
    )

    with pytest.raises(HTTPError):
        # ensure the method is whitelisted for retries
        mw(method="eth_call", params={})  # type: ignore


def test_middleware_error_http_429() -> None:
    def make_request(*args, **kwargs) -> None:
        r = requests.get("https://httpbin.org/status/429")
        r.headers["Retry-After"] = "3"
        r.raise_for_status()

    mw = http_backoff_retry_request_middleware(
        make_request=make_request,
        w3=None,
        retries=5,
        max_delay=60,
    )

    with pytest.raises(HTTPError):
        # ensure the method is whitelisted for retries
        mw(method="eth_call", params={})  # type: ignore


def test_middleware_error_timeout() -> None:
    def make_request(*args, **kwargs) -> None:
        delay = 1
        r = requests.get(f"https://httpbin.org/delay/{delay}", timeout=0.1)
        r.raise_for_status()

    mw = http_backoff_retry_request_middleware(
        make_request=make_request,
        w3=None,  # type: ignore
        retries=2,
        max_delay=60,
    )

    with pytest.raises(Timeout):
        # ensure the method is whitelisted for retries
        mw(method="eth_call", params={})  # type: ignore


def test_middleware_error_redirect() -> None:
    def make_request(*args, **kwargs) -> None:
        N = 5
        s = requests.Session()
        s.max_redirects = N - 1
        r = s.get(f"https://httpbin.org/absolute-redirect/{N}", allow_redirects=True)
        r.raise_for_status()

    mw = http_backoff_retry_request_middleware(
        make_request=make_request,
        w3=None,
        retries=2,
        max_delay=60,
    )

    with pytest.raises(TooManyRedirects):
        # ensure the method is whitelisted for retries
        mw(method="eth_call", params={})  # type: ignore
