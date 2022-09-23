#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    Callable,
    Generator,
    Optional,
    Union,
)

import time
import logging
import email.utils

from requests.exceptions import (
    ConnectionError,
    HTTPError,
    Timeout,
    TooManyRedirects,
)

from web3 import Web3
from web3.middleware.exception_retry_request import check_if_retry_on_failure
from web3.types import (
    RPCEndpoint,
    RPCResponse,
)

log = logging.getLogger(__name__)


def _parse_retry_after(value: Union[str, int]) -> int:
    """
    Determine a delay time in seconds from a Retry-After header of a HTTP 429.
    In case of an error or a negative value, 0 is returned.

    See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After

    :param value: Retry-After header entry
    :return: number of seconds to sleep
    """

    try:
        return max(0, int(value))
    except ValueError:
        pass

    try:
        t = email.utils.parsedate(value)
        delay = int(time.time() - time.mktime(t))
        return max(0, delay)
    except TypeError:
        pass

    return 0


def _backoff(
    base: int = 2,
    factor: int = 1,
    max_value: Optional[int] = None
) -> Generator[int, Any, None]:
    """
    Generator for exponential growth.

    Taken from https://github.com/litl/backoff

    :param base: The mathematical base of the exponentiation operation
    :param factor: Factor to multiply the exponentiation by.
    :param max_value: The maximum value to yield. Once the value in the true exponential
        sequence exceeds this, the value of max_value will forever after be yielded.
    :return:
    """
    n = 0
    while True:
        a = factor * base ** n
        if max_value is None or a < max_value:
            yield a
            n += 1
        else:
            yield max_value


def http_backoff_retry_request_middleware(
    make_request: Callable[[RPCEndpoint, Any], RPCResponse],
    w3: Web3,
    retries: int = 5,
    max_delay: int = 60,
) -> Callable[[RPCEndpoint, Any], RPCResponse]:
    """
    Creates middleware that retries failed HTTP requests with an exponential backoff.
    Additionally, the middleware tries to honor HTTP 429 (Too Many Requests) headers.

    Note: cannot handle 'eth_getLogs' throttle errors

    :param make_request:
    :param w3: web3 provider
    :param retries: max number of retries
    :param max_delay: max sleep delay in seconds
    :return:
    """
    def middleware(method: RPCEndpoint, params: Any) -> RPCResponse:
        if check_if_retry_on_failure(method):
            delay_gen = _backoff(max_value=max_delay)
            for i in range(retries):
                try:
                    return make_request(method, params)
                except (ConnectionError, HTTPError, Timeout, TooManyRedirects) as e:
                    if i < retries - 1:
                        # check for specific 429 HTTPError
                        retry_after = 0
                        if e.response is not None and e.response.status_code == 429:
                            retry_after = e.response.headers.get("Retry-After", 0)
                            retry_after = _parse_retry_after(retry_after)
                            log.warning(f"Encountered rate limiting (retry-after: {retry_after}s)")

                        delay = min(retry_after, max_delay)
                        # if retry_after > max_delay:
                        #     raise

                        # exponential backoff (retry_after has precedence)
                        delay = max(next(delay_gen), delay)
                        log.debug(f"HTTP request retry backoff: {delay}s")
                        time.sleep(delay)

                        continue
                    else:
                        raise
            return None
        else:
            return make_request(method, params)
    return middleware
