#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    Union,
    cast,
)

import orjson

from web3 import HTTPProvider
from web3.types import (
    RPCEndpoint,
    RPCResponse,
)
from web3._utils.request import make_post_request


class BatchHTTPProvider(HTTPProvider):
    """
    Can be removed once the batch feature is added to web3.py
    See: https://github.com/ethereum/web3.py/issues/832
    """

    def decode_rpc_response(self, raw_response: bytes) -> RPCResponse:
        """
        Optimised JSON-RPC decoding

        This greatly improves JSON-RPC API access speeds, when fetching
        multiple or large responses.

        See: https://web3py.readthedocs.io/en/stable/troubleshooting.html#making-ethereum-json-rpc-api-access-faster

        :param raw_response: byte encoded rpc response
        :return:
        """
        decoded = orjson.loads(raw_response)
        return cast(RPCResponse, decoded)

    @staticmethod
    def build_entry(method: RPCEndpoint, params: Any, request_id: int = 1):
        """
        Generate a call entry for a batched request.

        :param method: rpc endpoint
        :param params: method parameters
        :param request_id: index
        :return:
        """
        return {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": request_id,
        }

    def make_batch_request(self, calls: Union[list, tuple]) -> RPCResponse:
        """
        Batched rpc request. Largely based on 'HTTPProvider.make_request()'.

        :param calls: array of method descriptions
        :return:
        """
        text = orjson.dumps(calls)
        self.logger.debug(f"Making request HTTP. URI: {self.endpoint_uri}, Request: {text}")
        raw_response = make_post_request(
            endpoint_uri=self.endpoint_uri,
            data=text,
            **self.get_request_kwargs()
        )
        response = self.decode_rpc_response(raw_response)
        self.logger.debug(f"Getting response HTTP. URI: {self.endpoint_uri}, Request: {text}, Response: {response}")
        return response
