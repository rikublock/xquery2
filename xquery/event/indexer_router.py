#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    Dict,
    List,
    Optional,
)

import logging

from eth_typing import AnyAddress

from web3 import Web3
from web3.exceptions import (
    BlockNotFound,
    TransactionNotFound,
)
from web3.types import ABI

import xquery.cache
import xquery.contract
import xquery.db.orm as orm
from xquery.types import ExtendedLogReceipt
from xquery.util.misc import compute_xhash
from .indexer import EventIndexer

log = logging.getLogger(__name__)


class EventIndexerBase(EventIndexer):

    def __init__(self, w3: Web3, cache: xquery.cache.Cache, chain: orm.Chains = None) -> None:
        self.w3 = w3
        self._cache = cache
        self._chain = chain

    def _get_block_timestamp(self, hash_: str) -> Optional[int]:
        """
        Get block timestamp

        :param hash_: block hash
        :return:
        """
        key = f"block_{hash_}".lower()
        timestamp = self._cache.get(key)

        if not timestamp:
            try:
                block_info = self.w3.eth.get_block(hash_)
            except BlockNotFound:
                log.error(f"Failed to fetch block '{hash_}'")
                return None

            timestamp = int(block_info["timestamp"])
            self._cache.set(key, timestamp, ttl=300)

        return timestamp

    def process(self, entry: ExtendedLogReceipt) -> Dict[str, Any]:
        """
        Only process the most basic information required for any XQuery, namely:
            - xhash
            - chain
            - block_height
            - block_hash
            - timestamp
            - tx_hash
            - event_name

        :param entry: event log entry
        :return:
        """
        # TODO Currently we cannot handle this (prevent db corruption)
        assert not entry.removed

        xhash = compute_xhash(entry)
        timestamp = self._get_block_timestamp(entry.blockHash.hex())
        assert timestamp is not None

        result = {
            "xhash": xhash,
            "chain": self._chain,
            "block_height": entry.blockNumber,
            "block_hash": entry.blockHash.hex(),
            "block_timestamp": timestamp,
            "tx_hash": entry.transactionHash.hex(),
            "event_name": entry.name,
        }

        return result


class EventIndexer_Router(EventIndexerBase):

    def __init__(self, w3: Web3, cache: xquery.cache.Cache, chain: orm.Chains, abi_rc20: ABI, abi_pair: ABI, abis: List[ABI]) -> None:
        """
        Deprecated indexer used only in classic XQuery.

        :param w3: web3 provider
        :param cache: cache service
        :param chain: blockchain
        :param abi_rc20: abi of an RC20 token contract
        :param abi_pair: abi of a Pair contract
        :param abis: list of abis used to find the function identifier
        """
        super().__init__(w3, cache, chain)

        self._abi_rc20 = abi_rc20
        self._abi_pair = abi_pair
        self._abis = abis

    def _get_function_name(self, entry: ExtendedLogReceipt) -> Optional[str]:
        """
        Find the function identifier from the caller contract.

        Note: Only searches in the list of provided abis. Might not necessarily find a matching function.

        :param entry: event log entry
        :return: function identifier or None if not found
        """
        tx_hash = entry.transactionHash.hex()

        key = f"_tx_{tx_hash}".lower()
        identifier = self._cache.get(key)

        if not identifier:
            try:
                tx = self.w3.eth.get_transaction(tx_hash)
            except TransactionNotFound:
                log.error(f"Failed to fetch tx '{tx_hash}'")
                return None

            for abi in self._abis:
                try:
                    contract = self.w3.eth.contract(abi=abi)
                    func_obj, func_params = contract.decode_function_input(tx.input)
                except ValueError:
                    continue

                identifier = func_obj.function_identifier
                self._cache.set(key, identifier, ttl=300)

        return identifier

    def _get_token_info(self, address: AnyAddress) -> Optional[Dict[str, Any]]:
        """
        Fetch token info from a rc20 contract

        Example result:
        {
            "name": str,
            "symbol": str,
            "decimals": int
        }

        :param address: rc20 contract address
        :return: token info or None if not found
        """
        address = Web3.toChecksumAddress(address)

        key = f"_token_{address}".lower()
        token = self._cache.get(key)

        if not token:
            contract = self.w3.eth.contract(address=address, abi=self._abi_rc20)

            # TODO convert to batch request
            try:
                name = contract.functions.name().call()
                symbol = contract.functions.symbol().call()
                decimals = contract.functions.decimals().call()
            except Exception:
                log.error(f"Failed to fetch token info from rc20 contract '{address}'")
                return None

            token = {
                "name": name,
                "symbol": symbol,
                "decimals": int(decimals),
            }

            self._cache.set(key, token)

        return token

    def _get_pair_info(self, address: AnyAddress) -> Optional[List[str]]:
        """
        Fetch token addresses from a pair contract

        Should be called for events related to the pair contract:
            - Mint
            - Burn
            - Swap
            - Sync

        Note: Assumes that the address values are immutable once the contract is fully deployed.

        :param address: pair contract address
        :return: token addresses or None if not found
        """
        address = Web3.toChecksumAddress(address)

        key = f"_pair_{address}".lower()
        addresses = self._cache.get(key)

        if not addresses:
            contract = self.w3.eth.contract(address=address, abi=self._abi_pair)

            # TODO convert to batch request
            try:
                token0_address = contract.functions.token0().call()
                token1_address = contract.functions.token1().call()
            except Exception:
                log.error(f"Failed to fetch token addresses from pair contract '{address}'")
                return None

            addresses = [
                token0_address,
                token1_address,
            ]
            self._cache.set(key, addresses)

        return addresses

    def _process_args(self, entry: ExtendedLogReceipt) -> Dict[str, Any]:
        """
        Process the decoded event log entry data (function arguments)

        :param entry: event log entry
        :return:
        """
        result = {}
        args = entry.dataDecoded

        # TODO refactor: use structural pattern matching once available
        if entry.name in ["Mint", "Burn", "Swap", "Sync"]:
            token_addresses = self._get_pair_info(address=Web3.toChecksumAddress(entry.address))

            if token_addresses:
                assert len(token_addresses) == 2

                token0 = self._get_token_info(address=Web3.toChecksumAddress(token_addresses[0]))
                token1 = self._get_token_info(address=Web3.toChecksumAddress(token_addresses[1]))

                if token0 and token1:
                    result.update({
                        "token0_name": token0["name"],
                        "token0_symbol": token0["symbol"],
                        "token0_decimals": token0["decimals"],
                        "token1_name": token1["name"],
                        "token1_symbol": token1["symbol"],
                        "token1_decimals": token1["decimals"],
                    })

            if entry.name == "Mint":
                result.update({
                    "address_sender": args.sender,
                    "amount0": args.amount0,
                    "amount1": args.amount1,
                })
            elif entry.name == "Burn":
                result.update({
                    "address_sender": args.sender,
                    "address_to": args.to,
                    "amount0": args.amount0,
                    "amount1": args.amount1,
                })
            elif entry.name == "Swap":
                result.update({
                    "address_sender": args.sender,
                    "address_to": args.to,
                    "amount0_in": args.amount0In,
                    "amount0_out": args.amount0Out,
                    "amount1_in": args.amount1In,
                    "amount1_out": args.amount1Out,
                })
            elif entry.name == "Sync":
                result.update({
                    "reserve0": args.reserve0,
                    "reserve1": args.reserve1,
                })

        elif entry.name in ["Approval", "Transfer", "Deposit", "Withdrawal"]:
            token0 = self._get_token_info(address=Web3.toChecksumAddress(entry.address))

            if token0:
                result.update({
                    "token0_name": token0["name"],
                    "token0_symbol": token0["symbol"],
                    "token0_decimals": token0["decimals"],
                })

            if entry.name == "Approval":
                result.update({
                    "address_sender": args.owner,
                    "address_to": args.spender,
                    "value": args.value,
                })
            elif entry.name == "Transfer":
                result.update({
                    "address_sender": args["from"],
                    "address_to": args.to,
                    "value": args.value,
                })
            elif entry.name == "Deposit":
                result.update({
                    "address_to": args.dst,
                    "value": args.wad,
                })
            elif entry.name == "Withdrawal":
                result.update({
                    "address_sender": args.src,
                    "value": args.wad,
                })

        else:
            log.warning(f"Encountered unknown event {entry.name} while processing event data")

        return result

    def process(self, entry: ExtendedLogReceipt) -> Dict[str, Any]:
        """
        Process remaining event information required for any XQuery, namely:

        Note: Certain fields might be left empty, if not available.

        All contracts:
            - func_identifier
            - address_sender
            - address_to
            - token0_name
            - token0_symbol
            - token0_decimals

        RC20, WAVAX contracts:
            # Approval, Transfer, Deposit, Withdrawal
            - value

        Pair Contract:
            - token1_name
            - token1_symbol
            - token1_decimals

            # Mint, Burn
            - amount0
            - amount1

            # Swap
            - amount0_in
            - amount1_in
            - amount0_out
            - amount1_out

            # Sync
            - reserve0
            - reserve1

        :param entry: event log entry
        :return:
        """
        result = super().process(entry)

        func_identifier = self._get_function_name(entry)
        result.update({
            "func_identifier": func_identifier,
        })

        # process event data (args)
        result.update(self._process_args(entry))

        return result


class EventIndexer_Pangolin(EventIndexer_Router):

    def __init__(self, w3: Web3, cache: xquery.cache.Cache):
        """
        Indexer for the Pangolin Exchange
        """
        super().__init__(
            w3=w3,
            cache=cache,
            chain=orm.Chains.AVAX,
            abi_rc20=xquery.contract.png_rc20.abi,
            abi_pair=xquery.contract.png_pair.abi,
            abis=[
                xquery.contract.png_router.abi,
                xquery.contract.png_pair.abi,
                xquery.contract.png_rc20.abi,
                xquery.contract.png_wavax.abi,
            ],
        )
