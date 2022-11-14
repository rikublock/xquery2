#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Dict,
    List,
    Tuple,
    Type,
    Union,
)

import logging
import pprint

from dataclasses import dataclass
from decimal import (
    Decimal,
    ROUND_HALF_UP,
)

from eth_typing import ChecksumAddress
from sqlalchemy import select
from web3 import Web3

import xquery.cache
import xquery.db
import xquery.db.orm as orm
from xquery.util import (
    MAX_DECIMAL_PLACES,
    split_interval,
)

from .processor import EventProcessorStage

log = logging.getLogger(__name__)


@dataclass
class PairInfo(object):
    """
    Used to configure tracked pairs. The ``order`` determines the denominator for the price calculations.

    Attributes:
        address: pair contract checksum address
        order: position of the denominator token in the pair (either 0 or 1)
    """
    address: ChecksumAddress
    order: int


@dataclass
class PriceInfo(object):
    """
    Used to store current price information for averaging.

    Attributes:
        price: current token exchange rate
        weight: absolute liquidity of denominator token
    """
    price: Decimal
    weight: Decimal


class EventProcessorStageExchange_Bundle(EventProcessorStage):

    def __init__(self, db: xquery.db.FusionSQL, cache: xquery.cache.Cache, pair_infos: List[PairInfo], default_price: Decimal) -> None:
        """
        Exchange processor stage to compute the USD/native price for a given range of blocks.

        The current price value (in USD) for an arbitrary (block, logIndex) tuple can be determined by
        loading the first price at position <= (block, logIndex).

        Computes the following data:
          - bundle table

        Depends on:
          - None

        Note: Only adds a new bundle entry when the price actually changes (not at every block height).
        Note: Initializing a new worker has a certain overhead, job intervals should therefore be large enough.

        :param db: database service
        :param cache: cache service
        :param pair_infos: tracked pairs with token order
        :param default_price: default price in USD used for initialization
        """
        super().__init__(db, cache)

        self._pair_infos = {info.address: info.order for info in pair_infos}
        self._default_price = default_price

        # cache for tracking the latest price (exchange rate) and weight (liquidity) of each relevant pair
        self._price_infos = {}

    @staticmethod
    def calc_price(a: Union[int, Decimal], b: Union[int, Decimal], order: Union[int, bool]) -> PriceInfo:
        """
        Calculate the ``price`` value for two non-zero values ``a`` and ``b``

        Note: Values are assume to be strictly greater than zero (a, b > 0)
        Note: Always uses the weight of the denominator token (depending on ``order``)

        :param a: reserve of first token
        :param b: reserve of second token
        :param order: if True, use (b / a), else (a / b)
        :return:
        """
        if bool(order):
            v = Decimal(b) / Decimal(a)
            weight = a
        else:
            v = Decimal(a) / Decimal(b)
            weight = b

        return PriceInfo(v.quantize(Decimal(f"0.{MAX_DECIMAL_PLACES * '0'}"), rounding=ROUND_HALF_UP), weight)

    @staticmethod
    def calc_weighted_average(price_infos: Dict[str, PriceInfo]) -> Decimal:
        """
        Calculate the weighted average from a list of price infos

        Details:
          sum(price * weight) / total_weight

        :param price_infos: current prices with weight
        :return:
        """
        total_value = Decimal(0)
        total_weight = Decimal(0)
        for info in price_infos.values():
            total_value += info.price * info.weight
            total_weight += info.weight

        if total_weight == Decimal(0):
            result = Decimal(0)
        else:
            result = (total_value / total_weight)

        return result.quantize(Decimal(f"0.{MAX_DECIMAL_PLACES * '0'}"), rounding=ROUND_HALF_UP)

    def _find_initial_price(self, start_block: int, pair_address: str, order: int) -> PriceInfo:
        """
        Look for the first occurrence of a sync event strictly before ``start_block`` for each tracked pair in order
        to deduce an initial price value. In the case that no event is found, default to price 0 and weight 0.

        :param start_block: first block (start of interval)
        :param pair_address: tracked pair addresses
        :return:
        """
        with self._db.session() as session:
            result = session.execute(
                select(orm.Sync)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number < start_block)
                    .filter(orm.Sync.pair_address == pair_address)
                    .order_by(orm.Block.number.desc(), orm.Sync.logIndex.desc())
                    .limit(1)
            ).scalar()

            if result is None:
                return PriceInfo(Decimal(0), Decimal(0))
            else:
                # Note: the price depends on the token order in a pair
                return self.__class__.calc_price(result.reserve0, result.reserve1, order)

    def _init_prices(self, start_block: int, pair_addresses: List[str]) -> List[dict]:
        """
        Initialize or reset the price info cache with an initial price for tracked pairs.

        Note: Computes and returns a "transition" price

        :param start_block: first block (start of interval)
        :param pair_addresses: tracked pair addresses
        :return:
        """
        self._price_infos = {}
        for addr in pair_addresses:
            order = self._pair_infos[addr]
            self._price_infos[addr] = self._find_initial_price(start_block, addr, order)

        log.debug(f"Initial prices:\n{pprint.pformat(self._price_infos)}")

        if sum(info.weight for info in self._price_infos.values()) > Decimal(0):
            price = self.__class__.calc_weighted_average(self._price_infos)
        else:
            price = self._default_price.quantize(Decimal(f"0.{MAX_DECIMAL_PLACES * '0'}"), rounding=ROUND_HALF_UP)

        # Note: Ideally we would want to use the block at height a where the interval starts. However, that specific
        #       block might not necessarily be available (not indexed). Instead, we settle for the first block <= a
        #       and set the max logIndex. This will yield the same result when looking up price values.
        with self._db.session() as session:
            block = session.execute(
                select(orm.Block)
                    .filter(orm.Block.number < start_block)
                    .order_by(orm.Block.number.desc())
                    .limit(1)
            ).scalar()

            # Ensure that the "transition" bundle is only created once in case of subsequent runs
            if block:
                bundle = session.execute(
                    select(orm.Bundle)
                        .filter((orm.Bundle.block_id == block.id) & (orm.Bundle.logIndex == 0x7FFFFFFF))
                ).scalar()

                # already exists
                if bundle:
                    assert bundle.nativePrice == price
                    return []

        # Note: using simple dict instead of an orm object for better performance
        bundle = {
            "nativePrice": price,
            "block_id": block.id if block else None,
            "logIndex": 0x7FFFFFFF,
        }

        return [bundle]

    def _process(self, start_block: int, end_block: int, pair_addresses: List[str]) -> List[dict]:
        """
        Compute the weighted price average and create a new bundle object each time the price of one of
        the tracked pairs changes (sync event).

        Note: Updates the price info cache

        :param start_block: first block (start of interval)
        :param end_block: last block (included in the computation)
        :param pair_addresses: tracked pair addresses
        :return:
        """
        with self._db.session() as session:
            results = session.execute(
                select(orm.Sync)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number.between(start_block, end_block))
                    .filter(orm.Sync.pair_address.in_(pair_addresses))
                    .order_by(orm.Block.number.asc(), orm.Sync.logIndex.asc())
            ).yield_per(1000).scalars()

            objects = []
            for sync in results:
                addr = sync.pair_address

                # calculate the weighted average price USD/native
                order = self._pair_infos[addr]
                price_info = self.__class__.calc_price(sync.reserve0, sync.reserve1, order)
                self._price_infos[addr] = price_info

                price = self.__class__.calc_weighted_average(self._price_infos)

                # Note: using simple dict instead of an orm object for better performance
                bundle = {
                    "nativePrice": price,
                    "block_id": sync.transaction.block_id,
                    "logIndex": sync.logIndex,
                }

                objects.append(bundle)

            return objects

    @classmethod
    def setup(cls, db: xquery.db.FusionSQL, start_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        return []

    def process(self, start_block: int, end_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        assert start_block <= end_block

        pair_addresses = list(self._pair_infos.keys())
        objects = self._init_prices(start_block, pair_addresses)
        result = self._process(start_block, end_block, pair_addresses)
        objects.extend(result)

        if len(objects) > 0:
            return [(orm.Bundle, objects)]
        else:
            return []


class EventProcessorStageExchangePangolin_Bundle(EventProcessorStageExchange_Bundle):

    pair_AEB_USDT_WAVAX = Web3.toChecksumAddress("0x9EE0a4E21bd333a6bb2ab298194320b8DaA26516")  # created block 60337
    pair_AEB_DAI_WAVAX = Web3.toChecksumAddress("0x17a2E8275792b4616bEFb02EB9AE699aa0DCb94b")  # created block 60355
    pair_AB_DAI_WAVAX = Web3.toChecksumAddress("0xbA09679Ab223C6bdaf44D45Ba2d7279959289AB0")  # created block 2781964
    pair_AB_USDT_WAVAX = Web3.toChecksumAddress("0xe28984e1EE8D431346D32BeC9Ec800Efb643eef4")  # created block 2781997

    def __init__(self, db: xquery.db.FusionSQL, cache: xquery.cache.Cache) -> None:
        """
        Event processor stage "bundle" for the Pangolin Exchange (on AVAX)
        """
        super().__init__(
            db=db,
            cache=cache,
            pair_infos=[
                PairInfo(self.__class__.pair_AEB_USDT_WAVAX, 1),
                PairInfo(self.__class__.pair_AEB_DAI_WAVAX, 1),
                PairInfo(self.__class__.pair_AB_DAI_WAVAX, 1),
                PairInfo(self.__class__.pair_AB_USDT_WAVAX, 1),
            ],
            default_price=Decimal("30.0"),
        )

    def process(self, start_block: int, end_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        assert start_block <= end_block

        # Note: Pangolin is a special case as tracked pairs change depending on block height
        AEB_USDT_WAVAX_PAIR_BLOCK = 60337
        AEB_DAI_WAVAX_PAIR_BLOCK = 60355
        AB_MIGRATION_CUTOVER_BLOCK = 3117207

        intervals = split_interval(
            a=start_block,
            b=end_block,
            values=[
                AEB_USDT_WAVAX_PAIR_BLOCK,
                AEB_DAI_WAVAX_PAIR_BLOCK,
                AB_MIGRATION_CUTOVER_BLOCK,
            ],
        )

        objects = []
        for a, b in intervals:
            if a > AB_MIGRATION_CUTOVER_BLOCK:
                pair_addresses = [self.__class__.pair_AB_DAI_WAVAX, self.__class__.pair_AB_USDT_WAVAX]

            elif a > AEB_DAI_WAVAX_PAIR_BLOCK:
                assert b <= AB_MIGRATION_CUTOVER_BLOCK
                pair_addresses = [self.__class__.pair_AEB_USDT_WAVAX, self.__class__.pair_AEB_DAI_WAVAX]

            elif a > AEB_USDT_WAVAX_PAIR_BLOCK:
                assert b <= AEB_DAI_WAVAX_PAIR_BLOCK
                pair_addresses = [self.__class__.pair_AEB_USDT_WAVAX]

            else:
                assert b <= AEB_USDT_WAVAX_PAIR_BLOCK
                pair_addresses = []

            result = self._init_prices(a, pair_addresses)
            objects.extend(result)
            result = self._process(a, b, pair_addresses)
            objects.extend(result)

        if len(objects) > 0:
            return [(orm.Bundle, objects)]
        else:
            return []


class EventProcessorStageExchangePegasys_Bundle(EventProcessorStageExchange_Bundle):

    def __init__(self, db: xquery.db.FusionSQL, cache: xquery.cache.Cache) -> None:
        """
        Event processor stage "bundle" for the Pegasys Exchange (on SYS)
        """
        pair_DAI_WSYS = Web3.toChecksumAddress("0x3DE7BEE2cA971f3D3D7dD04bE028161912513d55")  # created block 40971
        pair_USDC_WSYS = Web3.toChecksumAddress("0x2CDF912CbeaF76d67feaDC994D889c2F4442b300")  # created block 40154
        pair_USDT_WSYS = Web3.toChecksumAddress("0x0Df7d92a4DB09d3828a725D039B89FDC8dfC96A6")  # created block 40928

        super().__init__(
            db=db,
            cache=cache,
            pair_infos=[
                PairInfo(pair_DAI_WSYS, 1),
                PairInfo(pair_USDC_WSYS, 0),
                PairInfo(pair_USDT_WSYS, 0),
            ],
            default_price=Decimal("0.0"),
        )
