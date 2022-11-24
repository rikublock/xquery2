#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Optional,
    Tuple,
)

import logging

from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import func

import xquery.db.orm as orm
from xquery.cache import Cache
from xquery.db import FusionSQL

from .processor import EventProcessorStage

log = logging.getLogger(__name__)


class EventProcessorStageExchange_Stats(EventProcessorStage):

    def __init__(self, db: FusionSQL, cache: Cache) -> None:
        """
        Exchange processor stage to compute hourly and daily statistics.

        Computes the following data:
          - table pair_hour_data
          - table pair_day_data

        TODO Currently missing:
          - pair_hour_data.reserveUSD
          - pair_hour_data.hourlyVolumeUSD
          - table token_hour_data
          - table token_day_data

        Depends on:
          - stage: bundle
          - stage: derived
          - stage: amount

        :param db: database service
        :param cache: cache service
        :return:
        """
        super().__init__(db, cache)

        self._local_cache_pair = {}
        self._local_cache_token = {}

    @classmethod
    def _check_timestamps(cls, db: FusionSQL, start_block: int, end_block: int) -> bool:
        """
        Check that ascending blocks have a strictly larger or equal timestamp

        TODO There is likely a more efficient way to do this with functions

        :param db: database service
        :param start_block: first block
        :param end_block: last block
        :return:
        """
        with db.session() as session:
            results = session.execute(
                select(orm.Block.timestamp)
                    .filter(orm.Block.number.between(start_block, end_block))
                    .order_by(orm.Block.number.asc())
            ).yield_per(1000).scalars()

            previous_timestamp = 0
            for timestamp in results:
                if timestamp < previous_timestamp:
                    return False
                previous_timestamp = timestamp

            return True

    @classmethod
    def _find_timestamp_window(cls, db: FusionSQL, start_block: int, end_block: int, size: int) -> Tuple[Optional[int], Optional[int]]:
        """
        Determine the unix timestamp window given a range of blocks

        Assumption:
          - if there is an event in the (start, end) block interval, we would also have indexed the
            corresponding transaction and block
          - ascending blocks have a strictly larger timestamp

        Note:
          - a block interval might not necessarily align with a unix timestamp window
                     [      interval 1      ][      interval 2      ]...
              [ window timestamps 1  ][ window timestamps 2  ]...
          - can depend on blocks before this interval, never on any blocks after the interval (might not exist)
          - it is possible that there are no blocks in a certain interval (include it in the next window)

        :param db: database service
        :param start_block: first block (start of interval)
        :param end_block: last block (included in the computation)
        :param size: length of the timestamp window in seconds
        :return:
        """
        assert start_block <= end_block

        with db.session() as session:
            block_last = session.execute(
                select(orm.Block)
                    .filter(orm.Block.number <= end_block)
                    .filter(orm.Block.number.between(start_block, end_block))
                    .order_by(orm.Block.number.desc())
                    .limit(1)
            ).scalar()

            # no blocks found in this interval
            if block_last is None:
                return None, None

            block_first = session.execute(
                select(orm.Block)
                    .filter(orm.Block.number >= start_block)
                    .filter(orm.Block.number.between(start_block, end_block))
                    .order_by(orm.Block.number.asc())
                    .limit(1)
            ).scalar()

            # the "first" relevant block is the last block from the previous interval (ensure continuous timestamps)
            block_first_relevant = session.execute(
                select(orm.Block)
                    .filter(orm.Block.number < start_block)
                    .order_by(orm.Block.number.desc())
                    .limit(1)
            ).scalar()

            if block_first.timestamp == block_first_relevant.timestamp:
                log.warning(f"Detected identical timestamp ({block_first_relevant.number} -> {block_first.number})")

            if block_first_relevant:
                index_first = block_first_relevant.timestamp // size
            else:
                index_first = 0

            index_last = block_last.timestamp // size

            # range not large enough
            if index_first == index_last:
                return None, None

            start_timestamp = index_first * size
            end_timestamp = index_last * size - 1

            return start_timestamp, end_timestamp

    def _get_pair_hour_data(self, index, pair_address, size: int) -> dict:
        """
        Get the ``hour_data`` dict at ``index`` for a ``pair_address``.

        Cache structure:
        {
            "hour_index": {
                "addr1": hour_data,
                "addr2": hour_data,
                ...
            },
            ...
        }

        :param index: hour index
        :param pair_address: pair contract checksum address
        :param size: length of the timestamp window in seconds
        :return:
        """
        entries = self._local_cache_pair.setdefault(index, {})
        if pair_address in entries:
            hour_data = entries[pair_address]
        else:
            hour_data = entries.setdefault(pair_address, {
                "hourIndex": index,
                "hourStartUnix": index * size,
                "pair_address": pair_address,
                "reserve0": None,
                "reserve1": None,
                "reserveUSD": Decimal(0),
                "totalSupply": None,
                "totalSupplyChange": Decimal(0),
                "hourlyVolumeToken0": Decimal(0),
                "hourlyVolumeToken1": Decimal(0),
                "hourlyVolumeUSD": Decimal(0),
                "hourlyTxns": 0,
            })

        return hour_data

    def _get_token_hour_data(self, index, token_address, size: int) -> dict:
        """
        Get the ``hour_data`` dict at ``index`` for a ``token_address``.

        Cache structure:
        {
            "hour_index": {
                "addr1": hour_data,
                "addr2": hour_data,
                ...
            },
            ...
        }

        :param index: hour index
        :param token_address: token contract checksum address
        :param size: length of the timestamp window in seconds
        :return:
        """
        entries = self._local_cache_token.setdefault(index, {})
        if token_address in entries:
            hour_data = entries[token_address]
        else:
            hour_data = entries.setdefault(token_address, {
                "hourIndex": index,
                "hourStartUnix": index * size,
                "token_address": token_address,
                "hourlyVolumeToken": Decimal(0),
                "hourlyVolumeNative": Decimal(0),
                "hourlyVolumeUSD": Decimal(0),
                "hourlyTxns": 0,
                "totalLiquidityToken": None,
                "totalLiquidityTokenChange": Decimal(0),
                "totalLiquidityNative": None,
                "totalLiquidityUSD": None,
                "priceUSD": Decimal(0),
            })

        return hour_data

    @classmethod
    def setup(cls, db: FusionSQL, first_block: int) -> orm.TDBObjs:
        return []

    @classmethod
    def pre_process(cls, db: FusionSQL, cache: Cache, start_block: int, end_block: int) -> None:
        # the "first" relevant block is the last from the previous interval (ensure continuous timestamps)
        with db.session() as session:
            block_first = session.execute(
                select(orm.Block)
                    .filter(orm.Block.number < start_block)
                    .order_by(orm.Block.number.desc())
                    .limit(1)
            ).scalar()

        if block_first is None:
            assert cls._check_timestamps(db, 0, end_block)
        else:
            assert cls._check_timestamps(db, block_first.number, end_block)

    def process(self, start_block: int, end_block: int) -> orm.TDBObjs:
        assert start_block <= end_block
        size = 3600

        start_timestamp, end_timestamp = self.__class__._find_timestamp_window(self._db, start_block, end_block, size)
        if start_timestamp is None:
            return []

        assert start_timestamp < end_timestamp

        with self._db.session() as session:
            mints = session.execute(
                select(orm.Mint)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.timestamp.between(start_timestamp, end_timestamp))
                    .order_by(orm.Block.number.asc())
                    .options(joinedload(orm.Mint.transaction).subqueryload(orm.Transaction.block))
            ).yield_per(1000).scalars()

            for mint in mints:
                index = mint.transaction.block.timestamp // size
                hour_data = self._get_pair_hour_data(index, mint.pair_address, size)
                hour_data["totalSupplyChange"] += mint.liquidity
                hour_data["totalSupplyChange"] += Decimal(0) if mint.feeLiquidity is None else mint.feeLiquidity
                hour_data["hourlyTxns"] += 1

            burns = session.execute(
                select(orm.Burn)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.timestamp.between(start_timestamp, end_timestamp))
                    .order_by(orm.Block.number.asc())
                    .options(joinedload(orm.Burn.transaction).subqueryload(orm.Transaction.block))
            ).yield_per(1000).scalars()

            for burn in burns:
                index = burn.transaction.block.timestamp // size
                hour_data = self._get_pair_hour_data(index, burn.pair_address, size)
                hour_data["totalSupplyChange"] -= burn.liquidity
                hour_data["totalSupplyChange"] += Decimal(0) if burn.feeLiquidity is None else burn.feeLiquidity
                hour_data["hourlyTxns"] += 1

            # TODO temporary: replace with values from the "amount" table once available
            swaps = session.execute(
                select(orm.Swap)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.timestamp.between(start_timestamp, end_timestamp))
                    .order_by(orm.Block.number.asc())
                    .options(joinedload(orm.Swap.pair))
                    .options(joinedload(orm.Swap.transaction).subqueryload(orm.Transaction.block))
            ).yield_per(1000).scalars()

            for swap in swaps:
                index = swap.transaction.block.timestamp // size
                amount0_total = swap.amount0Out + swap.amount0In
                amount1_total = swap.amount1Out + swap.amount1In

                hour_data = self._get_pair_hour_data(index, swap.pair_address, size)
                hour_data["hourlyVolumeToken0"] += amount0_total
                hour_data["hourlyVolumeToken1"] += amount1_total
                hour_data["hourlyTxns"] += 1

                hour_data = self._get_token_hour_data(index, swap.pair.token0_address, size)
                hour_data["hourlyVolumeToken"] += amount0_total
                hour_data["hourlyTxns"] += 1

                hour_data = self._get_token_hour_data(index, swap.pair.token1_address, size)
                hour_data["hourlyVolumeToken"] += amount1_total
                hour_data["hourlyTxns"] += 1

            # TODO load and aggregate token liquidity change

            # find last sync for that pair/interval
            # fixme: seems to be expensive
            for index, entries in self._local_cache_pair.items():
                timestamp = (index + 1) * size - 1
                for pair_address, hour_data in entries.items():
                    sync = session.execute(
                        select(orm.Sync)
                            .filter(orm.Sync.pair_address == pair_address)
                            .join(orm.Transaction)
                            .join(orm.Block)
                            .filter(orm.Block.timestamp <= timestamp)
                            .order_by(orm.Block.number.desc(), orm.Sync.logIndex.desc())
                            .limit(1)
                    ).scalar()

                    hour_data["reserve0"] = sync.reserve0
                    hour_data["reserve1"] = sync.reserve1

        objects_pair = []
        for index, entries in sorted(self._local_cache_pair.items()):
            objects_pair.extend(sorted(entries.values(), key=lambda x: x["pair_address"].lower()))

        objects_token = []
        for index, entries in sorted(self._local_cache_token.items()):
            objects_token.extend(sorted(entries.values(), key=lambda x: x["token_address"].lower()))

        return [(orm.PairHourData, objects_pair), (orm.TokenHourData, objects_token)]

    @classmethod
    def post_process(cls, db: FusionSQL, cache: Cache, first_block: int, end_block: int, state: orm.State) -> None:
        """
        Finalize the stage

        For performance reasons this function is doing several things simultaneously:
        - aggregate total supply for each hour_data entry
        - aggregate and create day data entries
        - commit in regular intervals (in case of large data sets)
        - update the state.finalized field
        - makes use of caching to store the latest total supply data between invocations

        Note: Only computes entries for hours and days that have fully concluded

        TODO aggregate daily token data

        :param db: database service
        :param cache: cache service
        :param first_block: the earliest block this stage will ever process
        :param end_block: last block (included in the computation)
        :param state: database state associated with this stage
        :return:
        """
        size_hour = 3600
        size_day = 24 * size_hour

        start_timestamp_hour, end_timestamp_hour = cls._find_timestamp_window(db, first_block, end_block, size_hour)
        if start_timestamp_hour is None:
            return

        # adjust timestamp depending on state
        if state.finalized is not None:
            start_timestamp_hour = max(start_timestamp_hour, state.finalized + 1)

        if start_timestamp_hour >= end_timestamp_hour:
            return

        # derive day timestamp
        start_timestamp_day = start_timestamp_hour // size_day * size_day
        end_timestamp_day = (end_timestamp_hour + 1) // size_day * size_day - 1

        start_timestamp = min(start_timestamp_hour, start_timestamp_day)
        end_timestamp = max(end_timestamp_hour, end_timestamp_day)

        assert start_timestamp < end_timestamp
        assert start_timestamp_hour >= start_timestamp_day
        assert end_timestamp_hour >= end_timestamp_day

        cache_objects = {}
        cache_supply = cache.get(f"_stage_stats_cache_{start_timestamp_hour - 1}", {})

        with db.session() as session:
            results = session.execute(
                select(orm.PairHourData)
                    .filter(orm.PairHourData.hourStartUnix.between(start_timestamp, end_timestamp))
                    .order_by(orm.PairHourData.hourIndex.asc())
            ).yield_per(1000).scalars()

            day_index_previous = 0
            for i, hour_data in enumerate(results):
                day_index = hour_data.hourStartUnix // size_day

                # regularly commit changes
                if day_index > day_index_previous and i > 0:
                    objects = []
                    for index, entries in sorted(cache_objects.items()):
                        objects.extend(sorted(entries.values(), key=lambda x: x["pair_address"].lower()))

                    if len(objects) > 0:
                        session.bulk_insert_mappings(orm.PairDayData, objects)

                    # reset cache
                    cache_objects = {}

                    state.finalized = hour_data.hourStartUnix - 1
                    session.merge(state, load=True)
                    session.commit()

                day_index_previous = day_index

                # hourly updates (aggregate total supply)
                if hour_data.hourStartUnix >= start_timestamp_hour:
                    assert hour_data.totalSupply is None

                    if hour_data.pair_address in cache_supply:
                        total_supply = cache_supply[hour_data.pair_address]
                    else:
                        # attempt to find the latest hour data entry
                        hour_data_previous = session.execute(
                            select(orm.PairHourData)
                                .filter(orm.PairHourData.pair_address == hour_data.pair_address)
                                .filter(orm.PairHourData.hourStartUnix < start_timestamp_hour)
                                .order_by(orm.PairHourData.hourStartUnix.desc())
                                .limit(1)
                        ).scalar()

                        if hour_data_previous is None:
                            total_supply = Decimal(0)
                        else:
                            assert hour_data_previous.totalSupply is not None
                            total_supply = hour_data_previous.totalSupply

                    total_supply += hour_data.totalSupplyChange
                    hour_data.totalSupply = total_supply
                    cache_supply[hour_data.pair_address] = total_supply

                # daily updates (aggregate daily entries)
                if hour_data.hourStartUnix <= end_timestamp_day:
                    assert hour_data.totalSupply is not None

                    entries = cache_objects.setdefault(day_index, {})
                    if hour_data.pair_address in entries:
                        day_data = entries[hour_data.pair_address]
                    else:
                        day_data = entries.setdefault(hour_data.pair_address, {
                            "dayIndex": day_index,
                            "dayStartUnix": day_index * size_day,
                            "pair_address": hour_data.pair_address,
                            "reserve0": None,
                            "reserve1": None,
                            "reserveUSD": None,
                            "totalSupply": None,
                            "dailyVolumeToken0": Decimal(0),
                            "dailyVolumeToken1": Decimal(0),
                            "dailyVolumeUSD": Decimal(0),
                            "dailyTxns": 0,
                        })

                    day_data["reserve0"] = hour_data.reserve0
                    day_data["reserve1"] = hour_data.reserve1
                    day_data["reserveUSD"] = hour_data.reserveUSD
                    day_data["totalSupply"] = hour_data.totalSupply
                    day_data["dailyVolumeToken0"] += hour_data.hourlyVolumeToken0
                    day_data["dailyVolumeToken1"] += hour_data.hourlyVolumeToken1
                    day_data["dailyVolumeUSD"] += hour_data.hourlyVolumeUSD
                    day_data["dailyTxns"] += hour_data.hourlyTxns

            # commit final changes
            objects = []
            for index, entries in sorted(cache_objects.items()):
                objects.extend(sorted(entries.values(), key=lambda x: x["pair_address"].lower()))

            if len(objects) > 0:
                session.bulk_insert_mappings(orm.PairDayData, objects)

            state.finalized = end_timestamp
            session.merge(state, load=True)
            session.commit()

        # store latest supply cache
        cache.set(f"_stage_stats_cache_{end_timestamp}", cache_supply, ttl=None)


class EventProcessorStageExchangePangolin_Stats(EventProcessorStageExchange_Stats):
    pass


class EventProcessorStageExchangePegasys_Stats(EventProcessorStageExchange_Stats):
    pass
