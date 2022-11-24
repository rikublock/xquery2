#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    List,
    Tuple,
    Type,
    Union,
)

import logging
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.sql import func

import xquery.cache
import xquery.db
import xquery.db.orm as orm
import xquery.event.indexer
from xquery.contract import (
    png_factory,
    psys_factory,
)
from .processor import EventProcessorStage

log = logging.getLogger(__name__)


class EventProcessorStageExchange_Count(EventProcessorStage):

    def __init__(self, db: xquery.db.FusionSQL, cache: xquery.cache.Cache, factory_address: str) -> None:
        """
        Exchange processor stage to aggregate transaction and pair counts.

        Computes the following data:
          - factory.pairCount
          - factory.txCount
          - pair.txCount
          - pair.totalSupply
          - pair.volumeToken0
          - pair.volumeToken1
          - token.txCount
          - token.tradeVolume

        Depends on:
          - None

        Note: Should be run in a single job

        :param db: database service
        :param cache: cache service
        """
        super().__init__(db, cache)

        self.factory_address = factory_address

    def _aggregate_factory(self, session, start_block: int, end_block: int) -> List[orm.Base]:
        factory = session.execute(
            select(orm.Factory)
                .filter(orm.Factory.address == self.factory_address)
                .limit(1)
        ).scalar()
        assert factory is not None

        # update the pair count
        factory.pairCount += (
            session.query(func.count(orm.Pair.id))
                .join(orm.Block)
                .filter(orm.Block.number.between(start_block, end_block))
        ).scalar()

        # update the tx count (actually more of an event counter)
        # sum (mint, burn, swap) between (start_block, end_block)
        factory.txCount += (
            session.query(func.count(orm.Mint.id))
                .join(orm.Transaction)
                .join(orm.Block)
                .filter(orm.Block.number.between(start_block, end_block))
        ).scalar()

        factory.txCount += (
            session.query(func.count(orm.Burn.id))
                .join(orm.Transaction)
                .join(orm.Block)
                .filter(orm.Block.number.between(start_block, end_block))
        ).scalar()

        factory.txCount += (
            session.query(func.count(orm.Swap.id))
                .join(orm.Transaction)
                .join(orm.Block)
                .filter(orm.Block.number.between(start_block, end_block))
        ).scalar()

        return [factory]

    def _aggregate_pair(self, session, start_block: int, end_block: int) -> List[orm.Base]:
        pairs = session.execute(
            select(orm.Pair)
                .order_by(orm.Pair.id)
        ).yield_per(1000).scalars()

        # update the tx count (actually more of an event counter)
        # sum (mint, burn, swap) between (start_block, end_block)
        objects = []
        for pair in pairs:
            mint_count, mint_value, mint_fee_value = (
                session.query(func.count(orm.Mint.id), func.sum(orm.Mint.liquidity), func.sum(orm.Mint.feeLiquidity))
                    .filter(orm.Mint.pair_address == pair.address)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number.between(start_block, end_block))
            ).one()
            pair.txCount += mint_count
            pair.totalSupply += Decimal(0) if mint_value is None else mint_value
            pair.totalSupply += Decimal(0) if mint_fee_value is None else mint_fee_value

            burn_count, burn_value, burn_fee_value = (
                session.query(func.count(orm.Burn.id), func.sum(orm.Burn.liquidity), func.sum(orm.Burn.feeLiquidity))
                    .filter(orm.Burn.pair_address == pair.address)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number.between(start_block, end_block))
            ).one()
            pair.txCount += burn_count
            pair.totalSupply -= Decimal(0) if burn_value is None else burn_value
            pair.totalSupply += Decimal(0) if burn_fee_value is None else burn_fee_value
            assert pair.totalSupply >= Decimal(0)

            swap_count, swap_value0, swap_value1 = (
                session.query(func.count(orm.Swap.id), func.sum(orm.Swap.amount0Out + orm.Swap.amount0In), func.sum(orm.Swap.amount1Out + orm.Swap.amount1In))
                    .filter(orm.Swap.pair_address == pair.address)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number.between(start_block, end_block))
            ).one()
            pair.txCount += swap_count
            pair.volumeToken0 += Decimal(0) if swap_value0 is None else swap_value0
            pair.volumeToken1 += Decimal(0) if swap_value1 is None else swap_value1

            objects.append(pair)

        return objects

    def _aggregate_token(self, session, start_block: int, end_block: int) -> List[orm.Base]:
        tokens = session.execute(
            select(orm.Token)
                .order_by(orm.Token.id)
        ).yield_per(1000).scalars()

        # update the tx count (actually more of an event counter)
        # sum (mint, burn, swap) between (start_block, end_block)
        objects = []
        for token in tokens:
            token.txCount += (
                session.query(func.count(orm.Mint.id))
                    .join(orm.Pair)
                    .filter((orm.Pair.token0_address == token.address) | (orm.Pair.token1_address == token.address))
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number.between(start_block, end_block))
            ).scalar()

            token.txCount += (
                session.query(func.count(orm.Burn.id))
                    .join(orm.Pair)
                    .filter((orm.Pair.token0_address == token.address) | (orm.Pair.token1_address == token.address))
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number.between(start_block, end_block))
            ).scalar()

            swap_count, swap_value = (
                session.query(func.count(orm.Swap.id), func.sum(orm.Swap.amount0Out + orm.Swap.amount0In))
                    .join(orm.Pair)
                    .filter(orm.Pair.token0_address == token.address)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number.between(start_block, end_block))
            ).one()
            token.txCount += swap_count
            token.tradeVolume += Decimal(0) if swap_value is None else swap_value

            swap_count, swap_value = (
                session.query(func.count(orm.Swap.id), func.sum(orm.Swap.amount1Out + orm.Swap.amount1In))
                    .join(orm.Pair)
                    .filter(orm.Pair.token1_address == token.address)
                    .join(orm.Transaction)
                    .join(orm.Block)
                    .filter(orm.Block.number.between(start_block, end_block))
            ).one()
            token.txCount += swap_count
            token.tradeVolume += Decimal(0) if swap_value is None else swap_value

            objects.append(token)

        return objects

    @classmethod
    def setup(cls, db: xquery.db.FusionSQL, first_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        return []

    def process(self, start_block: int, end_block: int) -> Union[List[orm.Base], List[Tuple[Type[orm.Base], List[dict]]]]:
        objects = []
        with self._db.session() as session:
            objects.extend(self._aggregate_factory(session, start_block, end_block))
            objects.extend(self._aggregate_pair(session, start_block, end_block))
            objects.extend(self._aggregate_token(session, start_block, end_block))

        return objects


class EventProcessorStageExchangePangolin_Count(EventProcessorStageExchange_Count):

    def __init__(self, db: xquery.db.FusionSQL, cache: xquery.cache.Cache):
        """
        Event processor stage "count" for the Pangolin Exchange (on AVAX)
        """
        super().__init__(
            db=db,
            cache=cache,
            factory_address=png_factory.address,
        )


class EventProcessorStageExchangePegasys_Count(EventProcessorStageExchange_Count):

    def __init__(self, db: xquery.db.FusionSQL, cache: xquery.cache.Cache):
        """
        Event processor stage "count" for the Pegasys Exchange (on SYS)
        """
        super().__init__(
            db=db,
            cache=cache,
            factory_address=psys_factory.address,
        )
