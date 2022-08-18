#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import enum

from sqlalchemy import (
    Column,
    Enum,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
)

from .base import (
    Base,
    BaseModel,
)


# TODO possibly use chain ids
@enum.unique
class Chains(enum.Enum):
    UNKNOWN = 0
    ETH = enum.auto()
    AVAX = enum.auto()
    SYS = enum.auto()


class BaseModelXQuery(BaseModel):
    """
    Base xquery model

    Contains data fields found in any Query
    """
    xhash = Column(String(length=66), nullable=False, unique=True)
    chain = Column(Enum(Chains, name="enum_chains"), default=Chains.UNKNOWN, nullable=False)
    block_height = Column(Integer, nullable=False)
    block_hash = Column(String(length=66), nullable=False)
    block_timestamp = Column(Integer, nullable=False)
    tx_hash = Column(String(length=66), nullable=False)
    event_name = Column(String(length=128), nullable=False)

    Index(chain, block_height, xhash)


class XQuery(BaseModelXQuery, Base):
    """
    Store event log information

    Relationships
    """
    __tablename__ = "xquery"

    func_identifier = Column(String(length=128))

    address_sender = Column(String(length=66))
    address_to = Column(String(length=66))

    token0_name = Column(String(length=64))
    token0_symbol = Column(String(length=16))
    token0_decimals = Column(SmallInteger)
    token1_name = Column(String(length=64))
    token1_symbol = Column(String(length=16))
    token1_decimals = Column(SmallInteger)

    # Approval, Transfer, Deposit, Withdrawal (RC20, WAVAX)
    value = Column(Numeric(precision=78, scale=0))

    # Mint, Burn (Pair)
    amount0 = Column(Numeric(precision=78, scale=0))
    amount1 = Column(Numeric(precision=78, scale=0))

    # Swap (Pair)
    amount0_in = Column(Numeric(precision=78, scale=0))
    amount1_in = Column(Numeric(precision=78, scale=0))
    amount0_out = Column(Numeric(precision=78, scale=0))
    amount1_out = Column(Numeric(precision=78, scale=0))

    # Sync (Pair)
    reserve0 = Column(Numeric(precision=34, scale=0))
    reserve1 = Column(Numeric(precision=34, scale=0))
