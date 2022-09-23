#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import enum

from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    Integer,
    SmallInteger,
    String,
    Numeric,
    Table,
    ForeignKey,
    UniqueConstraint,
    CheckConstraint,
)
from sqlalchemy.orm import (
    relationship,
    backref,
)

from .base import (
    Base,
    BaseModel,
)

# TODO craete a blocks model, then just add relationship
# block should contain: chain, hash, height, timestamp


# Deviations
# UniswapFactory -> Factory
# Factory.id -> Factory.address
# Factory.totalVolumeETH -> Factory.totalVolumeNative
# Factory.totalLiquidityETH -> Factory.totalLiquidityNative
# Token.id -> Token.address
# Token.derivedETH -> Token.derivedNative
# Pair.id -> Pair.address
# TODO



class Factory(BaseModel, Base):
    """
    Store factory contract information

    Relationships
    """
    __tablename__ = "factory"

    # contract address used as unique identifier
    address = Column(String(length=42), nullable=False, unique=True)

    pairCount = Column(Integer, nullable=False)

    # total volume
    totalVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    totalVolumeNative = Column(Numeric(precision=78, scale=0), nullable=False)

    # untracked values - less confident USD scores
    untrackedVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)

    # total liquidity
    totalLiquidityUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    totalLiquidityNative = Column(Numeric(precision=78, scale=0), nullable=False)

    # transactions
    txCount = Column(Integer, nullable=False)


class Token(BaseModel, Base):
    """
    Store RC20 token contract information

    Relationships
    """
    __tablename__ = "token"

    # contract address used as unique identifier
    address = Column(String(length=42), nullable=False, unique=True)

    # mirrored from the smart contract
    symbol = Column(String(length=16), nullable=False)
    name = Column(String(length=64), nullable=False)
    decimals = Column(SmallInteger, nullable=False)

    # used for other stats like marketcap
    totalSupply = Column(Numeric(precision=78, scale=0), nullable=False)

    # token specific volume
    tradeVolume = Column(Numeric(precision=78, scale=0), nullable=False)
    tradeVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    untrackedVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)

    # transactions across all pairs
    txCount = Column(Integer, nullable=False)

    # liquidity across all pairs
    totalLiquidity = Column(Numeric(precision=78, scale=0), nullable=False)

    # derived prices
    derivedNative = Column(Numeric(precision=78, scale=0))

    # Relationships to other tables
    # derived fields
    tokenDayData = relationship("TokenDayData", back_populates="token")
    pairDayDataBase = relationship("PairDayData", back_populates="token0", foreign_keys="PairDayData.token0_id")
    pairDayDataQuote = relationship("PairDayData", back_populates="token1", foreign_keys="PairDayData.token1_id")
    pairBase = relationship("Pair", back_populates="token0", foreign_keys="Pair.token0_address")
    pairQuote = relationship("Pair", back_populates="token1", foreign_keys="Pair.token1_address")


class Pair(BaseModel, Base):
    """
    Store Pair contract information

    Relationships
        - ManyToOne with Tokens
    """
    __tablename__ = "pair"

    # contract address used as unique identifier
    address = Column(String(length=42), nullable=False, unique=True)

    # mirrored from the smart contract
    token0_address = Column(String(length=42), ForeignKey("token.address"))
    token0 = relationship("Token", back_populates="pairBase", foreign_keys=[token0_address])

    token1_address = Column(String(length=42), ForeignKey("token.address"))
    token1 = relationship("Token", back_populates="pairQuote", foreign_keys=[token1_address])

    # Constraints
    UniqueConstraint(token0_address, token1_address)

    reserve0 = Column(Numeric(precision=78, scale=0), nullable=False)
    reserve1 = Column(Numeric(precision=78, scale=0), nullable=False)
    totalSupply = Column(Numeric(precision=78, scale=0), nullable=False)

    # derived liquidity
    reserveNative = Column(Numeric(precision=78, scale=0), nullable=False)
    reserveUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    # used for separating per pair reserves and global
    trackedReserveNative = Column(Numeric(precision=78, scale=0), nullable=False)

    # Price in terms of the asset pair
    token0Price = Column(Numeric(precision=78, scale=0), nullable=False)
    token1Price = Column(Numeric(precision=78, scale=0), nullable=False)

    # lifetime volume stats
    volumeToken0 = Column(Numeric(precision=78, scale=0), nullable=False)
    volumeToken1 = Column(Numeric(precision=78, scale=0), nullable=False)
    volumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    untrackedVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    txCount = Column(Integer, nullable=False)

    # legacy
    # creation stats
    createdAtTimestamp = Column(Integer, nullable=False)
    createdAtBlockNumber = Column(Integer, nullable=False)

    block_id = Column(Integer, ForeignKey("block.id"))
    block = relationship("Block", foreign_keys=[block_id])

    # Fields used to help derived relationship
    # used to detect new exchanges
    liquidityProviderCount = Column(Integer, nullable=False)

    # derived fields
    pairHourData = relationship("PairHourData", back_populates="pair")
    liquidityPositions = relationship("LiquidityPosition", back_populates="pair")
    liquidityPositionSnapshots = relationship("LiquidityPositionSnapshot", back_populates="pair")
    mints = relationship("Mint", back_populates="pair")
    burns = relationship("Burn", back_populates="pair")
    swaps = relationship("Swap", back_populates="pair")


class User(BaseModel, Base):
    """
    Store exchange user information

    Relationships
    """
    __tablename__ = "user"

    # wallet address used as unique identifier
    address = Column(String(length=42), nullable=False, unique=True)

    liquidityPositions = relationship("LiquidityPosition", back_populates="user")
    liquidityPositionSnapshots = relationship("LiquidityPositionSnapshot", back_populates="user")

    usdSwapped = Column(Numeric(precision=78, scale=0), nullable=False)


class LiquidityPosition(BaseModel, Base):
    """
    Store liquidity position information

    Relationships
    """
    __tablename__ = "liquidity_position"

    user_id = Column(Integer, ForeignKey("user.id"))
    user = relationship("User", back_populates="liquidityPositions")

    pair_address = Column(String(length=42), ForeignKey("pair.address"))
    pair = relationship("Pair", back_populates="liquidityPositions", foreign_keys=[pair_address])

    liquidityTokenBalance = Column(Numeric(precision=78, scale=0), nullable=False)

    UniqueConstraint(user_id, pair_address)


class LiquidityPositionSnapshot(BaseModel, Base):
    """
    Store liquidity position snapshot information

    Saved over time for return calculations, gets created and never updated

    Relationships
    """
    __tablename__ = "liquidity_position_snapshot"

    # TODO saved for fast historical lookups (block contains timestamp)
    block_id = Column(Integer, ForeignKey("block.id"))
    block = relationship("Block", foreign_keys=[block_id])

    # saved for fast historical lookups
    timestamp = Column(Integer, nullable=False)
    blockHeight = Column(Integer, nullable=False)

    # TODO does this even make sense ?
    liquidityPosition_id = Column(Integer, ForeignKey("liquidity_position.id"))
    liquidityPosition = relationship("LiquidityPosition")

    user_id = Column(Integer, ForeignKey("user.id"))
    user = relationship("User", back_populates="liquidityPositionSnapshots", foreign_keys=[user_id])

    pair_address = Column(String(length=42), ForeignKey("pair.address"))
    pair = relationship("Pair", back_populates="liquidityPositionSnapshots", foreign_keys=[pair_address])

    # snapshot
    token0PriceUSD = Column(Numeric(precision=78, scale=0), nullable=False) # snapshot of token0 price
    token1PriceUSD = Column(Numeric(precision=78, scale=0), nullable=False) # snapshot of token1 price
    reserve0 = Column(Numeric(precision=78, scale=0), nullable=False) # snapshot of pair token0 reserves
    reserve1 = Column(Numeric(precision=78, scale=0), nullable=False) # snapshot of pair token1 reserves
    reserveUSD = Column(Numeric(precision=78, scale=0), nullable=False) # snapshot of pair reserves in USD
    liquidityTokenTotalSupply = Column(Numeric(precision=78, scale=0), nullable=False) # snapshot of pool token supply
    liquidityTokenBalance = Column(Numeric(precision=78, scale=0), nullable=False) # snapshot of users pool token balance


class Block(BaseModel, Base):
    """
    Store block information

    Relationships
     - OneToMany with Transaction
    """
    __tablename__ = "block"

    hash = Column(String(length=66), nullable=False, unique=True)
    number = Column(Integer, nullable=False)
    timestamp = Column(Integer, nullable=False)

    # Relationships to other tables
    transactions = relationship("Transaction", back_populates="block")


class Transaction(BaseModel, Base):
    """
    Store transaction information

    Relationships
     - ManyToOne with Block
    """
    __tablename__ = "transaction"

    hash = Column(String(length=66), nullable=False, unique=True)
    from_ = Column(String(length=42), nullable=False)

    # Relationships to other tables
    block_id = Column(Integer, ForeignKey("block.id"))
    block = relationship("Block", back_populates="transactions", foreign_keys=[block_id])

    # legacy
    timestamp = Column(Integer, nullable=False)


class Transfer(BaseModel, Base):
    """
    Store Transfer event information

    Only used to store information for later post-processing.

    Relationships
        - OneToMany with Pair
    """
    __tablename__ = "transfer"

    transaction_id = Column(Integer, ForeignKey("transaction.id"))
    transaction = relationship("Transaction", foreign_keys=[transaction_id])

    pair_address = Column(String(length=42), ForeignKey("pair.address"))
    pair = relationship("Pair", foreign_keys=[pair_address])

    from_ = Column(String(length=42), nullable=False)
    to = Column(String(length=42), nullable=False)
    value = Column(Numeric(precision=78, scale=18), nullable=False)
    logIndex = Column(Integer, nullable=False)


class Mint(BaseModel, Base):
    """
    Store mint event information

    Relationships
    """
    __tablename__ = "mint"

    # transaction hash + "-" + index in mints Transaction array
    # identifier = Column(String(length=128), nullable=False, unique=True)

    # TODO
    transaction_id = Column(Integer, ForeignKey("transaction.id"))
    transaction = relationship("Transaction", foreign_keys=[transaction_id])

    pair_address = Column(String(length=42), ForeignKey("pair.address"))
    pair = relationship("Pair", back_populates="mints", foreign_keys=[pair_address])

    # populated from the primary Transfer event
    liquidity = Column(Numeric(precision=78, scale=18), nullable=False)

    # populated from the Mint event
    sender = Column(String(length=42))
    amount0 = Column(Numeric(precision=78, scale=18))
    amount1 = Column(Numeric(precision=78, scale=18))
    to = Column(String(length=42), nullable=False)
    logIndex = Column(Integer)
    # derived amount based on available prices of tokens
    amountUSD = Column(Numeric(precision=78, scale=18))

    # optional fee fields, if a Transfer event is fired in _mintFee
    feeTo = Column(String(length=42))
    feeLiquidity = Column(Numeric(precision=78, scale=18))


class Burn(BaseModel, Base):
    """Store burn event information

    Relationships
    """
    __tablename__ = "burn"

    # transaction hash + "-" + index in burns Transaction array
    # identifier = Column(String(length=128), nullable=False, unique=True)

    transaction_id = Column(Integer, ForeignKey("transaction.id"))
    transaction = relationship("Transaction", foreign_keys=[transaction_id])

    pair_address = Column(String(length=42), ForeignKey("pair.address"))
    pair = relationship("Pair", back_populates="burns", foreign_keys=[pair_address])

    # populated from the primary Transfer event
    liquidity = Column(Numeric(precision=78, scale=18), nullable=False)

    # populated from the Mint event
    sender = Column(String(length=42))
    amount0 = Column(Numeric(precision=78, scale=18))
    amount1 = Column(Numeric(precision=78, scale=18))
    to = Column(String(length=42), nullable=False)
    logIndex = Column(Integer)
    # derived amount based on available prices of tokens
    amountUSD = Column(Numeric(precision=78, scale=18))

    # mark uncomplete in ETH case
    needsComplete = Column(Boolean, nullable=False)

    # optional fee fields, if a Transfer event is fired in _mintFee
    feeTo = Column(String(length=42))
    feeLiquidity = Column(Numeric(precision=78, scale=18))


class Swap(BaseModel, Base):
    """
    Store swap event information

    Relationships
    """
    __tablename__ = "swap"

    # transaction hash + "-" + index in swaps Transaction array
    # identifier = Column(String(length=128), nullable=False, unique=True)

    transaction_id = Column(Integer, ForeignKey("transaction.id"))
    transaction = relationship("Transaction", foreign_keys=[transaction_id])

    pair_address = Column(String(length=42), ForeignKey("pair.address"))
    pair = relationship("Pair", back_populates="swaps", foreign_keys=[pair_address])

    # populated from the Swap event
    sender = Column(String(length=42), nullable=False)
    from_ = Column(String(length=42), nullable=False)  # the EOA that initiated the txn
    amount0In = Column(Numeric(precision=78, scale=18), nullable=False)
    amount1In = Column(Numeric(precision=78, scale=18), nullable=False)
    amount0Out = Column(Numeric(precision=78, scale=18), nullable=False)
    amount1Out = Column(Numeric(precision=78, scale=18), nullable=False)
    to = Column(String(length=42), nullable=False)
    logIndex = Column(Integer)

    # derived info
    amountUSD = Column(Numeric(precision=78, scale=18), nullable=False)


class Sync(BaseModel, Base):
    """
    Store Sync event information

    Only used to store information for later post-processing.

    Relationships
        - OneToMany with Pair
    """
    __tablename__ = "sync"

    transaction_id = Column(Integer, ForeignKey("transaction.id"))
    transaction = relationship("Transaction", foreign_keys=[transaction_id])

    pair_address = Column(String(length=42), ForeignKey("pair.address"))
    pair = relationship("Pair", foreign_keys=[pair_address])

    reserve0 = Column(Numeric(precision=78, scale=18), nullable=False)
    reserve1 = Column(Numeric(precision=78, scale=18), nullable=False)
    logIndex = Column(Integer, nullable=False)


class Bundle(BaseModel, Base):
    """
    Store USD calculations information

    Relationships
    """
    __tablename__ = "bundle"

    # using BaseModel.id as identifier

    # price of Native to USD
    nativePrice = Column(Numeric(precision=78, scale=18), nullable=False)

    block_id = Column(Integer, ForeignKey("block.id"))
    block = relationship("Block", foreign_keys=[block_id])

    logIndex = Column(Integer)


class ExchangeDayData(BaseModel, Base):
    """
    Data accumulated and condensed into daily stats for the entire exchange.

    Relationships
    """
    __tablename__ = "exchange_day_data"

    # timestamp rounded to current day by dividing by 86400
    identifier = Column(Integer, nullable=False, unique=True)
    date = Column(Integer, nullable=False)

    dailyVolumeNative = Column(Numeric(precision=78, scale=0), nullable=False)
    dailyVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    dailyVolumeUntracked = Column(Numeric(precision=78, scale=0), nullable=False)

    totalVolumeNative = Column(Numeric(precision=78, scale=0), nullable=False)
    totalLiquidityNative = Column(Numeric(precision=78, scale=0), nullable=False)
    # Accumulate at each trade, not just calculated off whatever totalVolume is. making it more accurate as it is a live conversion
    totalVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    totalLiquidityUSD = Column(Numeric(precision=78, scale=0), nullable=False)

    txCount = Column(Integer, nullable=False)


class PairHourData(BaseModel, Base):
    """
    Data accumulated and condensed into hourly stats for each pair.

    Relationships
    """
    __tablename__ = "pair_hour_data"

    # using BaseModel.id as identifier

    # unix timestamp for start of hour
    hourStartUnix = Column(Integer, nullable=False)

    pair_address = Column(String(length=42), ForeignKey("pair.address"))
    pair = relationship("Pair", back_populates="pairHourData", foreign_keys=[pair_address])

    # reserves
    reserve0 = Column(Numeric(precision=78, scale=0), nullable=False)
    reserve1 = Column(Numeric(precision=78, scale=0), nullable=False)

    # total supply for LP historical returns
    totalSupply = Column(Numeric(precision=78, scale=0), nullable=False)

    # derived liquidity
    reserveUSD = Column(Numeric(precision=78, scale=0), nullable=False)

    # volume stats
    hourlyVolumeToken0 = Column(Numeric(precision=78, scale=0), nullable=False)
    hourlyVolumeToken1 = Column(Numeric(precision=78, scale=0), nullable=False)
    hourlyVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    hourlyTxns = Column(Integer, nullable=False)


class PairDayData(BaseModel, Base):
    """
    Data accumulated and condensed into daily stats for each pair.

    Relationships
    """
    __tablename__ = "pair_day_data"

    # using BaseModel.id as identifier

    date = Column(Integer, nullable=False)
    pairAddress = Column(String(length=42), nullable=False)  # TODO use relationship ?

    token0_id = Column(Integer, ForeignKey("token.id"))
    token0 = relationship("Token", back_populates="pairDayDataBase", foreign_keys=[token0_id])

    token1_id = Column(Integer, ForeignKey("token.id"))
    token1 = relationship("Token", back_populates="pairDayDataQuote", foreign_keys=[token1_id])

    # reserves
    reserve0 = Column(Numeric(precision=78, scale=0), nullable=False)
    reserve1 = Column(Numeric(precision=78, scale=0), nullable=False)

    # total supply for LP historical returns
    totalSupply = Column(Numeric(precision=78, scale=0), nullable=False)

    # derived liquidity
    reserveUSD = Column(Numeric(precision=78, scale=0), nullable=False)

    # volume stats
    dailyVolumeToken0 = Column(Numeric(precision=78, scale=0), nullable=False)
    dailyVolumeToken1 = Column(Numeric(precision=78, scale=0), nullable=False)
    dailyVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    dailyTxns = Column(Integer, nullable=False)


class TokenDayData(BaseModel, Base):
    """
    Data accumulated and condensed into daily stats for each token.

    Relationships
    """
    __tablename__ = "token_day_data"

    # using BaseModel.id as identifier

    date = Column(Integer, nullable=False)

    token_id = Column(Integer, ForeignKey("token.id"))
    token = relationship("Token", back_populates="tokenDayData", foreign_keys=[token_id])

    # volume stats
    dailyVolumeToken = Column(Numeric(precision=78, scale=0), nullable=False)
    dailyVolumeNative = Column(Numeric(precision=78, scale=0), nullable=False)
    dailyVolumeUSD = Column(Numeric(precision=78, scale=0), nullable=False)
    dailyTxns = Column(Integer, nullable=False)

    # liquidity stats
    totalLiquidityToken = Column(Numeric(precision=78, scale=0), nullable=False)
    totalLiquidityNative = Column(Numeric(precision=78, scale=0), nullable=False)
    totalLiquidityUSD = Column(Numeric(precision=78, scale=0), nullable=False)

    # price stats
    priceUSD = Column(Numeric(precision=78, scale=0), nullable=False)
