#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    Any,
    List,
    Tuple,
)

import logging
import time

from decimal import Decimal

from sqlalchemy import select
import sqlalchemy.exc

from eth_utils import add_0x_prefix

from web3 import Web3
from web3.exceptions import (
    BadFunctionCallOutput,
    BlockNotFound,
    ContractLogicError,
    TransactionNotFound,
)
from web3.types import (
    ABI,
    HexStr,
)

from eth_typing import (
    AnyAddress,
    ChecksumAddress,
)

import xquery.cache
import xquery.db
import xquery.db.orm as orm
from xquery.contract import (
    png_factory,
    png_rc20,
    png_router,
    psys_factory,
    psys_rc20,
    psys_router,
    rc20_bytes,
)
from xquery.types import ExtendedLogReceipt
from xquery.util import (
    MAX_DECIMAL_PLACES,
    token_to_decimal,
)
from .indexer import EventIndexer

log = logging.getLogger(__name__)

# The event indexer needs to be considerate about what data is being committed to ensure database state consistency.
# Generally, any additional data (not events) is consider safe and can already be committed in the indexer as it
# doesn't directly affect the indexed state. At worst we end up with an unused object in the database. Events in
# particular should never be committed here, but instead be returned in the job result where they can later
# be verified and processed by the database handler.
#
# Example:
# A transaction for instance can be added at any time. Worst case, we stored information about a dangling tx
# that is no longer part of the main chain. A similar argument can be made for blocks, tokens, etc. The only
# assumption we have to make is that the fields (e.g. hash, height, name, symbol, decimals, etc.) of said objects
# are immutable (cannot be changed in a later block) and therefore block height independent.
#
# Note: The event indexer partially works with transient (no database identity) or cached orm objects that might
# not necessarily be up to date with the database state. In such a case, only use the basic fields of that object.
# For more details see:
#  - https://docs.sqlalchemy.org/en/14/orm/session_state_management.html
#  - https://docs.sqlalchemy.org/en/14/orm/session_state_management.html#expunging
#  - https://docs.sqlalchemy.org/en/14/orm/session_state_management.html#merging


def is_complete(mint: orm.Mint) -> bool:
    return mint.sender is not None


class EventIndexerExchange(EventIndexer):

    def __init__(
        self,
        w3: Web3,
        db: xquery.db.FusionSQL,
        cache: xquery.cache.Cache,
        abi_rc20: ABI,
        factory_address: ChecksumAddress,
        router_address: ChecksumAddress,
    ) -> None:
        """
        Basic exchange event indexer

        :param w3: web3 provider
        :param db: database service
        :param cache: cache service
        :param abi_rc20: ABI of an RC20 token contract
        :param factory_address: factory contract checksum address of the exchange
        :param router_address: router contract checksum address of the exchange
        """
        super().__init__(w3, db, cache)

        self._abi_rc20 = abi_rc20
        self._factory_address = factory_address
        self._router_address = router_address

        # transport incomplete orm objects between event processing invocations
        self._local_cache = xquery.cache.Cache_Memory()

        self._mints = {}
        self._burns = {}

    def reset(self) -> None:
        """
        Clear the local in-memory cache between jobs.
        """
        super().reset()
        self._local_cache.flush()

        # sanity checks
        for tx_hash, mints in self._mints.items():
            for mint in mints:
                if not is_complete(mint):
                    log.warning(f"Encountered incomplete mint event in tx '{tx_hash}'")

        for tx_hash, burns in self._burns.items():
            for burn in burns:
                if burn.needsComplete:
                    log.warning(f"Encountered incomplete burn event in tx '{tx_hash}'")

        self._mints = {}
        self._burns = {}

    @staticmethod
    def _sanitize_db_result(obj: Any) -> Any:
        """
        There is currently a bug in sqlalchemy that causes ``one_or_none()`` to return either
        the object itself or a row of objects (with 1 entry) depending on whether the query was cached.

        :param obj: object returned by a sqlalchemy query
        :return:
        """
        if isinstance(obj, sqlalchemy.engine.row.Row):
            return obj[0]
        else:
            return obj

    def _get_block(self, hash_: HexStr) -> orm.Block:
        """
        Get a block object

        Example block info:
          AttributeDict({
            'blockExtraData': '0x',
            'difficulty': 1,
            'extDataHash': '0x0000000000000000000000000000000000000000000000000000000000000000',
            'gasLimit': 8000000,
            'gasUsed': 3449251,
            'hash': '0x8ed42786cb8fa0aa8ef0121cfc50b7e23277d513b5f4486078141a9f540d982b',
            'logsBloom': '0x0000000000000008000c0000000000....00000000001000000004000000000004',
            'miner': '0x0100000000000000000000000000000000000000',
            'mixHash': '0x0000000000000000000000000000000000000000000000000000000000000000',
            'nonce': '0x0000000000000000',
            'number': 57347,
            'parentHash': '0xee825229c585aa70c5707aab099dbf1c97877a8fde7c7cc77c311c4b545bd5d4',
            'proofOfAuthorityData': '0xd883010916846765746888676f312e31352e35856c696e757862d5d619e2745169d099dd768d18644bf8c46afab90417d5bd31f274d38f3500',
            'receiptsRoot': '0x117c7107a50ee1372d6d549ace4321d75b4ff72529868899633d1e823a5c7601',
            'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
            'size': 977,
            'stateRoot': '0x8cc342ded682634449d0e9519e85aacf0adea6318101be9be875da1484c77068',
            'timestamp': 1612814799,
            'totalDifficulty': 57347,
            'transactions': ['0xd5fae76d06da05c5a7629cd40b8005fe00c83b19b573f218503f93bf866350e4'],
            'transactionsRoot': '0x34619d385fe698c16fd80936b2d27edc1498de81f9a2f739d92a86f5ce835ee0',
            'uncles': []
          })

        :param hash_: block hash identifier
        :return:
        """
        hash_ = add_0x_prefix(hash_)

        log.debug(f"Getting block '{hash_}'")

        key = f"_block_{hash_}"
        block = self._cache.get(key)

        if not block:
            def load_block(s):
                obj = s.execute(
                    select(orm.Block)
                        .filter(orm.Block.hash == hash_)
                ).one_or_none()
                return EventIndexerExchange._sanitize_db_result(obj)

            with self._db.session() as session:
                block = load_block(session)

                if block is None:
                    try:
                        block_info = self._w3.eth.get_block(hash_)
                    except BlockNotFound:
                        log.error(f"Failed to fetch block '{hash_}'")
                        raise

                    assert hash_ == block_info.hash.hex()

                    block = orm.Block(
                        hash=hash_,
                        number=block_info.number,
                        timestamp=block_info.timestamp,
                    )

                    session.add(block)

                    # handle race conditions
                    try:
                        session.commit()
                    except sqlalchemy.exc.IntegrityError:
                        session.rollback()
                        block = load_block(session)

            self._cache.set(key, block, ttl=300)

        # ensure only persistent/detached objects get loaded from the cache
        assert block.id is not None

        return block

    def _get_tx(self, hash_: HexStr) -> orm.Transaction:
        """
        Get a transaction object

        Example tx info:
          AttributeDict({
            'blockHash': '0x8ed42786cb8fa0aa8ef0121cfc50b7e23277d513b5f4486078141a9f540d982b',
            'blockNumber': 57347,
            'from': '0x808cE8deC9E10beD8d0892aCEEf9F1B8ec2F52Bd',
            'gas': 3817586,
            'gasPrice': 470000000000,
            'hash': '0xd5fae76d06da05c5a7629cd40b8005fe00c83b19b573f218503f93bf866350e4',
            'input': '0xe8e337000000000000000000000000....74f80fa31621612887d26df40bcf0ca900000000000',
            'nonce': 122,
            'r': '0x6ea16a5f3f0ad6cd83be4a66ba8f95b8b6e2756392c027a9876dc01e781d986e',
            's': '0x3d0dcff13efb14eebf89d350895f0a5281f9edb72f799aafd5b8e6076201d3ed',
            'to': '0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106',
            'transactionIndex': 0,
            'type': '0x0',
            'v': 86263,
            'value': 0
          })

        :param hash_: transaction hash identifier
        :return:
        """
        hash_ = add_0x_prefix(hash_)

        log.debug(f"Getting tx '{hash_}'")

        key = f"_tx_{hash_}".lower()
        tx = self._cache.get(key)

        if not tx:
            def load_tx(s):
                obj = s.execute(
                    select(orm.Transaction)
                        .filter(orm.Transaction.hash == hash_)
                ).one_or_none()
                return EventIndexerExchange._sanitize_db_result(obj)

            with self._db.session() as session:
                tx = load_tx(session)

                if tx is None:
                    try:
                        tx_info = self._w3.eth.get_transaction(hash_)
                    except TransactionNotFound:
                        log.error(f"Failed to fetch tx '{hash_}'")
                        raise

                    block = self._get_block(tx_info.blockHash.hex())

                    assert hash_ == tx_info.hash.hex()

                    tx = orm.Transaction(
                        hash=hash_,
                        from_=tx_info["from"],
                        block_id=block.id,
                        timestamp=block.timestamp,
                    )

                    session.add(tx)

                    # handle race conditions
                    try:
                        session.commit()
                    except sqlalchemy.exc.IntegrityError:
                        session.rollback()
                        tx = load_tx(session)

            self._cache.set(key, tx, ttl=300)

        # ensure only persistent/detached objects get loaded from the cache
        assert tx.id is not None

        return tx

    def _get_factory(self, address: AnyAddress) -> orm.Factory:
        """
        Get a factory object

        :param address: factory contract address
        :return:
        """
        address = Web3.toChecksumAddress(address)

        log.debug(f"Getting factory '{address}'")

        key = f"_factory_{address}"
        factory = self._cache.get(key)

        if not factory:
            def load_factory(s):
                obj = s.execute(
                    select(orm.Factory)
                        .filter(orm.Factory.address == address)
                ).one_or_none()
                return EventIndexerExchange._sanitize_db_result(obj)

            with self._db.session() as session:
                factory = load_factory(session)

                if factory is None:
                    factory = orm.Factory(
                        address=address,
                        pairCount=0,
                        totalVolumeUSD=0,
                        totalVolumeNative=0,
                        untrackedVolumeUSD=0,
                        totalLiquidityUSD=0,
                        totalLiquidityNative=0,
                        txCount=0,
                    )

                    session.add(factory)

                    # handle race conditions
                    try:
                        session.commit()
                    except sqlalchemy.exc.IntegrityError:
                        session.rollback()
                        factory = load_factory(session)

            self._cache.set(key, factory)

        # ensure only persistent/detached objects get loaded from the cache
        assert factory.id is not None

        return factory

    def _fetch_token_info(self, address: AnyAddress) -> Tuple[str, str, int, int]:
        """
        Fetch token info from an RC20 contract

        :param address: RC20 token contract address
        :return:
        """
        address = Web3.toChecksumAddress(address)
        contract = self._w3.eth.contract(address=address, abi=self._abi_rc20)
        contract_bytes = self._w3.eth.contract(address=address, abi=rc20_bytes.abi)

        # TODO convert to batch request

        # try types string and bytes32 for symbol
        symbol = "unknown"
        try:
            symbol = contract.functions.symbol().call()
        except (BadFunctionCallOutput, ContractLogicError, OverflowError):
            try:
                symbol_bytes = contract_bytes.functions.symbol().call()
                symbol = symbol_bytes.decode("utf-8")
            except (BadFunctionCallOutput, ContractLogicError, OverflowError, ValueError):
                log.warning(f"Encountered uncommon token contract '{address}' (symbol func)")

        assert isinstance(symbol, str)
        symbol = symbol[:16]

        # try types string and bytes32 for name
        name = "unknown"
        try:
            name = contract.functions.name().call()
        except (BadFunctionCallOutput, ContractLogicError, OverflowError):
            try:
                name_bytes = contract_bytes.functions.name().call()
                name = name_bytes.decode("utf-8")
            except (BadFunctionCallOutput, ContractLogicError, OverflowError, ValueError):
                log.warning(f"Encountered uncommon token contract '{address}' (name func)")

        assert isinstance(name, str)
        name = name[:64]

        decimals = 0
        try:
            decimals = contract.functions.decimals().call()
        except (BadFunctionCallOutput, ContractLogicError, OverflowError):
            log.warning(f"Encountered uncommon token contract '{address}' (decimals func)")

        assert isinstance(decimals, int)

        total_supply = 0
        try:
            total_supply = contract.functions.totalSupply().call()
        except (BadFunctionCallOutput, ContractLogicError, OverflowError):
            log.warning(f"Encountered uncommon token contract '{address}' (totalSupply func)")

        assert isinstance(total_supply, int)

        return symbol, name, decimals, total_supply

    def _get_token(self, address: AnyAddress) -> orm.Token:
        """
        Get an RC20 token object

        :param address: rc20 token contract address
        :return: token
        """
        address = Web3.toChecksumAddress(address)

        log.debug(f"Getting token '{address}'")

        key = f"_token_{address}"
        token = self._cache.get(key)

        if not token:
            def load_token(s):
                obj = s.execute(
                    select(orm.Token)
                        .filter(orm.Token.address == address)
                ).one_or_none()
                return EventIndexerExchange._sanitize_db_result(obj)

            with self._db.session() as session:
                token = load_token(session)

                if token is None:
                    symbol, name, decimals, total_supply = self._fetch_token_info(address)

                    # TODO Currently we cannot handle more digits (underlying db data type)
                    assert decimals <= MAX_DECIMAL_PLACES

                    token = orm.Token(
                        address=address,
                        symbol=symbol,
                        name=name,
                        decimals=decimals,
                        totalSupply=total_supply,
                        tradeVolume=0,
                        tradeVolumeUSD=0,
                        untrackedVolumeUSD=0,
                        txCount=0,
                        totalLiquidity=0,
                        derivedNative=0,
                    )

                    session.add(token)

                    # handle race conditions
                    try:
                        session.commit()
                    except sqlalchemy.exc.IntegrityError:
                        session.rollback()
                        token = load_token(session)

            self._cache.set(key, token)

        # ensure only persistent/detached objects get added to the cache
        assert token.id is not None

        return token

    def _get_user(self, address: AnyAddress) -> orm.User:
        """
        Get a user object

        :param address: user wallet address
        :return:
        """
        address = Web3.toChecksumAddress(address)

        log.debug(f"Getting User '{address}'")

        key = f"_user_{address}"
        user = self._cache.get(key)

        if not user:
            def load_user(s):
                obj = s.execute(
                    select(orm.User)
                        .filter(orm.User.address == address)
                ).one_or_none()
                return EventIndexerExchange._sanitize_db_result(obj)

            with self._db.session() as session:
                user = load_user(session)

                if user is None:
                    user = orm.User(
                        address=address,
                        usdSwapped=0,
                    )

                    session.add(user)

                    # handle race conditions
                    try:
                        session.commit()
                    except sqlalchemy.exc.IntegrityError:
                        session.rollback()
                        user = load_user(session)

            self._cache.set(key, user, ttl=3600)

        # ensure only persistent/detached objects get added to the cache
        assert user.id is not None

        return user

    def _load_pair(self, address: AnyAddress, timeout: int = 600) -> orm.Pair:
        """
        Load a pair object

        Note: can return a transient object (no database identity)
        Warning: potential deadlock

        Two things can happen:
          - the PairCreated event was emitted in the same block as another event and the resulting pair object
            has not yet been committed to the database. We use a local per worker cache to temporarily share such objects.
          - the PairCreated event was handled by worker1, but we need the pair object in worker2 to process another event.
            In this case, wait for worker1 to finish the job (multiple events) and the DBhandler to commit the result.
            At that point the pair can be loaded from the database and is available to all workers.
            This approach has several problems:
              - potential deadlock (w1 with pair1 and event that needs pair2, w2 with pair2 and event that needs pair1)
              - might hit a timeout, if the DBhandler can't commit events fast enough

        TODO consider caching the pair object in the global redis cache

        :param address: pair contract checksum address
        :param timeout: raises TimeoutError, if the pair object was not found within ``timeout`` seconds
        :return:
        """
        address = Web3.toChecksumAddress(address)

        key = f"_pair_{address}"
        pair = self._local_cache.get(key)

        if pair is None:
            start = time.time()
            with self._db.session() as session:
                while time.time() - start < timeout:
                    pair = session.execute(
                        select(orm.Pair)
                            .filter(orm.Pair.address == address)
                    ).scalar()

                    if pair is None:
                        time.sleep(0.2)
                        continue
                    else:
                        elapsed = time.time() - start
                        if elapsed > 0.2:
                            log.warning(f"Waited {elapsed:0.4f}s for pair '{address}' orm object")
                        return pair

                # timeout
                raise TimeoutError

        else:
            return pair

    def _handle_pair_created(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        """
        Process a ``PairCreated`` event (factory contract)

        Prototype:
          event PairCreated(address indexed token0, address indexed token1, address pair, uint);

        Example entry:
          AttributeDict({
            'address': '0xefa94DE7a4656D787667C749f7E1223D71E9FD88',
            'blockHash': '0x8ed42786cb8fa0aa8ef0121cfc50b7e23277d513b5f4486078141a9f540d982b',
            'blockNumber': 57347,
            'data': '0x000000000000000000000000a37cd29a87975f44b83f06f9ba4d51879a99d3780000000000000000000000000000000000000000000000000000000000000001',
            'dataDecoded': {'': 1,
                            'pair': '0xa37cd29A87975f44b83F06F9BA4D51879a99d378',
                            'token0': '0x97b99B4009041e948337ebCA7e6ae52f9f6e633c',
                            'token1': '0xa47a05ED74f80fA31621612887d26DF40bcF0cA9'},
            'logIndex': 0,
            'name': 'PairCreated',
            'removed': False,
            'topics': ['0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9',
                       '0x00000000000000000000000097b99b4009041e948337ebca7e6ae52f9f6e633c',
                       '0x000000000000000000000000a47a05ed74f80fa31621612887d26df40bcf0ca9'],
            'transactionHash': '0xd5fae76d06da05c5a7629cd40b8005fe00c83b19b573f218503f93bf866350e4',
            'transactionIndex': 0
          })

        Examples:
          - pair created event
            https://snowtrace.io/tx/0xc184868ec951dc039d46bfe8a42605954583e8bdad5ce5c6d837b538bf564973#eventlog

        :param entry: event log entry
        :return:
        """
        args = entry.dataDecoded

        # TODO possibly move to post processing
        factory = self._get_factory(self._factory_address)

        block = self._get_block(hash_=entry.blockHash.hex())
        token0 = self._get_token(address=args.token0)
        token1 = self._get_token(address=args.token1)

        address = Web3.toChecksumAddress(args.pair)

        pair = orm.Pair(
            address=address,
            token0_address=token0.address,
            token1_address=token1.address,
            reserve0=0,
            reserve1=0,
            totalSupply=0,
            reserveNative=0,
            reserveUSD=0,
            trackedReserveNative=0,
            token0Price=0,
            token1Price=0,
            volumeToken0=0,
            volumeToken1=0,
            volumeUSD=0,
            untrackedVolumeUSD=0,
            txCount=0,
            createdAtTimestamp=block.timestamp,
            createdAtBlockNumber=block.number,
            block_id=block.id,
            liquidityProviderCount=0,
        )

        key = f"_pair_{address}"
        self._local_cache.set(key, pair)

        return [pair]

    def _handle_transfer(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        """
        Process a ``Transfer`` event (pair contract)

        Note: orm transfer objects are ONLY created for transfers not related to mint/burn (e.g. user transfers)

        Prototype:
          event Transfer(address indexed from, address indexed to, uint value);

        Example entry:
          AttributeDict({
            "address": "0xd7538cABBf8605BdE1f4901B47B8D42c61DE0367",
            "blockHash": "0xdfd02169408e32faf1e9a83471aba1515f9d6ff24ef78d7e0a645fec30608bb4",
            "blockNumber": 64462,
            "data": "0x000000000000000000000000000000000000000000000000b9a4b401f2cce928",
            "dataDecoded": {"from": "0x0000000000000000000000000000000000000000",
                            "to": "0x0Ff0780b031c29557262149b44876560c34BA1AA",
                            "value": 13377014713658698024},
            "logIndex": 3,
            "name": "Transfer",
            "removed": false,
            "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                      "0x0000000000000000000000000000000000000000000000000000000000000000",
                      "0x0000000000000000000000000ff0780b031c29557262149b44876560c34ba1aa"],
            "transactionHash": "0xc1c8bcd6267ebe9b738fd5489be9df341d0d6afa20d59a0d39e0c31f7697183b",
            "transactionIndex": 0
        })

        Examples:
          - transfer event
            https://snowtrace.io/tx/0xab351b4c9ff15ccb9b7d3a53b61355efb7d7ef3bdf0c7da5d6a1378058c3809c#eventlog

        :param entry: event log entry
        :return:
        """
        ADDRESS_ZERO = "0x0000000000000000000000000000000000000000"
        MINIMUM_LIQUIDITY = 1000

        args = entry.dataDecoded
        pair_address = Web3.toChecksumAddress(entry.address)

        # ignore initial transfers for first adds
        if args.to == ADDRESS_ZERO and args.value == MINIMUM_LIQUIDITY:
            log.debug("Skipping minimum liquidity transfer")
            return []

        # create users
        user_from = self._get_user(address=args["from"])
        user_to = self._get_user(address=args.to)

        tx = self._get_tx(entry.transactionHash.hex())

        # keep cache per tx
        if not tx.hash in self._mints:
            self._mints[tx.hash] = []

        if not tx.hash in self._burns:
            self._burns[tx.hash] = []

        # liquidity token amount being transferred
        value = token_to_decimal(args.value, 18)

        # mints
        if args["from"] == ADDRESS_ZERO:
            mints = self._mints[tx.hash]

            # create new mint, if no mints so far or last one is already done
            if len(mints) == 0 or is_complete(mints[-1]):
                mint = orm.Mint(
                    transaction_id=tx.id,
                    pair_address=Web3.toChecksumAddress(entry.address),
                    timestamp=tx.timestamp,
                    liquidity=value,
                    sender=None,
                    amount0=Decimal("0"),
                    amount1=Decimal("0"),
                    to=Web3.toChecksumAddress(args.to),
                    logIndex=None,
                    amountUSD=Decimal("0"),
                    feeTo=None,
                    feeLiquidity=None,
                )
                self._mints[tx.hash].append(mint)

            else:
                # if this logical mint included a fee mint, account for it
                mint = mints[-1]

                mint.feeTo = mint.to
                mint.to = Web3.toChecksumAddress(args.to)
                mint.feeLiquidity = mint.liquidity
                mint.liquidity = value

        # burns
        # case where direct send first on native asset withdrawals
        if args.to == pair_address:
            burn = orm.Burn(
                transaction_id=tx.id,
                pair_address=Web3.toChecksumAddress(entry.address),
                timestamp=tx.timestamp,
                liquidity=value,
                sender=Web3.toChecksumAddress(args["from"]),
                amount0=Decimal("0"),
                amount1=Decimal("0"),
                to=Web3.toChecksumAddress(args.to),
                logIndex=None,
                amountUSD=Decimal("0"),
                needsComplete=True,
                feeTo=None,
                feeLiquidity=None,
            )
            self._burns[tx.hash].append(burn)

        if args["from"] == pair_address and args.to == ADDRESS_ZERO:
            burns = self._burns[tx.hash]

            burn = None
            if len(burns) > 0:
                current_burn = burns[-1]
                if current_burn.needsComplete:
                    burn = current_burn

            if burn is None:
                burn = orm.Burn(
                        transaction_id=tx.id,
                        pair_address=Web3.toChecksumAddress(entry.address),
                        timestamp=tx.timestamp,
                        liquidity=value,
                        sender=None,
                        amount0=Decimal("0"),
                        amount1=Decimal("0"),
                        to=None,
                        logIndex=None,
                        amountUSD=Decimal("0"),
                        needsComplete=False,
                        feeTo=None,
                        feeLiquidity=None,
                    )

            # if this logical burn included a fee mint, account for this
            mints = self._mints[tx.hash]
            if len(mints) > 0 and not is_complete(mints[-1]):
                mint = mints[-1]
                burn.feeTo = Web3.toChecksumAddress(mint.to)
                burn.feeLiquidity = mint.liquidity

                # remove the logical mint
                del mints[-1]

            # if accessing last one, update it, else add new one
            if burn.needsComplete:
                burn.needsComplete = False
            else:
                self._burns[tx.hash].append(burn)

        objects = []
        if (args["from"] not in {ADDRESS_ZERO, pair_address}) or (args.to not in {ADDRESS_ZERO, pair_address}):
            transfer = orm.Transfer(
                transaction_id=tx.id,
                pair_address=pair_address,
                from_=Web3.toChecksumAddress(args["from"]),
                to=Web3.toChecksumAddress(args.to),
                value=value,
                logIndex=entry.logIndex,
            )
            objects.append(transfer)

        return objects

    def _handle_burn(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        """
        Process a ``Burn`` event (pair contract)

        Note: always has several preceding events
          - 1-3 Transfer
          - 1 Sync

        Prototype:
          event Burn(address indexed sender, uint amount0, uint amount1, address indexed to);

        Pair contract burn function flow:
          transferFrom(msg.sender, pair, liquidity) CONDITIONAL
            emit Transfer(from, to, value)
          _mintFee(_reserve0, _reserve1) CONDITIONAL
            _mint(feeTo, liquidity)
              emit Transfer(address(0), to, value)
          _burn(address(this), liquidity)
            emit Transfer(from, address(0), value)
          _safeTransfer(_token0, to, amount0) FILTERED -> emitted by token contract
             emit Transfer(from, to, value)
          _safeTransfer(_token1, to, amount1) FILTERED -> emitted by token contract
             emit Transfer(from, to, value)
          _update(balance0, balance1, _reserve0, _reserve1)
            emit Sync(reserve0, reserve1)
          emit Burn(msg.sender, amount0, amount1, to)

        Example entry:
          AttributeDict({
            "address": "0x1aCf1583bEBdCA21C8025E172D8E8f2817343d65",
            "blockHash": "0x46fb03e3e5ac271708d4dd18354f7a5b94cee529aa99d2f50c594df4042984e3",
            "blockNumber": 65299,
            "data": "0x0000000000000000000000000000000000000000000000057a67bd96741d89f300000000000000000000000000000000000000000000000016563d0b63132a81",
            "dataDecoded": {"sender": "0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106",
                            "to": "0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106",
                            "amount0": 101053947217667000819,
                            "amount1": 1609541035947666049},
            "logIndex": 5,
            "name": "Burn",
            "removed": false,
            "topics": ["0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496",
                       "0x000000000000000000000000e54ca86531e17ef3616d22ca28b0d458b6c89106",
                       "0x000000000000000000000000e54ca86531e17ef3616d22ca28b0d458b6c89106"],
            "transactionHash": "0xab351b4c9ff15ccb9b7d3a53b61355efb7d7ef3bdf0c7da5d6a1378058c3809c",
            "transactionIndex": 0
            })

        Examples:
          - burn event
            https://snowtrace.io/tx/0xab351b4c9ff15ccb9b7d3a53b61355efb7d7ef3bdf0c7da5d6a1378058c3809c#eventlog
          - burn event (mint enabled)
            https://snowtrace.io/tx/0xad14a2a903ba02f04cf54f6158b7950df893e2f7fe97c2100c9de1800a79c50c#eventlog
          - burn event (no direct transfer)
            https://snowtrace.io/tx/0xab923fd14687ec6b6225eb8d0237120c3d7e8dcba550f2b7dd9fcfc161350b4e#eventlog
            https://snowtrace.io/tx/0x738eea1f9543cd2019c3a606cc2b3e03efeaf633084eff2bf5d4f6c1b2890520#eventlog
          - burn event (mint enabled, swap in same tx)
            https://snowtrace.io/tx/0xe414b312457981f0b2e689d95240ce1352fe35676ee43c854cddb5a7d437c471#eventlog
          - burn event (incomplete)
            https://snowtrace.io/tx/0x10852ccc105abc262b5b8d1369c93a85e7fb9198f3c34aaac5907dbe449897dd#eventlog

        :param entry: event log entry
        :return:
        """
        args = entry.dataDecoded

        pair = self._load_pair(entry.address)
        tx = self._get_tx(entry.transactionHash.hex())
        token0 = self._get_token(pair.token0_address)
        token1 = self._get_token(pair.token1_address)

        token0_amount = token_to_decimal(args.amount0, token0.decimals)
        token1_amount = token_to_decimal(args.amount1, token1.decimals)

        assert tx.hash in self._burns
        burns = self._burns[tx.hash]

        assert len(burns) > 0
        burn = burns[-1]

        burn.amount0 = token0_amount
        burn.amount1 = token1_amount
        burn.logIndex = entry.logIndex
        burn.amountUSD = Decimal("0")

        return [burn]

    def _handle_mint(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        """
        Process a ``Mint`` event (pair contract)

        Note: always has several preceding events
          - 1-2 Transfer
          - 1 Sync

        Prototype:
          event Mint(address indexed sender, uint amount0, uint amount1);

        Pair contract mint function flow:
          _mintFee(_reserve0, _reserve1) CONDITIONAL
            _mint(feeTo, liquidity)
              emit Transfer(address(0), to, value)
          _mint(address(0), MINIMUM_LIQUIDITY) OR _mint(to, liquidity)
            emit Transfer(address(0), to, value)
          _update(balance0, balance1, _reserve0, _reserve1)
            emit Sync(reserve0, reserve1)
          emit Mint(msg.sender, amount0, amount1)

        Example entry:
          AttributeDict({
            "address": "0x17a2E8275792b4616bEFb02EB9AE699aa0DCb94b",
            "blockHash": "0xbb1b388b15b229572fe02d1688d97323b8c897995ae3dccc328c2111b2e7c321",
            "blockNumber": 64638,
            "data": "0x0000000000000000000000000000000000000000000000004a720f1a2b408f36000000000000000000000000000000000000000000000007ef8e6dc064870baf",
            "dataDecoded": {"sender": "0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106",
                            "amount0": 5364366711220899638,
                            "amount1": 146389063610812271535},
            "logIndex": 6,
            "name": "Mint",
            "removed": false,
            "topics": ["0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f",
                       "0x000000000000000000000000e54ca86531e17ef3616d22ca28b0d458b6c89106"],
            "transactionHash": "0xfe3dd0340c96bd88002ac1f08931926741cd79efd672da5dd2296c8e0a06ab1d",
            "transactionIndex": 0
          })

        Examples:
          - mint event
            https://snowtrace.io/tx/0xc1c8bcd6267ebe9b738fd5489be9df341d0d6afa20d59a0d39e0c31f7697183b#eventlog
          - mint event (fee enabled)
            https://snowtrace.io/tx/0x9062db269bd5e030764132dc22e109ebcaf5e474675a3519e79627a2172c1179#eventlog


        :param entry: event log entry
        :return:
        """
        args = entry.dataDecoded

        pair = self._load_pair(entry.address)
        tx = self._get_tx(entry.transactionHash.hex())
        token0 = self._get_token(pair.token0_address)
        token1 = self._get_token(pair.token1_address)

        token0_amount = token_to_decimal(args.amount0, token0.decimals)
        token1_amount = token_to_decimal(args.amount1, token1.decimals)

        assert tx.hash in self._mints
        mints = self._mints[tx.hash]

        assert len(mints) > 0
        mint = mints[-1]

        mint.sender = Web3.toChecksumAddress(args.sender)
        mint.amount0 = token0_amount
        mint.amount1 = token1_amount
        mint.logIndex = entry.logIndex
        mint.amountUSD = Decimal("0")

        return [mint]

    def _handle_swap(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        """
        Process a ``Swap`` event (pair contract)

        Note: always has one preceding event
          - 1 Sync

        Prototype:
          event Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out,  address indexed to);

        Pair contract swap function flow:
          _safeTransfer(_token0, to, amount0Out) FILTERED -> emitted by token contract
            emit Transfer(from, to, value)
          _safeTransfer(_token1, to, amount1Out) FILTERED -> emitted by token contract
            emit Transfer(from, to, value)
          _update(balance0, balance1, _reserve0, _reserve1)
            emit Sync(reserve0, reserve1)
          emit Swap(msg.sender, amount0In, amount1In, amount0Out, amount1Out, to)

        Example entry:
          AttributeDict({
            "address": "0xd7538cABBf8605BdE1f4901B47B8D42c61DE0367",
            "blockHash": "0x8f0d823889e8777508e8319a2f008ec8fac079df290958b45cf97327bfabb1a9",
            "blockNumber": 65267,
            "data": "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000053444835ec5800000000000000000000000000000000000000000000000000002ae0d4da8683327c0000000000000000000000000000000000000000000000000000000000000000",
            "dataDecoded": {"sender": "0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106",
                            "to": "0x0C2679e44852C62486AA0DA7cFB705877f30B5A7",
                            "amount0In": 0,
                            "amount1In": 6000000000000000000,
                            "amount0Out": 3089703379400864380,
                            "amount1Out": 0},
            "logIndex": 4,
            "name": "Swap",
            "removed": false,
            "topics": ["0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
                       "0x000000000000000000000000e54ca86531e17ef3616d22ca28b0d458b6c89106",
                       "0x0000000000000000000000000c2679e44852c62486aa0da7cfb705877f30b5a7"],
            "transactionHash": "0xc1636e49c0115e62893402ba917fa3510ec507fcbe2a0847a5aae4c9ec512c12",
            "transactionIndex": 0
          })

        Examples:
        - swap event
          https://snowtrace.io/tx/0xc1636e49c0115e62893402ba917fa3510ec507fcbe2a0847a5aae4c9ec512c12#eventlog
        - swap event (via router)
          https://snowtrace.io/tx/0xe27926d6e184ad433bf60e74eb7960599bc6a566f6fcc5860fa0fa4ea3c9b4cf#eventlog

        :param entry: event log entry
        :return:
        """
        args = entry.dataDecoded

        pair = self._load_pair(entry.address)
        tx = self._get_tx(entry.transactionHash.hex())
        token0 = self._get_token(pair.token0_address)
        token1 = self._get_token(pair.token1_address)

        # check if sender and dest are equal to the router
        # if so, change the 'to' address to the tx issuer
        dest = args.to
        if args.sender == self._router_address and args.to == self._router_address:
            dest = tx.from_

        amount0_in = token_to_decimal(args.amount0In, token0.decimals)
        amount1_in = token_to_decimal(args.amount1In, token1.decimals)
        amount0_out = token_to_decimal(args.amount0Out, token0.decimals)
        amount1_out = token_to_decimal(args.amount1Out, token1.decimals)

        swap = orm.Swap(
            transaction_id=tx.id,
            pair_address=Web3.toChecksumAddress(entry.address),
            timestamp=tx.timestamp,
            sender=args.sender,
            from_=tx.from_,
            amount0In=amount0_in,
            amount1In=amount1_in,
            amount0Out=amount0_out,
            amount1Out=amount1_out,
            to=dest,
            logIndex=entry.logIndex,
            amountUSD=Decimal("0"),
        )

        return [swap]

    def _handle_sync(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        """
        Process a ``Sync`` event (pair contract)

        Prototype:
          event Sync(uint112 reserve0, uint112 reserve1);

        Example entry:
          AttributeDict({
            "address": "0xd7538cABBf8605BdE1f4901B47B8D42c61DE0367",
            "blockHash": "0xdfd02169408e32faf1e9a83471aba1515f9d6ff24ef78d7e0a645fec30608bb4",
            "blockNumber": 64462,
            "data": "0x00000000000000000000000000000000000000000000000051b3058123ed3c5a00000000000000000000000000000000000000000000000394bc2b6217787539",
            "dataDecoded": {"reserve0": 5887055190115040346,
                            "reserve1": 66057721134664152377},
            "logIndex": 4,
            "name": "Sync",
            "removed": false,
            "topics": ["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],
            "transactionHash": "0xc1c8bcd6267ebe9b738fd5489be9df341d0d6afa20d59a0d39e0c31f7697183b",
            "transactionIndex": 0
          })

        Examples:
        - sync event
          https://snowtrace.io/tx/0xc1636e49c0115e62893402ba917fa3510ec507fcbe2a0847a5aae4c9ec512c12#eventlog

        :param entry: event log entry
        :return:
        """
        args = entry.dataDecoded

        pair = self._load_pair(entry.address)
        tx = self._get_tx(entry.transactionHash.hex())
        token0 = self._get_token(pair.token0_address)
        token1 = self._get_token(pair.token1_address)

        reserve0 = token_to_decimal(args.reserve0, token0.decimals)
        reserve1 = token_to_decimal(args.reserve1, token1.decimals)

        sync = orm.Sync(
            transaction_id=tx.id,
            pair_address=Web3.toChecksumAddress(entry.address),
            reserve0=reserve0,
            reserve1=reserve1,
            logIndex=entry.logIndex,
        )

        return [sync]

    @classmethod
    def setup(cls, w3: Web3, db: xquery.db.FusionSQL, start_block: int) -> List[orm.Base]:
        try:
            block_info = w3.eth.get_block(start_block)
        except BlockNotFound:
            raise

        block = orm.Block(
            hash=block_info.hash.hex(),
            number=block_info.number,
            timestamp=block_info.timestamp,
        )

        return [block]

    def process(self, entry: ExtendedLogReceipt) -> List[orm.Base]:
        """
        Index an event log entry

        :param entry: event log entry
        :return:
        """
        # TODO Currently we cannot handle this (prevent db corruption)
        assert not entry.removed

        objects = []
        if entry.name == "PairCreated":
            result = self._handle_pair_created(entry)
            objects.extend(result)
        elif entry.name == "Transfer":
            result = self._handle_transfer(entry)
            objects.extend(result)
        elif entry.name == "Burn":
            result = self._handle_burn(entry)
            objects.extend(result)
        elif entry.name == "Mint":
            result = self._handle_mint(entry)
            objects.extend(result)
        elif entry.name == "Swap":
            result = self._handle_swap(entry)
            objects.extend(result)
        elif entry.name == "Sync":
            result = self._handle_sync(entry)
            objects.extend(result)
        else:
            log.warning(f"Encountered unknown event '{entry.name}'")

        return objects


class EventIndexerExchangePangolin(EventIndexerExchange):

    def __init__(self, w3: Web3, db: xquery.db.FusionSQL, cache: xquery.cache.Cache):
        """
        Event indexer for the Pangolin Exchange (on AVAX)
        """
        # TODO
        # assert w3.eth.chain_id == int(orm.Chain.AVAX)

        super().__init__(
            w3=w3,
            db=db,
            cache=cache,
            abi_rc20=png_rc20.abi,
            factory_address=png_factory.address,
            router_address=png_router.address,
        )


class EventIndexerExchangePegasys(EventIndexerExchange):

    def __init__(self, w3: Web3, db: xquery.db.FusionSQL, cache: xquery.cache.Cache):
        """
        Event indexer for the Pegasys Exchange (on SYS)
        """
        # TODO
        # assert w3.eth.chain_id == int(orm.Chain.SYS)

        super().__init__(
            w3=w3,
            db=db,
            cache=cache,
            abi_rc20=psys_rc20.abi,
            factory_address=psys_factory.address,
            router_address=psys_router.address,
        )
