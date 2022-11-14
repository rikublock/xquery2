#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import json
import logging
import pprint
import requests
import sys

from xquery.config import CONFIG as C
from xquery.util.misc import timeit

log = logging.getLogger(__name__)


@timeit
def main() -> int:
    logging.basicConfig(level=logging.DEBUG, format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])

    host = "localhost"
    port = 8080
    url = f"http://{host}:{port}/v1/metadata"

    db_schema = C["DB_SCHEMA"]

    tables = [
        "factory",
        "token",
        "pair",
        "user",
        "liquidity_position",
        "liquidity_position_snapshot",
        "block",
        "transaction",
        "transfer",
        "mint",
        "burn",
        "swap",
        "sync",
        "bundle",
        "exchange_day_data",
        "pair_hour_data",
        "pair_day_data",
        "token_day_data",
    ]

    relationships_object = {
        # table.name: foreign_key
        "burn.transaction": "transaction_id",
        "burn.pair": "pair_address",

        "liquidity_position.user": "user_id",
        "liquidity_position.pair": "pair_address",

        "liquidity_position_snapshot.block": "block_id",
        # "liquidity_position_snapshot.liquidityPosition": "liquidityPosition_id",
        "liquidity_position_snapshot.user": "user_id",
        "liquidity_position_snapshot.pair": "pair_address",

        "mint.transaction": "transaction_id",
        "mint.pair": "pair_address",

        "pair.token0": "token0_address",
        "pair.token1": "token1_address",
        "pair.block": "block_id",

        # "pair_day_data.token0": "token0_id",
        # "pair_day_data.token1": "token1_id",

        "swap.transaction": "transaction_id",
        "swap.pair": "pair_address",

        "sync.transaction": "transaction_id",
        "sync.pair": "pair_address",

        # "token_day_data.token": "token_id",

        "transaction.block": "block_id",

        "transfer.transaction": "transaction_id",
        "transfer.pair": "pair_address",
    }

    relationships_array = {
        # table.name: foreign_key (table, column)
        "block.transactions": ("transaction", "block_id"),

        "pair.pairHourData": ("pair_hour_data", "pair_address"),
        "pair.liquidityPositions": ("liquidity_position", "pair_address"),
        "pair.liquidityPositionSnapshots": ("liquidity_position_snapshot", "pair_address"),
        "pair.mints": ("mint", "pair_address"),
        "pair.burns": ("burn", "pair_address"),
        "pair.swaps": ("swap", "pair_address"),

        # "token.tokenDayData": ("token_day_data", "token"),
        # "token.pairDayDataBase": ("pair_day_data", "token0_id"),
        # "token.pairDayDataQuote": ("pair_day_data", "token1_id"),
        # "token.pairBase": ("pair", "token0_address"),
        # "token.pairQuote": ("Pair", "token1_address"),

        "user.liquidityPositions": ("liquidity_position", "user_id"),
        "user.liquidityPositionSnapshots": ("liquidity_position_snapshot", "user_id"),
    }

    headers = {
        "Content-Type": "application/json",
        "X-Hasura-Role": "admin",
    }

    payload = {
        "type": "bulk",
        "args": [
            # filled bellow
        ],
    }

    # track tables
    # see: https://hasura.io/docs/latest/schema/postgres/tables/
    assert len(tables) > 0
    for table in tables:
        payload["args"].append({
            "type": "pg_track_table",
            "args": {
                "schema": db_schema,
                "name": table,
            },
        })

    # track relationships
    # see: https://hasura.io/docs/latest/schema/postgres/table-relationships/create/
    for key, value in relationships_object.items():
        table, name = key.split(".")
        payload["args"].append({
            "type": "pg_create_object_relationship",
            "args": {
                "table": {
                    "schema": db_schema,
                    "name": table,
                },
                "name": name,
                "using": {
                    "foreign_key_constraint_on": value,
                },
            },
        })

    for key, value in relationships_array.items():
        table, name = key.split(".")
        payload["args"].append({
            "type": "pg_create_array_relationship",
            "args": {
                "table": {
                    "schema": db_schema,
                    "name": table,
                },
                "name": name,
                "using": {
                    "foreign_key_constraint_on": {
                        "table": {
                            "schema": db_schema,
                            "name": value[0],
                        },
                        "column": value[1],
                    }
                }
            },
        })

    log.debug(pprint.pformat(payload))

    # make the request
    r = requests.post(url, data=json.dumps(payload), headers=headers)

    log.debug(pprint.pformat(r.content))
    r.raise_for_status()

    # check each response status
    results = r.json()
    for result in results:
        assert result["message"] == "success"

    return 0


if __name__ == "__main__":
    sys.exit(main())
