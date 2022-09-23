#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import os
import logging


DEFAULT = {
    # Logging settings
    "LOG_LEVEL": logging.INFO,
    "LOG_FORMAT": "%(asctime)s.%(msecs)04d %(levelname)-5s [%(threadName)-10s %(process)5d] %(name)s: %(message)s",
    "LOG_DATE_FORMAT": "%H:%M:%S",

    # Database settings
    "DB_DRIVER": "postgresql",
    "DB_HOST": os.getenv("DB_HOST", "localhost"),
    "DB_PORT": os.getenv("DB_PORT", 5432),
    "DB_USERNAME": os.getenv("DB_USERNAME", "root"),
    "DB_PASSWORD": os.getenv("DB_PASSWORD", "password"),
    "DB_DATABASE": os.getenv("DB_DATABASE", "debug"),
    "DB_SCHEMA": os.getenv("DB_SCHEMA", "public"),

    "DB_DEBUG": False,

    # Redis cache settings
    "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
    "REDIS_PORT": os.getenv("REDIS_PORT", 6379),
    "REDIS_PASSWORD": os.getenv("REDIS_PASSWORD", "password"),
    "REDIS_DATABASE": os.getenv("REDIS_DATABASE", 0),

    # Controller settings
    "XQ_NUM_WORKERS": os.getenv("XQ_NUM_WORKERS", 16),

    # web3 provider RPC url
    "API_URL": os.getenv("API_URL", "http://localhost:8545/"),
    # "API_URL": os.getenv("API_URL", "https://cloudflare-eth.com/v1/mainnet"),  # ETH
    # "API_URL": os.getenv("API_URL", "https://api.avax.network/ext/bc/C/rpc"),  # AVAX
    # "API_URL": os.getenv("API_URL", "https://rpc.syscoin.org/"),  # SYS
}

CONFIG = dict(DEFAULT)
