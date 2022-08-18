#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import os


DEFAULT = {
    "LOG_FORMAT": "%(asctime)s.%(msecs)04d %(levelname)-7s [%(threadName)-10s, %(process)5d] %(name)s: %(message)s",
    "LOG_DATE_FORMAT": "%H:%M:%S",

    # Database settings
    "DB_DRIVER": "postgresql",
    "DB_HOST": os.getenv("DB_HOST", "localhost"),
    "DB_PORT": os.getenv("DB_PORT", 5432),
    "DB_USERNAME": os.getenv("DB_USERNAME", "root"),
    "DB_PASSWORD": os.getenv("DB_PASSWORD", "password"),
    "DB_DATABASE": os.getenv("DB_DATABASE", "debug"),

    "DB_DEBUG": os.getenv("DEBUG", False),

    # Redis cache settings
    "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
    "REDIS_PORT": os.getenv("REDIS_PORT", 6379),
    "REDIS_PASSWORD": os.getenv("REDIS_PASSWORD", "password"),
    "REDIS_DATABASE": os.getenv("REDIS_DATABASE", 0),

    # web3 provider RPC url
    "API_URL": {
        "ETH": None,
        "AVAX": "https://api.avax.network/ext/bc/C/rpc",
        "SYS": "https://rpc.syscoin.org/",
    },

}


CONFIG = dict(DEFAULT)
