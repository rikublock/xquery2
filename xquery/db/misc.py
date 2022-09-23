#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.


def build_url(driver: str, host: str, port: int, username: str, password: str, database: str) -> str:
    """
    Format the database connection string (database dialect and connection arguments) for
    the usage in sqlalchemy.

    See: https://docs.sqlalchemy.org/en/14/core/engines.html

    Examples:
        - 'dialect+driver://username:password@host:port/database'
        - 'postgresql://scott:tiger@localhost:5432/mydatabase'

    :return:
    """
    return f"{driver}://{username}:{password}@{host}:{port}/{database}"
