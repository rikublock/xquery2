#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from logging.config import fileConfig

from sqlalchemy import create_engine
from sqlalchemy import pool

from alembic import (
    context,
    operations,
)

import xquery.db.orm as orm
from xquery.db.misc import build_url
from xquery.config import CONFIG as C

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = orm.Base.metadata


def get_url() -> str:
    return build_url(
        driver=C["DB_DRIVER"],
        host=C["DB_HOST"],
        port=C["DB_PORT"],
        username=C["DB_USERNAME"],
        password=C["DB_PASSWORD"],
        database=C["DB_DATABASE"],
    )


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def _process_revision_directives(context, revision, directives) -> None:
    """
    Modify the ``MigrationScript`` directives to create schemata as required.

    See: https://stackoverflow.com/a/70571077/14834858

    :param context: the ``MigrationContext`` in use
    :param revision: tuple of revision identifiers representing the current revision of the database
    :param directives: list containing a single ``MigrationScript`` directive
    :return:
    """
    assert len(directives) == 1

    script = directives[0]
    for schema in frozenset(i.schema for i in target_metadata.tables.values()):
        script.upgrade_ops.ops.insert(0, operations.ops.ExecuteSQLOp(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        script.downgrade_ops.ops.append(operations.ops.ExecuteSQLOp(f"DROP SCHEMA IF EXISTS {schema} RESTRICT"))


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    url = get_url()
    connectable = create_engine(
        url=url,
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            process_revision_directives=_process_revision_directives,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
