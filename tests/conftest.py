#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import logging
import pytest
import shutil
import tempfile

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.environment import EnvironmentContext
from alembic import autogenerate

from sqlalchemy import event

from web3 import Web3
from web3.middleware import geth_poa_middleware

import xquery.db
import xquery.db.orm as orm
import xquery.cache
import xquery.provider
from xquery.config import CONFIG as C
from xquery.util import init_decimal_context

log = logging.getLogger(__name__)


def pytest_configure(config):
    logging.basicConfig(level=C["LOG_LEVEL"], format=C["LOG_FORMAT"], datefmt=C["LOG_DATE_FORMAT"])
    init_decimal_context()


@pytest.fixture(scope="session")
def c() -> xquery.cache.Cache:
    return xquery.cache.Cache_Redis(
        host=C["REDIS_HOST"],
        port=C["REDIS_PORT"],
        password=C["REDIS_PASSWORD"],
        db=C["REDIS_DATABASE"],
    )


@pytest.fixture(scope="session")
def w3() -> Web3:
    w = Web3(Web3.HTTPProvider(endpoint_uri=C["API_URL"]))
    w.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w


@pytest.fixture(scope="session")
def db() -> xquery.db.FusionSQL:
    return xquery.db.FusionSQL(
        conn=xquery.db.build_url(
            driver=C["DB_DRIVER"],
            host=C["DB_HOST"],
            port=C["DB_PORT"],
            username=C["DB_USERNAME"],
            password=C["DB_PASSWORD"],
            database=C["DB_DATABASE"],
        ),
        verbose=C["DB_DEBUG"],
    )


@pytest.fixture(scope="session")
def dbm() -> xquery.db.FusionSQL:
    """
    In-memory SQlite database for testing
    """
    db = xquery.db.FusionSQL(
        conn="sqlite:///:memory:",
        verbose=C["DB_DEBUG"],
    )

    # Note: SQlite doesn't have the concept of schemata as found in postgres.
    #       However, we can work around it by attaching another external database.
    @event.listens_for(db._engine, "first_connect")
    def schema_attach(dbapi_connection, connection_record) -> None:
        dbapi_connection.execute(f"ATTACH DATABASE ':memory:' AS {orm.Base.metadata.schema}")

    # initialize the database (create tables)
    with tempfile.TemporaryDirectory() as tmpdir:
        Path(tmpdir, "versions").mkdir(parents=True, exist_ok=True)

        # copy a template file
        shutil.copy(
            src=Path("alembic/script.py.mako"),
            dst=Path(tmpdir, "script.py.mako"),
        )

        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", tmpdir)
        alembic_script = ScriptDirectory.from_config(alembic_cfg)

        context = EnvironmentContext(alembic_cfg, alembic_script)
        revision_context = autogenerate.RevisionContext(
            config=alembic_cfg,
            script_directory=alembic_script,
            command_args={
                "message": "create debug schema",
                "autogenerate": True,
                "sql": False,
                "head": "head",
                "splice": False,
                "branch_label": None,
                "version_path": None,
                "rev_id": None,
                "depends_on": None,
            },
        )

        # generate and apply a revision
        def do_upgrade(rev, context):
            revision_context.run_autogenerate(rev, context)
            list(revision_context.generate_scripts())
            return alembic_script._upgrade_revs("head", rev)

        with db._engine.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=orm.Base.metadata,
                fn=do_upgrade,
            )

            with context.begin_transaction():
                context.run_migrations()

    return db
