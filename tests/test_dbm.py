#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from sqlalchemy import select

import xquery.db
import xquery.db.orm as orm


def test_dbm(dbm: xquery.db.FusionSQL) -> None:
    with dbm.session() as session:
        state = orm.State(
            name="default",
            block_number=55,
            block_hash=None,
        )
        session.add(state)
        session.commit()

    with dbm.session() as session:
        state = session.execute(
            select(orm.State)
                .filter(orm.State.name == "default")
        ).scalar()

        assert state.block_number == 55
