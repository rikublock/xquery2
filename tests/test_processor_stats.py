#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from random import randint

import xquery.db.orm as orm
from xquery.db import FusionSQL
from xquery.event.processor_exchange_stats import EventProcessorStageExchange_Stats as stage_cls


def test_processor_stats_timestamp_strict(dbm: FusionSQL):
    start = 1644600000
    size = 3600  # 1 hour

    # timestamp difference ("gap") between next block in seconds
    spacings = [
        0,
        size // 4,
        size // 2,
        size // 8,
        size,
        0,  # ok
        size // 8,
        size // 8,
        -1,  # error
        size // 4,
        size // 4,
        0,
        0,
        0,
        size // 4,
        size // 8,
        0,
        -1,
        0,
        size // 2,
    ]

    objects = []
    timestamp = start
    for i, gap in enumerate(spacings):
        if gap is None:
            continue

        timestamp += gap
        objects.append(
            orm.Block(
                hash=f"0x{i:064x}",
                number=i,
                timestamp=timestamp,
            )
        )

    with dbm.session() as session:
        session.add_all(objects)
        session.commit()

    # 1644600000 0
    # 1644600900 1
    # 1644602700 2
    # 1644603150 3
    # 1644606750 4
    # 1644606750 5
    # 1644607200 6
    # 1644607650 7
    # 1644607649 8
    # 1644608549 9
    # 1644609449 10
    # 1644609449 11
    # 1644609449 12
    # 1644609449 13
    # 1644610349 14
    # 1644610799 15
    # 1644610799 16
    # 1644610798 17
    # 1644610798 18
    # 1644612598 19
    results = [
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        False,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        False,
        True,
        True,
    ]

    # check each block individually
    for i, result in enumerate(results):
        assert stage_cls._check_timestamps(dbm, i, i + 1) is result

    # check some ranges
    assert stage_cls._check_timestamps(dbm, 0, 4) is True
    assert stage_cls._check_timestamps(dbm, 0, 5) is True
    assert stage_cls._check_timestamps(dbm, 2, 7) is True
    assert stage_cls._check_timestamps(dbm, 6, 8) is False
    assert stage_cls._check_timestamps(dbm, 7, 11) is False
    assert stage_cls._check_timestamps(dbm, 9, 15) is True
    assert stage_cls._check_timestamps(dbm, 14, 17) is False
    assert stage_cls._check_timestamps(dbm, 17, 18) is True
    assert stage_cls._check_timestamps(dbm, 13, 19) is False

    # clean up
    with dbm.session() as session:
        session.query(orm.Block).delete()
        session.commit()


def test_processor_stats_timestamp_window(dbm: FusionSQL):
    start = 1644600000
    size = 3600  # hour
    void_percent = 30

    # timestamp difference ("gap") between next block in seconds
    spacings = [
        0,
        size // 8,
        size // 8 - 1,
        size // 8 - 2,
        None,
        size // 8 - 3,
        3 * size,
        size // 8 - 4,
        size // 8,
        None,
        None,
        None,
        size // 8,
        size // 8,
        3 * size,
        None,
        None,
        3 * size,
        size // 2,
        None,
        3 * size,
        size // 2,
        size,
        None,
        size + 1,
        None,
        None,
        size + 2,
        size + 3,
        None,
        None,
        None,
        3 * size,
        size + 4,
        0,
        size + 4,
        size + 4,
        0,
        0,
        size + 2,
        0,
        None,
        0,
        0,
        size + 4,
        size + 4,
        *[None if randint(1, 100) <= void_percent else randint(1, 4 * size) for _ in range(300)],  # random entries
        *[None if randint(1, 100) <= 90 else randint(1, 4 * size) for _ in range(300)],  # random entries
    ]

    objects = []
    timestamp = start
    for i, gap in enumerate(spacings):
        if gap is None:
            continue

        timestamp += gap
        objects.append(
            orm.Block(
                hash=f"0x{i:064x}",
                number=i,
                timestamp=timestamp,
            )
        )

    with dbm.session() as session:
        session.add_all(objects)
        session.commit()

    nums = [
        1,
        2,
        3,
        5,
        10,
    ]

    sizes = [
        3600 // 2,
        3600,
        24 * 3600,
        3600 // 2 - 7,
    ]

    for num in nums:
        for size in sizes:
            tmp_b = - 1
            for j in range(0, len(spacings), num):
                start_block = j
                end_block = j + num - 1
                a, b = stage_cls._find_timestamp_window(dbm, start_block, end_block, size)

                if a is None:
                    continue

                assert tmp_b + 1 == a
                tmp_b = b

    # clean up
    with dbm.session() as session:
        session.query(orm.Block).delete()
        session.commit()
