#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from xquery.util.misc import compute_xhash


def test_xhash():
    data = {
        "address": "0xd7538cABBf8605BdE1f4901B47B8D42c61DE0367",
        "blockHash": "0x2544fe8d16e56008130750149d13552b1e85eab65c638bbba951b31bb506fa53",
        "logIndex": 14,
        "transactionHash": "0x250f403ba38cc46bef098b8cbcd85e2af3b57db71e8603112419a66f006a21a2",
    }

    assert compute_xhash(data) == "0x7ceefa1adb70dd9145753925d66d980e212a2f40de6a46ab40986363049d4dff"  # type: ignore
