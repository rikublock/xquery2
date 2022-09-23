#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    List,
    Optional,
    cast,
)

import json

from pathlib import Path

from web3.datastructures import AttributeDict
from web3.types import LogReceipt

from web3._utils.method_formatters import log_entry_formatter


def load_logs(file: Path, txids: Optional[list] = None) -> List[LogReceipt]:
    with open(file, "r") as f:
        data = json.load(f)

    entries = []
    for entry in data["logs"]:
        entry = log_entry_formatter(entry)
        entry = AttributeDict.recursive(entry)
        entry = cast(LogReceipt, entry)
        entries.append(entry)

    # only return logs from certain transactions
    if txids is not None:
        entries = [entry for entry in entries if entry.transactionHash.hex() in txids]

    return entries
