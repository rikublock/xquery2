#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import Optional

import json


class Info(object):
    """
    Temporary hard code contract information for the sake of simplicity.
    This will eventually be replaced/complemented with a more dynamic config file.
    """

    def __init__(self, address: Optional[str], abi_file: str, from_block: Optional[int]) -> None:
        """
        Contract information

        :param address: contract address
        :param abi_file: json file containing list of event/function interfaces
        :param from_block: block height of contract deployment (used to filter events)
        """
        self.address = address

        with open(abi_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.abi = data["abi"]

        self.from_block = from_block

    def __repr__(self):
        return f"Info <address={self.address} from_block={self.from_block}>"


png_pair = Info(
    address=None,
    abi_file="xquery/contract/png_Pair.json",
    from_block=None,
)

png_rc20 = Info(
    address=None,
    abi_file="xquery/contract/png_RC20.json",
    from_block=None,
)

png_factory = Info(
    address="0xefa94DE7a4656D787667C749f7E1223D71E9FD88",
    abi_file="xquery/contract/png_Factory.json",
    from_block=56877,
)

png_router = Info(
    address="0xE54Ca86531e17Ef3616d22Ca28b0D458b6C89106",
    abi_file="xquery/contract/png_Router.json",
    from_block=56879,
)

png_wavax = Info(
    address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
    abi_file="xquery/contract/png_WAVAX.json",
    from_block=820,
)
