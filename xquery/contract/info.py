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


rc20_bytes = Info(
    address=None,
    abi_file="xquery/contract/RC20_bytes.json",
    from_block=None,
)

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

wavax = Info(
    address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
    abi_file="xquery/contract/WAVAX.json",
    from_block=820,
)

psys_pair = Info(
    address=None,
    abi_file="xquery/contract/psys_Pair.json",
    from_block=None,
)

psys_rc20 = Info(
    address=None,
    abi_file="xquery/contract/psys_RC20.json",
    from_block=None,
)

psys_factory = Info(
    address="0x7Bbbb6abaD521dE677aBe089C85b29e3b2021496",
    abi_file="xquery/contract/psys_Factory.json",
    from_block=38185,
)

psys_router = Info(
    address="0x017dAd2578372CAEE5c6CddfE35eEDB3728544C4",
    abi_file="xquery/contract/psys_Router.json",
    from_block=38190,
)

wsys = Info(
    address="0xd3e822f3ef011Ca5f17D82C956D952D8d7C3A1BB",
    abi_file="xquery/contract/WSYS.json",
    from_block=1523,
)

uni_v2_pair = Info(
    address=None,
    abi_file="xquery/contract/uni_v2_Pair.json",
    from_block=None,
)

uni_v2_rc20 = Info(
    address=None,
    abi_file="xquery/contract/uni_v2_RC20.json",
    from_block=None,
)

uni_v2_factory = Info(
    address="0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
    abi_file="xquery/contract/uni_v2_Factory.json",
    from_block=10000835,
)

uni_v2_router = Info(
    address="0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
    abi_file="xquery/contract/uni_v2_Router.json",
    from_block=10207858,
)

weth = Info(
    address="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    abi_file="xquery/contract/WETH9.json",
    from_block=4719568,
)
