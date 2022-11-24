#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from sqlalchemy import (
    Column,
    Integer,
    String,
)

from .base import (
    Base,
    BaseModel,
)


class State(BaseModel, Base):
    """
    Basic state information
    """
    __tablename__ = "state"

    name = Column(String(length=128), unique=True, nullable=False)
    block_number = Column(Integer)
    block_hash = Column(String(length=66))
    finalized = Column(Integer)
