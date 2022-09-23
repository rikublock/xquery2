#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    String,
)

from .base import (
    Base,
    BaseModel,
)


class IndexerState(BaseModel, Base):
    """
    Store indexer state information

    Relationships
    """
    __tablename__ = "indexer_state"

    name = Column(String(length=128), unique=True, nullable=False)
    block_number = Column(Integer, nullable=False)
    block_hash = Column(String(length=66))
    discarded = Column(Boolean)


class ProcessorState(BaseModel, Base):
    """
    Store processor state information

    Relationships
    """
    __tablename__ = "processor_state"

    name = Column(String(length=128), unique=True, nullable=False)
    stage = Column(Integer, unique=True, nullable=False)
    block_number = Column(Integer, nullable=False)
    block_hash = Column(String(length=66))
