#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from typing import (
    List,
    Tuple,
    Type,
    Union,
)

import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
)
from sqlalchemy.orm import declarative_base

from xquery.config import CONFIG as C

Base = declarative_base(
    metadata=MetaData(
        schema=C["DB_SCHEMA"],
    ),
)

TDBObjs = Union[List[Base], List[Tuple[Type[Base], List[dict]]]]


class BaseModel(object):
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)


class BaseModelAddDelete(BaseModel):
    date_added = Column(DateTime, default=datetime.datetime.utcnow)
    date_updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    date_deleted = Column(DateTime, default=None)
    deleted = Column(Boolean, default=False)
