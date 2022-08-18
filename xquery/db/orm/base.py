#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

import datetime

from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
)


Base = declarative_base()


class BaseModel(object):
    id = Column(Integer, primary_key=True, autoincrement=True)


class BaseModelAddDelete(BaseModel):
    date_added = Column(DateTime, default=datetime.datetime.utcnow)
    date_updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    date_deleted = Column(DateTime, default=None)
    deleted = Column(Boolean, default=False)
