#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022-2022 Riku Block
# All rights reserved.
#
# This file is part of XQuery2.

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from . import orm

# TODO Possibly adopt https://www.gorgias.com/blog/prevent-idle-in-transaction-engineering


class FusionPGSQL(object):

    def __init__(self, conn, verbose=False):
        """
        Manages sqlalchemy engine and session factory.

        Note: This should only be instantiated once per process.

        :param conn: postgres connection string
        :param verbose: enable sqlalchemy verbosity
        """
        assert isinstance(conn, str)
        assert isinstance(verbose, bool)

        self._engine = create_engine(conn, echo=verbose, future=True)

        self._session = sessionmaker(
            bind=self._engine,
            autoflush=True,
            autocommit=False,
            expire_on_commit=True,
            future=True,
        )

    @property
    def session(self):
        """
        Factory session object

        The returned object should be used in a context.

        Usage:

        # closes the session
        with FusionPGSQL.session() as session:
            session.add(some_object)
            session.add(some_other_object)
            session.commit()

        # auto commits the transaction, closes the session
        with FusionPGSQL.session.begin() as session:
            session.add(some_object)
            session.add(some_other_object)

        """
        return self._session

    @property
    def orm(self):
        """
        Convenience reference to the orm module
        """
        return orm
