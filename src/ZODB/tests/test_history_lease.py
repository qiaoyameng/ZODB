##############################################################################
#
# Copyright (c) 2026 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import unittest
from unittest import mock

import time
import transaction
from persistent.TimeStamp import TimeStamp

from ZODB import DB
from ZODB.FileStorage.FileStorage import FileStorage
from ZODB.FileStorage.historylease import HistoryLeaseManager
from ZODB.POSException import HistoryLeaseExpiredError
from ZODB.POSException import HistoryLeaseReleasedError
from ZODB.utils import z64

from . import util


def tid(second, fraction=0):
    return TimeStamp(
        *time.gmtime(second)[:5] + (second % 60 + fraction,)).raw()


class HistoryLeaseManagerTests(unittest.TestCase):

    def test_expiration_release_and_pack_bound(self):
        clock = [100.0]
        manager = HistoryLeaseManager(lambda: clock[0])
        before = tid(100)

        lease = manager.acquire(before, 10)
        lower_bound = lease.lower_bound
        self.assertEqual(manager.begin_pack(tid(200)), lower_bound)
        manager.cancel_pack()
        manager.release(lease)
        self.assertEqual(manager.begin_pack(tid(200)), tid(200))
        manager.cancel_pack()

        lease = manager.acquire(before, 10)
        clock[0] = 110.0
        with self.assertRaises(HistoryLeaseExpiredError):
            manager.renew(lease, 10)

    def test_renewal_extends_pack_bound(self):
        clock = [100.0]
        manager = HistoryLeaseManager(lambda: clock[0])
        lease = manager.acquire(tid(100), 10)

        clock[0] = 109.0
        manager.renew(lease, 10)
        self.assertEqual(manager.begin_pack(tid(200)), lease.lower_bound)
        manager.cancel_pack()

        clock[0] = 118.0
        manager.check(lease)
        clock[0] = 120.0
        manager.finish_pack(lease.lower_bound)
        with self.assertRaises(HistoryLeaseExpiredError):
            manager.check(lease)

    def test_closed_lease_can_not_be_renewed(self):
        clock = [100.0]
        manager = HistoryLeaseManager(lambda: clock[0])
        lease = manager.acquire(tid(100), 10)
        manager.release(lease)
        with self.assertRaises(HistoryLeaseReleasedError):
            manager.renew(lease, 10)


class FileStorageHistoryLeaseTests(util.TestCase):

    def test_close_releases_lease(self):
        clock = [1000.0]
        with mock.patch('time.time', side_effect=lambda: clock[0]):
            storage = FileStorage('close.fs')
            db = DB(storage)
            conn = db.open()
            conn.root()['value'] = 1
            transaction.commit()
            first_tid = db.lastTransaction()

            clock[0] = 1001.0
            conn.root()['value'] = 2
            transaction.commit()

            historical = db.open(
                at=first_tid, history_lease_timeout=10)
            historical.close()
            self.assertEqual(storage._history_leases._leases, {})

            clock[0] = 1002.0
            db.pack(1002.0)
            self.assertIsNone(storage.loadBefore(z64, historical.before))

            conn.close()
            db.close()

    def test_pack_respects_and_eventually_reclaims_lease(self):
        clock = [1000.0]
        with mock.patch('time.time', side_effect=lambda: clock[0]):
            storage = FileStorage('history.fs')
            db = DB(storage)
            conn = db.open()
            conn.root()['value'] = 1
            transaction.commit()
            first_tid = db.lastTransaction()

            clock[0] = 1001.0
            conn.root()['value'] = 2
            transaction.commit()
            second_tid = db.lastTransaction()

            clock[0] = 1002.0
            historical = db.open(
                at=first_tid, history_lease_timeout=10)
            self.assertEqual(historical.get(z64)['value'], 1)
            before = historical.before

            clock[0] = 1003.0
            db.pack(1003.0)
            self.assertEqual(
                storage.loadBefore(z64, before)[1], first_tid)

            clock[0] = 1008.0
            historical._cache.invalidate(z64)
            self.assertEqual(historical.get(z64)['value'], 1)
            historical.renewHistoryLease(10)

            clock[0] = 1019.0
            historical._cache.invalidate(z64)
            with self.assertRaises(HistoryLeaseExpiredError):
                historical.get(z64)

            clock[0] = 1020.0
            db.pack(1020.0)
            self.assertIsNone(storage.loadBefore(z64, before))
            current = storage.load(z64)
            self.assertEqual(current[1], second_tid)

            historical.close()
            conn.close()
            db.close()


def test_suite():
    return unittest.defaultTestLoader.loadTestsFromModule(__import__(__name__))
