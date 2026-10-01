"""Tests for historical read leases.

A historical connection registers a lease recording its read lower
bound when it is opened.  FileStorage.pack takes active leases into
account when computing the retention range.  Leases expire unless
renewed (a crashed process must not block cleanup forever), are
released when the connection is closed, and reading through an
expired lease raises HistoricalLeaseExpired rather than returning a
mix of revisions.
"""

import os
import shutil
import tempfile
import unittest

import transaction
from persistent.mapping import PersistentMapping

import ZODB
from ZODB.FileStorage import FileStorage
from ZODB.POSException import ConnectionStateError
from ZODB.POSException import HistoricalLeaseExpired
from ZODB.POSException import POSKeyError


LEASE_TIMEOUT = 100.0


class FakeClock:

    def __init__(self, now=1000000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class HistoricalLeaseTestCase(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix='zodb-lease-test-')
        self.clock = FakeClock()
        self.storage = FileStorage(
            os.path.join(self.dir, 'data.fs'),
            historical_lease_timeout=LEASE_TIMEOUT,
            historical_lease_clock=self.clock)
        self.db = ZODB.DB(self.storage)
        self.registry = self.storage.historical_lease_registry
        # Three revisions of the same object.
        conn = self.db.open()
        conn.root()['obj'] = PersistentMapping()
        conn.root()['obj']['count'] = 0
        transaction.commit()
        self.tid1 = self.storage.lastTransaction()
        conn.root()['obj']['count'] = 1
        transaction.commit()
        self.tid2 = self.storage.lastTransaction()
        conn.root()['obj']['count'] = 2
        transaction.commit()
        self.oid = conn.root()['obj']._p_oid
        conn.close()

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def open_historical(self):
        return self.db.open(at=self.tid1)

    def historical_count(self, hconn):
        # Force a storage load rather than a cache hit.
        obj = hconn.root()['obj']
        obj._p_deactivate()
        return hconn.root()['obj']['count']

    def test_lease_lifecycle(self):
        # Opening an historical connection registers a lease.
        hconn = self.open_historical()
        self.assertEqual(len(self.registry), 1)
        self.assertEqual(self.historical_count(hconn), 0)

        # Explicit renewal extends the lease.
        self.clock.advance(LEASE_TIMEOUT - 10)
        hconn.renewHistoricalLease()
        self.clock.advance(LEASE_TIMEOUT - 10)
        self.assertEqual(len(self.registry), 1)

        # Closing the connection releases the lease.
        hconn.close()
        self.assertEqual(len(self.registry), 0)

    def test_renew_on_non_historical_connection_fails(self):
        conn = self.db.open()
        try:
            with self.assertRaises(ConnectionStateError):
                conn.renewHistoricalLease()
        finally:
            conn.close()

    def test_pack_retains_then_collects_revisions(self):
        hconn = self.open_historical()
        self.assertEqual(self.historical_count(hconn), 0)

        # First pack: the active lease protects the old revisions.
        self.db.pack()
        data, tid, _ = self.storage.loadBefore(self.oid, self.tid2)
        self.assertEqual(tid, self.tid1)
        self.assertEqual(self.historical_count(hconn), 0)

        # Renewing keeps the lease (and the revisions) alive.
        hconn.renewHistoricalLease()
        self.clock.advance(LEASE_TIMEOUT + 1)
        with self.assertRaises(HistoricalLeaseExpired):
            hconn.renewHistoricalLease()

        # Reading through the expired lease fails loudly instead of
        # returning a mix of revisions.
        with self.assertRaises(HistoricalLeaseExpired):
            self.historical_count(hconn)

        # Second pack: the expired lease no longer protects anything,
        # so the old revisions are finally collected.
        self.db.pack()
        self.assertIsNone(self.storage.loadBefore(self.oid, self.tid2))
        with self.assertRaises(POSKeyError):
            self.storage.loadSerial(self.oid, self.tid1)

        # The current revision is, of course, still there.
        self.assertEqual(self.db.open().root()['obj']['count'], 2)

        # Reading the expired connection still fails with a clear error.
        with self.assertRaises(HistoricalLeaseExpired):
            self.historical_count(hconn)
        hconn.close()
        self.assertEqual(len(self.registry), 0)

    def test_pack_after_close_collects_revisions(self):
        hconn = self.open_historical()
        self.assertEqual(len(self.registry), 1)
        hconn.close()
        self.assertEqual(len(self.registry), 0)

        # No lease and no expiry wait: the pack collects right away.
        self.db.pack()
        self.assertIsNone(self.storage.loadBefore(self.oid, self.tid2))

    def test_second_pack_with_valid_lease_still_retains(self):
        hconn = self.open_historical()
        self.db.pack()
        hconn.renewHistoricalLease()
        self.clock.advance(LEASE_TIMEOUT - 10)
        # A second pack while the lease is valid retains revisions.
        self.db.pack()
        data, tid, _ = self.storage.loadBefore(self.oid, self.tid2)
        self.assertEqual(tid, self.tid1)
        self.assertEqual(self.historical_count(hconn), 0)
        hconn.close()


class HistoricalLeaseRegistryTestCase(unittest.TestCase):

    def test_registry_semantics(self):
        from ZODB.historical_leases import HistoricalLeaseRegistry
        clock = FakeClock()
        registry = HistoricalLeaseRegistry(timeout=10.0, clock=clock)

        lease1 = registry.register(b'\x00' * 7 + b'\x02')
        lease2 = registry.register(b'\x00' * 7 + b'\x01')
        self.assertEqual(len(registry), 2)
        # The retention limit is the lowest protected serial.
        self.assertEqual(registry.pack_retention_limit(),
                         b'\x00' * 7 + b'\x01')

        lease1.release()
        lease1.release()  # idempotent
        self.assertEqual(len(registry), 1)

        # Renewal extends the lease; expiry prunes it.
        clock.advance(9)
        lease2.renew()
        clock.advance(9)
        self.assertEqual(registry.pack_retention_limit(),
                         b'\x00' * 7 + b'\x01')
        clock.advance(11)
        self.assertIsNone(registry.pack_retention_limit())
        self.assertEqual(len(registry), 0)
        self.assertTrue(lease2.expired())
        with self.assertRaises(HistoricalLeaseExpired):
            lease2.renew()


if __name__ == '__main__':
    unittest.main()
