"""Tests for transaction-level read-only mode (ZODB.readonly)."""

import os
import shutil
import tempfile
import unittest

import transaction
from persistent import Persistent

import ZODB
from ZODB.blob import Blob
from ZODB.POSException import ReadOnlyError
from ZODB.readonly import is_read_only
from ZODB.readonly import set_read_only


class P(Persistent):
    pass


class ReadOnlyTransactionTests(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='zodb-readonly-')
        self.tm = transaction.TransactionManager()
        self.db = ZODB.DB(
            os.path.join(self.tmpdir, 'data.fs'),
            blob_dir=os.path.join(self.tmpdir, 'blobs'),
        )
        # Populate some initial data in a normal transaction.
        conn = self.db.open(self.tm)
        root = conn.root()
        root['obj'] = P()
        root['obj'].value = 1
        root['holder'] = P()
        root['holder'].blob = Blob(b'initial')
        self.tm.commit()
        conn.close()

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_modification_add_and_blob_write_rejected(self):
        conn1 = self.db.open(self.tm)
        conn2 = self.db.open(self.tm)
        txn = self.tm.get()
        set_read_only(txn)
        self.assertTrue(is_read_only(txn))

        obj = conn1.root()['obj']
        # Registering a modification is rejected up front.
        with self.assertRaises(ReadOnlyError):
            obj.value = 2
        # The failed registration must not have joined the transaction
        # or left the object marked as changed.
        self.assertFalse(obj._p_changed)
        self.assertEqual(conn1._registered_objects, [])

        # Adding a new object through the other connection of the same
        # transaction is rejected as well.
        new_obj = P()
        with self.assertRaises(ReadOnlyError):
            conn2.add(new_obj)
        self.assertIsNone(getattr(new_obj, '_p_jar', None))
        self.assertIsNone(getattr(new_obj, '_p_oid', None))

        # So is modifying a persistent object through the second
        # connection.
        with self.assertRaises(ReadOnlyError):
            conn2.root()['other'] = P()

        # Opening a blob for writing is rejected before any file is
        # created, in all write modes.
        blob = conn1.root()['holder'].blob
        for mode in 'w', 'a', 'r+':
            with self.assertRaises(ReadOnlyError):
                blob.open(mode)
        self.assertIsNone(blob._p_blob_uncommitted)

        # Reading still works.
        self.assertEqual(obj.value, 1)
        with blob.open('r') as fp:
            self.assertEqual(fp.read(), b'initial')

        self.tm.abort()
        conn1.close()
        conn2.close()

    def test_ghost_activation_is_not_a_write(self):
        conn = self.db.open(self.tm)
        obj = conn.root()['obj']
        obj._p_deactivate()
        self.assertEqual(obj._p_status, 'ghost')

        set_read_only(self.tm.get())

        # Activating a ghost only reads state; it must not be
        # treated as a modification.
        self.assertEqual(obj.value, 1)
        self.assertFalse(obj._p_changed)
        self.assertEqual(conn._registered_objects, [])

        self.tm.abort()
        conn.close()

    def test_read_only_rejects_transactions_with_changes(self):
        conn = self.db.open(self.tm)
        conn.root()['obj'].value = 2
        # Entering read-only mode with dirty objects has a clear
        # result: it is refused.
        with self.assertRaises(ReadOnlyError):
            set_read_only(self.tm.get())
        self.assertFalse(is_read_only(self.tm.get()))
        self.tm.abort()
        conn.close()

    def test_savepoint_abort_and_next_transaction_unrestricted(self):
        conn = self.db.open(self.tm)
        txn = self.tm.get()
        set_read_only(txn)

        # Savepoints work in read-only mode and keep the restriction.
        sp = txn.savepoint()
        self.assertTrue(is_read_only(txn))
        sp.rollback()
        self.assertTrue(is_read_only(txn))
        with self.assertRaises(ReadOnlyError):
            conn.root()['obj'].value = 2

        # Aborting ends the restriction; the next transaction is a
        # normal one.
        self.tm.abort()
        txn = self.tm.get()
        self.assertFalse(is_read_only(txn))
        conn.root()['obj'].value = 3
        self.tm.commit()
        self.assertEqual(conn.root()['obj'].value, 3)
        conn.close()

    def test_other_transactions_can_still_write(self):
        conn = self.db.open(self.tm)
        set_read_only(self.tm.get())

        # A connection using a different transaction manager is not
        # affected; the database itself is not read-only.
        other_tm = transaction.TransactionManager()
        other = self.db.open(transaction_manager=other_tm)
        other.root()['obj'].value = 42
        other_tm.commit()
        other.close()

        # The read-only transaction still rejects writes.
        with self.assertRaises(ReadOnlyError):
            conn.root()['obj'].value = 2
        self.tm.abort()

        conn.sync()
        self.assertEqual(conn.root()['obj'].value, 42)
        conn.close()


if __name__ == '__main__':
    unittest.main()
