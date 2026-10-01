##############################################################################
#
# Copyright (c) 2026 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Tests for transaction-level read-only mode (ZODB.readonly)."""

import shutil
import tempfile
import unittest

import transaction
from persistent import Persistent

import ZODB
import ZODB.blob
from ZODB.MappingStorage import MappingStorage
from ZODB.POSException import ReadOnlyError
from ZODB.readonly import is_read_only
from ZODB.readonly import read_only
from ZODB.readonly import set_read_only


class P(Persistent):
    pass


class ReadOnlyTransactionTests(unittest.TestCase):

    def setUp(self):
        self.db = ZODB.DB(MappingStorage())
        self.tm = transaction.TransactionManager()
        self.conn = self.db.open(self.tm)
        self.root = self.conn.root()

    def tearDown(self):
        self.db.close()

    def test_modification_rejected_in_all_connections(self):
        # Two connections sharing one transaction manager are both
        # covered by the transaction's read-only mode.
        conn2 = self.db.open(self.tm)
        root2 = conn2.root()
        self.root['p'] = P()
        self.tm.commit()

        set_read_only(self.tm.get())
        with self.assertRaises(ReadOnlyError):
            self.root['x'] = 1
        with self.assertRaises(ReadOnlyError):
            root2['y'] = 1
        with self.assertRaises(ReadOnlyError):
            self.root['p'].attr = 1

        # The failed attempts did not register anything.
        self.assertEqual(self.conn._registered_objects, [])
        self.assertEqual(conn2._registered_objects, [])
        self.tm.abort()

    def test_add_rejected(self):
        set_read_only(self.tm.get())
        obj = P()
        with self.assertRaises(ReadOnlyError):
            self.conn.add(obj)
        # The object was rejected before an oid was assigned.
        self.assertIsNone(obj._p_oid)
        self.assertIsNone(obj._p_jar)
        self.tm.abort()

    def test_entering_with_dirty_objects_fails(self):
        self.root['x'] = 1
        with self.assertRaises(ReadOnlyError):
            set_read_only(self.tm.get())
        self.assertFalse(is_read_only(self.tm.get()))
        self.tm.abort()
        # A clean transaction can enter read-only mode.
        set_read_only(self.tm.get())
        self.assertTrue(is_read_only(self.tm.get()))
        self.tm.abort()

    def test_savepoint_keeps_mode_and_leaves_no_restriction(self):
        set_read_only(self.tm.get())
        self.tm.savepoint()  # savepoints are allowed
        with self.assertRaises(ReadOnlyError):
            self.root['x'] = 1
        self.tm.abort()
        # The next transaction is not restricted.
        self.assertFalse(is_read_only(self.tm.get()))
        self.root['x'] = 1
        self.tm.commit()
        self.assertEqual(self.root['x'], 1)

    def test_abort_clears_mode(self):
        set_read_only(self.tm.get())
        self.tm.abort()
        self.root['x'] = 1
        self.tm.commit()
        self.assertEqual(self.root['x'], 1)

    def test_context_manager_restores_previous_state(self):
        with read_only(self.tm):
            self.assertTrue(is_read_only(self.tm.get()))
            with self.assertRaises(ReadOnlyError):
                self.root['x'] = 1
        self.assertFalse(is_read_only(self.tm.get()))
        self.root['x'] = 1
        self.tm.commit()

    def test_other_transactions_can_still_write(self):
        set_read_only(self.tm.get())
        tm2 = transaction.TransactionManager()
        conn2 = self.db.open(tm2)
        conn2.root()['x'] = 42
        tm2.commit()
        self.tm.abort()
        self.tm.begin()
        self.assertEqual(self.root['x'], 42)

    def test_ghost_activation_is_not_a_write(self):
        self.root['p'] = P()
        self.root['p'].attr = 1
        self.tm.commit()
        p = self.root['p']
        p._p_deactivate()  # turn the object back into a ghost
        self.assertIsNone(p._p_changed)

        set_read_only(self.tm.get())
        # Activating a ghost loads state but does not register a change.
        self.assertEqual(p.attr, 1)
        self.assertFalse(p._p_changed)
        self.assertEqual(self.conn._registered_objects, [])
        self.assertEqual(self.tm.get()._resources, [])
        self.tm.abort()


class ReadOnlyBlobTests(unittest.TestCase):

    def setUp(self):
        self.blob_dir = tempfile.mkdtemp()
        storage = ZODB.blob.BlobStorage(self.blob_dir, MappingStorage())
        self.db = ZODB.DB(storage)
        self.tm = transaction.TransactionManager()
        self.conn = self.db.open(self.tm)
        self.root = self.conn.root()
        self.root['blob'] = ZODB.blob.Blob()
        with self.root['blob'].open('w') as f:
            f.write(b'committed')
        self.tm.commit()

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.blob_dir, ignore_errors=True)

    def test_blob_write_open_rejected(self):
        set_read_only(self.tm.get())
        blob = self.root['blob']
        for mode in 'w', 'a', 'r+':
            with self.assertRaises(ReadOnlyError):
                blob.open(mode)
        # No uncommitted file was created as a side effect.
        self.assertIsNone(blob._p_blob_uncommitted)
        # Reading is still possible.
        with blob.open('r') as f:
            self.assertEqual(f.read(), b'committed')
        self.tm.abort()

    def test_blob_write_open_rejected_in_second_connection(self):
        conn2 = self.db.open(self.tm)
        set_read_only(self.tm.get())
        with self.assertRaises(ReadOnlyError):
            conn2.root()['blob'].open('w')
        self.tm.abort()

    def test_blob_consume_file_rejected(self):
        set_read_only(self.tm.get())
        with self.assertRaises(ReadOnlyError):
            self.root['blob'].consumeFile(__file__)
        self.tm.abort()


def test_suite():
    return unittest.defaultTestLoader.loadTestsFromName(__name__)


if __name__ == '__main__':
    unittest.main()
