##############################################################################
#
# Copyright (c) Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AND FITNESS FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Tests for Connection.preview()."""

import unittest

import transaction
from persistent import Persistent

import ZODB.blob
import ZODB.tests.util
from ZODB.Connection import ChangePreview
from ZODB.DB import DB
from ZODB.MappingStorage import MappingStorage
from ZODB.utils import oid_repr


class P(Persistent):

    def __init__(self, name=''):
        self.name = name


class PreviewTests(ZODB.tests.util.TestCase):

    def setUp(self):
        super().setUp()
        self.db = DB(MappingStorage())
        self.conn = self.db.open()
        self.root = self.conn.root()

    def tearDown(self):
        self.db.close()
        super().tearDown()

    def commit(self):
        transaction.commit()

    def preview_keys(self, items):
        return {item.preview_id for item in items}

    def store_spy(self):
        # Return (stores, storeBlobs) lists recording what a commit
        # writes to the underlying storage.
        stores = []
        blobs = []
        storage = self.conn._normal_storage
        orig_store = storage.store
        orig_store_blob = getattr(storage, 'storeBlob', None)

        def store(oid, serial, data, version, transaction):
            stores.append((oid, len(data)))
            return orig_store(oid, serial, data, version, transaction)

        storage.store = store
        if orig_store_blob is not None:
            def storeBlob(oid, serial, data, blobfilename, version,
                          transaction):
                blobs.append((oid, len(data)))
                return orig_store_blob(
                    oid, serial, data, blobfilename, version, transaction)

            storage.storeBlob = storeBlob
        self.addCleanup(setattr, storage, 'store', orig_store)
        if orig_store_blob is not None:
            self.addCleanup(setattr, storage, 'storeBlob', orig_store_blob)
        return stores, blobs

    def check_correspondence(self, items, stores):
        # The write set of a real commit corresponds to the last
        # preview: same number of writes (objects previewed without an
        # oid get one assigned at commit time), and every previewed
        # object with a stable oid is written with the previewed size.
        self.assertEqual(len(stores), len(items))
        by_oid = {item.oid: item for item in items if item.oid is not None}
        for oid, size in stores:
            if oid in by_oid:
                self.assertEqual(by_oid[oid].new_size, size)

    def test_empty_preview(self):
        self.assertEqual(self.conn.preview(), [])

    def test_modified_object(self):
        self.root['a'] = P('a')
        self.commit()

        obj = self.root['a']
        old_serial = obj._p_serial
        obj.name = 'changed'

        (item,) = self.conn.preview()
        self.assertIsInstance(item, ChangePreview)
        self.assertEqual(item.oid, obj._p_oid)
        self.assertEqual(item.class_name,
                         'ZODB.tests.test_preview.P')
        self.assertEqual(item.old_serial, old_serial)
        self.assertGreater(item.new_size, 0)
        self.assertFalse(item.blob_changed)

    def test_preview_is_read_only(self):
        self.root['a'] = P('a')
        self.commit()
        obj = self.root['a']
        obj.name = 'changed'
        added = P('added')
        self.conn.add(added)
        implicit = P('implicit')
        added.friend = implicit

        new_oids = []
        storage = self.conn._normal_storage
        orig_new_oid = storage.new_oid

        def new_oid():
            new_oids.append(1)
            return orig_new_oid()

        storage.new_oid = new_oid
        self.addCleanup(setattr, storage, 'new_oid', orig_new_oid)

        items = self.conn.preview()
        self.assertEqual(len(items), 3)

        # No oids were allocated by the storage:
        self.assertEqual(new_oids, [])
        # The implicitly reachable object still has no identity:
        self.assertIsNone(implicit._p_oid)
        self.assertIsNone(implicit._p_jar)
        # Dirty flags are intact:
        self.assertTrue(obj._p_changed)
        self.assertTrue(added._p_changed)
        # The transaction was not committed:
        transaction.abort()
        self.assertEqual(self.root['a'].name, 'a')
        self.assertNotIn('added', self.root)

    def test_new_objects_referencing_each_other(self):
        # Two new objects referencing each other; only one of them is
        # added explicitly, so the other one has no stable oid yet.
        p1 = P('p1')
        p2 = P('p2')
        p1.friend = p2
        p2.friend = p1
        self.conn.add(p1)

        items = self.conn.preview()
        by_id = {item.preview_id: item for item in items}
        self.assertEqual(len(by_id), 2)

        (new,) = [item for item in items if item.oid is None]
        self.assertTrue(new.preview_id.startswith('new-'))
        self.assertIsNone(new.old_serial)
        (known,) = [item for item in items if item.oid is not None]
        self.assertEqual(known.oid, p1._p_oid)
        self.assertIsNone(known.old_serial)

        # The preview didn't assign a permanent identity:
        self.assertIsNone(p2._p_oid)
        self.assertIsNone(p2._p_jar)

        # The commit's write set corresponds to the preview:
        stores, _ = self.store_spy()
        self.commit()
        self.check_correspondence(items, stores)
        # The implicitly reachable object got the remaining oid and
        # was written with the previewed size:
        (new_oid, new_size), = [
            (oid, size) for oid, size in stores if oid != p1._p_oid]
        self.assertEqual(new_oid, p2._p_oid)
        self.assertEqual(new_size, new.new_size)

    def test_modify_then_restore_original_value(self):
        self.root['a'] = P('a')
        self.commit()
        obj = self.root['a']

        obj.name = 'something else'
        obj.name = 'a'  # back to the original value

        # The object is still registered as changed, so it shows up in
        # the preview and is written on commit:
        (item,) = self.conn.preview()
        self.assertEqual(item.oid, obj._p_oid)

        stores, _ = self.store_spy()
        self.commit()
        self.check_correspondence([item], stores)

    def test_savepoint_rollback(self):
        self.root['a'] = P('a')
        self.root['b'] = P('b')
        self.commit()

        self.root['a'].name = 'a1'
        sp = transaction.savepoint()

        # The checkpointed change is part of the preview:
        items = self.conn.preview()
        self.assertEqual(self.preview_keys(items),
                         {oid_repr(self.root['a']._p_oid)})

        self.root['b'].name = 'b1'
        items = self.conn.preview()
        self.assertEqual(len(items), 2)

        # After rolling back the savepoint, the preview reflects the
        # current state: the change to 'b' is gone.
        sp.rollback()
        items = self.conn.preview()
        self.assertEqual(len(items), 1)
        (item,) = items
        self.assertEqual(item.oid, self.root['a']._p_oid)
        self.assertEqual(self.root['b'].name, 'b')

        # The commit's write set corresponds to the last preview:
        stores, _ = self.store_spy()
        self.commit()
        self.check_correspondence(items, stores)
        self.assertEqual(self.root['a'].name, 'a1')

    def test_savepoint_then_modify_again(self):
        self.root['a'] = P('a')
        self.commit()
        self.root['a'].name = 'a1'
        transaction.savepoint()
        self.root['a'].name = 'a2'

        # The object is reported once, with its current state:
        (item,) = self.conn.preview()
        self.assertEqual(item.oid, self.root['a']._p_oid)

        stores, _ = self.store_spy()
        self.commit()
        self.check_correspondence([item], stores)
        self.assertEqual(self.root['a'].name, 'a2')

    def test_closed_connection(self):
        self.conn.close()
        from ZODB.POSException import ConnectionStateError
        with self.assertRaises(ConnectionStateError):
            self.conn.preview()


class BlobPreviewTests(ZODB.tests.util.TestCase):

    def setUp(self):
        super().setUp()
        self.db = DB(ZODB.blob.BlobStorage('blobs', MappingStorage()))
        self.conn = self.db.open()
        self.root = self.conn.root()

    def tearDown(self):
        self.db.close()
        super().tearDown()

    def store_spy(self):
        stores = []
        blobs = []
        storage = self.conn._normal_storage
        orig_store = storage.store
        orig_store_blob = storage.storeBlob

        def store(oid, serial, data, version, transaction):
            stores.append((oid, len(data)))
            return orig_store(oid, serial, data, version, transaction)

        def storeBlob(oid, serial, data, blobfilename, version,
                      transaction):
            blobs.append((oid, len(data)))
            return orig_store_blob(
                oid, serial, data, blobfilename, version, transaction)

        storage.store = store
        storage.storeBlob = storeBlob
        self.addCleanup(setattr, storage, 'store', orig_store)
        self.addCleanup(setattr, storage, 'storeBlob', orig_store_blob)
        return stores, blobs

    def test_blob_change(self):
        blob = ZODB.blob.Blob()
        self.root['blob'] = blob
        with blob.open('w') as f:
            f.write(b'hello')

        items = self.conn.preview()
        # The modified root is previewed as well:
        self.assertEqual(len(items), 2)
        (item,) = [i for i in items if i.oid == blob._p_oid]
        self.assertEqual(item.oid, blob._p_oid)
        self.assertTrue(item.blob_changed)
        self.assertIsNone(item.old_serial)

        # The preview didn't consume the uncommitted blob data:
        self.assertIsNotNone(blob._p_blob_uncommitted)

        stores, blobs = self.store_spy()
        transaction.commit()
        self.assertEqual(len(blobs), 1)
        self.assertEqual(blobs[0][0], blob._p_oid)
        self.assertEqual(blobs[0][1], item.new_size)
        self.assertEqual(len(stores), 1)  # the root

        # Modify the blob data in a new transaction:
        with blob.open('w') as f:
            f.write(b'hello, world')
        old_serial = blob._p_serial
        (item,) = self.conn.preview()
        self.assertTrue(item.blob_changed)
        self.assertEqual(item.old_serial, old_serial)

        stores, blobs = self.store_spy()
        transaction.commit()
        self.assertEqual(len(blobs), 1)
        self.assertEqual(stores, [])
        with blob.open() as f:
            self.assertEqual(f.read(), b'hello, world')

    def test_unchanged_blob_is_not_written(self):
        blob = ZODB.blob.Blob()
        self.root['blob'] = blob
        with blob.open('w') as f:
            f.write(b'hello')
        transaction.commit()

        # Registering the blob as changed without changing its data
        # doesn't produce a write, so the preview is empty:
        blob._p_changed = True
        self.assertEqual(self.conn.preview(), [])
        stores, blobs = self.store_spy()
        transaction.commit()
        self.assertEqual(stores, [])
        self.assertEqual(blobs, [])

    def test_blob_change_with_savepoint(self):
        blob = ZODB.blob.Blob()
        self.root['blob'] = blob
        with blob.open('w') as f:
            f.write(b'one')
        sp = transaction.savepoint()

        items = {i.oid: i for i in self.conn.preview()}
        item = items[blob._p_oid]
        self.assertEqual(item.oid, blob._p_oid)
        self.assertTrue(item.blob_changed)

        with blob.open('w') as f:
            f.write(b'two')
        sp.rollback()

        # After the rollback, the preview reflects the first state:
        items = {i.oid: i for i in self.conn.preview()}
        item = items[blob._p_oid]
        self.assertEqual(item.oid, blob._p_oid)
        self.assertTrue(item.blob_changed)

        stores, blobs = self.store_spy()
        transaction.commit()
        self.assertEqual(len(blobs), 1)
        self.assertEqual(blobs[0][1], item.new_size)
        with blob.open() as f:
            self.assertEqual(f.read(), b'one')


def test_suite():
    return unittest.TestSuite((
        unittest.defaultTestLoader.loadTestsFromTestCase(PreviewTests),
        unittest.defaultTestLoader.loadTestsFromTestCase(BlobPreviewTests),
    ))


if __name__ == '__main__':
    unittest.main()
