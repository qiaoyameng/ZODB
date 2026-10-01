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
"""Tests for the read-only change preview API."""

import os
import unittest

import transaction
from persistent.mapping import PersistentMapping

import ZODB
import ZODB.tests.util
from ZODB import blob as blob_module
from ZODB.blob import Blob
from ZODB.blob import BlobStorage
from ZODB.changepreview import ObjectChange
from ZODB.changepreview import PreviewOID
from ZODB.MappingStorage import MappingStorage
from ZODB.utils import z64


class RecordingMappingStorage(MappingStorage):
    """Mapping storage remembering every record handed to store()."""

    def init(self):
        self.records = []

    def __init__(self, *args, **kw):
        MappingStorage.__init__(self, *args, **kw)
        self.records = []

    def store(self, oid, serial, data, version, transaction):
        via_blob = bool(getattr(transaction, "_preview_via_blob", False))
        self.records.append((oid, serial, data, via_blob))
        return MappingStorage.store(
            self, oid, serial, data, version, transaction)


def install_blob_recording():
    original = blob_module.BlobStorageMixin.storeBlob

    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        transaction._preview_via_blob = True
        try:
            return original(
                self, oid, oldserial, data, blobfilename,
                version, transaction)
        finally:
            del transaction._preview_via_blob

    blob_module.BlobStorageMixin.storeBlob = storeBlob
    return original


class ChangePreviewTests(ZODB.tests.util.TestCase):

    def setUp(self):
        ZODB.tests.util.TestCase.setUp(self)
        self._original_storeBlob = install_blob_recording()
        self.storage = RecordingMappingStorage()
        self.blob_dir = os.path.join(os.getcwd(), 'blobs')
        self.db = ZODB.DB(BlobStorage(self.blob_dir, self.storage))
        self.conn = self.db.open()
        self.root = self.conn.root()
        self.root['v'] = 0
        transaction.commit()
        self.storage.records = []

    def tearDown(self):
        transaction.abort()
        self.db.close()
        blob_module.BlobStorageMixin.storeBlob = self._original_storeBlob
        ZODB.tests.util.TestCase.tearDown(self)

    def _preview_map(self):
        changes = self.conn.change_preview()
        for change in changes:
            self.assertIsInstance(change, ObjectChange)
        return {change.oid: change for change in changes}

    def _commit_map(self):
        transaction.commit()
        result = {}
        for oid, serial, data, via_blob in self.storage.records:
            result[oid] = (serial, len(data), via_blob)
        return result

    def assertPreviewMatchesCommit(self, preview):
        committed = self._commit_map()
        expected = {
            change.oid: (
                change.old_serial,
                change.new_size,
                change.blob_changed,
            )
            for change in preview.values()
        }
        self.assertEqual(committed, expected)

    def test_empty_preview(self):
        transaction.abort()
        self.assertEqual(self.conn.change_preview(), [])
        # The preview doesn't join the transaction.
        self.assertEqual(
            self.conn.transaction_manager.get()._resources, [])

    def test_preview_does_not_mutate_transaction_state(self):
        self.root['v'] = 1
        # The modification above joined the transaction; remember the
        # resource set so we can prove the preview doesn't join again.
        current = self.conn.transaction_manager.get()
        joined_before = list(current._resources)

        changes = self.conn.change_preview()
        self.assertEqual(len(changes), 1)
        change = changes[0]
        self.assertEqual(change.oid, self.root._p_oid)
        self.assertEqual(
            change.class_name,
            'persistent.mapping.PersistentMapping')
        self.assertIs(change.klass, PersistentMapping)
        self.assertEqual(change.old_serial, self.root._p_serial)
        self.assertGreater(change.new_size, 0)
        self.assertFalse(change.blob_changed)

        # Still dirty, still registered, not joined by the preview.
        self.assertTrue(self.root._p_changed)
        self.assertIn(self.root, self.conn._registered_objects)
        self.assertEqual(list(current._resources), joined_before)

        # Previewing again gives the same answer.
        again = self.conn.change_preview()
        self.assertEqual(
            [(c.old_serial, c.new_size) for c in again],
            [(change.old_serial, change.new_size)])

    def test_new_objects_get_preview_identities(self):
        first = PersistentMapping()
        second = PersistentMapping()
        first['second'] = second
        second['first'] = first
        self.root['first'] = first

        changes = self.conn.change_preview()
        new_changes = [c for c in changes
                       if isinstance(c.oid, PreviewOID)]
        self.assertEqual(len(new_changes), 2)
        tokens = {c.oid.token for c in new_changes}
        self.assertEqual(tokens, {1, 2})
        for change in new_changes:
            self.assertEqual(change.old_serial, z64)
            self.assertFalse(change.blob_changed)
            self.assertEqual(
                change.class_name,
                'persistent.mapping.PersistentMapping')

        # The preview identities are not permanently assigned.
        self.assertIsNone(first._p_oid)
        self.assertIsNone(second._p_oid)
        self.assertIsNone(first._p_jar)
        self.assertIsNone(second._p_jar)

        # The actual commit still works and writes matching records.
        committed = self._commit_map()
        new_oids = {oid for oid, (serial, size, blob) in committed.items()
                    if serial == z64}
        self.assertEqual(len(new_oids), 2)
        self.assertEqual(
            sorted(size for serial, size, blob in committed.values()),
            sorted(c.new_size for c in changes))

    def test_explicit_add_keeps_stable_oid(self):
        obj = PersistentMapping()
        self.conn.add(obj)
        self.root['obj'] = obj

        changes = self.conn.change_preview()
        change = [c for c in changes if c.oid == obj._p_oid]
        self.assertEqual(len(change), 1)
        self.assertEqual(change[0].old_serial, z64)
        # add() assigned a real oid; the preview must not touch it.
        self.assertNotIsInstance(obj._p_oid, PreviewOID)
        self.assertIs(obj._p_jar, self.conn)
        self.assertPreviewMatchesCommit(
            {c.oid: c for c in changes})

    def test_modified_then_restored_is_not_previewed(self):
        self.root['v'] = 1
        # Restore the original value and clear the dirty flag, as an
        # application that decided nothing changed would.
        self.root['v'] = 0
        self.root._p_changed = False
        self.assertEqual(self.conn.change_preview(), [])
        self.assertEqual(self._commit_map(), {})

    def test_modified_restored_value_but_still_dirty_is_written(self):
        # Changing and then assigning the original value back does not
        # clear _p_changed, so the record is still written -- and the
        # preview agrees with the actual write set.
        self.root['v'] = 1
        self.root['v'] = 0
        preview = self._preview_map()
        self.assertEqual(set(preview), {self.root._p_oid})
        self.assertPreviewMatchesCommit(preview)

    def test_blob_change_is_previewed(self):
        blob = Blob()
        self.root['blob'] = blob
        transaction.commit()
        self.storage.records = []
        # The blob was invalidated when committed; reloading its
        # committed data gives the current revision.
        with blob.open('r') as f:
            self.assertEqual(f.read(), b'')
        old_serial = blob._p_serial

        with blob.open('w') as f:
            f.write(b'blob payload')

        preview = self._preview_map()
        change = preview[blob._p_oid]
        self.assertTrue(change.blob_changed)
        self.assertEqual(change.old_serial, old_serial)
        self.assertGreater(change.new_size, 0)

        # Previewing doesn't consume the uncommitted blob data.
        self.assertIsNotNone(blob._p_blob_uncommitted)
        self.assertTrue(blob._p_changed)

        self.assertPreviewMatchesCommit(preview)

    def test_unchanged_blob_is_not_previewed(self):
        blob = Blob()
        self.root['blob'] = blob
        transaction.commit()
        self.storage.records = []
        self.assertEqual(self.conn.change_preview(), [])

    def test_savepoint_rollback_preview_reflects_state(self):
        # An object changed after a savepoint and a brand new object
        # added afterwards are discarded by savepoint rollback; the
        # preview must show only what the savepoint captured.
        self.root['v'] = 1
        savepoint = transaction.savepoint()
        self.root['v'] = 2
        new_obj = PersistentMapping()
        self.root['new'] = new_obj

        before = self._preview_map()
        self.assertIn(self.root._p_oid, before)
        self.assertEqual(
            [c for c in before.values()
             if isinstance(c.oid, PreviewOID)].__len__(), 1)

        savepoint.rollback()

        after = self._preview_map()
        # Only the root record captured by the savepoint remains; the
        # object added after the savepoint lost its identity again.
        self.assertEqual(set(after), {self.root._p_oid})
        self.assertIsNone(new_obj._p_oid)
        self.assertIsNone(new_obj._p_jar)
        self.assertPreviewMatchesCommit(after)

    def test_blob_change_after_savepoint_rollback(self):
        blob = Blob()
        self.root['blob'] = blob
        transaction.commit()
        self.storage.records = []

        with blob.open('w') as f:
            f.write(b'first version')
        savepoint = transaction.savepoint()
        with blob.open('w') as f:
            f.write(b'second version, rolled back')
        savepoint.rollback()

        preview = self._preview_map()
        change = preview[blob._p_oid]
        self.assertTrue(change.blob_changed)
        self.assertPreviewMatchesCommit(preview)

    def test_committed_write_set_matches_last_preview(self):
        first = PersistentMapping({'name': 'first'})
        second = PersistentMapping()
        first['second'] = second
        self.root['first'] = first
        self.root['v'] = 42

        # Preview several times; the last one is what gets compared.
        self.conn.change_preview()
        preview = {c.oid: c for c in self.conn.change_preview()}
        self.assertEqual(len(preview), 3)

        committed = self._commit_map()
        # One modified root plus two newly allocated object records.
        self.assertEqual(len(committed), 3)
        # Replace preview identities with the oids the commit
        # allocated: both sets have exactly two new records ...
        preview_serials = sorted(
            (c.old_serial, c.new_size, c.blob_changed)
            for c in preview.values())
        committed_serials = sorted(committed.values())
        self.assertEqual(preview_serials, committed_serials)

        # ... and after the commit the preview is empty.
        self.assertEqual(self.conn.change_preview(), [])

    def test_preview_identities_never_collide_with_real_oids(self):
        # Regression test: oids are allocated from low integers, so a
        # brand new implicit object must never borrow the oid of an
        # object that already exists (root is oid 0).  With several
        # existing objects in place, the modified container and every
        # newly reached object must show up as separate records and the
        # committed write set must match.
        for index in range(3):
            self.root['item%d' % index] = PersistentMapping(
                {'n': index})
        transaction.commit()
        self.storage.records = []

        container = self.root['item0']
        new_a = PersistentMapping({'a': 1})
        new_b = PersistentMapping({'b': new_a})
        container['new_a'] = new_a
        container['new_b'] = new_b

        preview = self._preview_map()
        # The modified existing container plus two preview identities.
        self.assertEqual(len([
            c for c in preview.values()
            if isinstance(c.oid, PreviewOID)]), 2)
        self.assertIn(container._p_oid, preview)
        self.assertIsNone(new_a._p_oid)
        self.assertIsNone(new_b._p_oid)
        self.assertPreviewMatchesCommit(preview)

    def test_preview_identity_cross_references_are_consistent(self):
        # The same new object referenced from two places must carry the
        # same preview identity wherever it appears, and the resulting
        # pickle sizes must match the real commit.
        first = PersistentMapping()
        second = PersistentMapping()
        third = PersistentMapping()
        first['peer'] = second
        third['peer'] = second
        self.root['first'] = first
        self.root['third'] = third

        preview = self.conn.change_preview()
        new_changes = [c for c in preview
                       if isinstance(c.oid, PreviewOID)]
        tokens = [c.oid.token for c in new_changes]
        self.assertEqual(len(tokens), 3)
        self.assertEqual(len(set(tokens)), 3)
        self.assertEqual(sorted(tokens), [1, 2, 3])

        transaction.commit()
        committed_sizes = sorted(
            len(data) for _, _, data, _ in self.storage.records)
        self.assertEqual(
            committed_sizes,
            sorted(c.new_size for c in preview))

    def test_preview_result_ordering_is_stable(self):
        new = PersistentMapping()
        self.root['new'] = new
        self.root['v'] = 1
        changes = self.conn.change_preview()
        # Existing (real oid) records come first, preview identities
        # last, ordered by token.
        kinds = [isinstance(c.oid, PreviewOID) for c in changes]
        self.assertEqual(kinds, sorted(kinds))
        preview_oids = [c.oid for c in changes
                        if isinstance(c.oid, PreviewOID)]
        self.assertEqual([o.token for o in preview_oids], sorted(
            o.token for o in preview_oids))

    def test_preview_on_closed_connection(self):
        self.db.close()
        from ZODB.POSException import ConnectionStateError
        with self.assertRaises(ConnectionStateError):
            self.conn.change_preview()


def test_suite():
    return unittest.defaultTestLoader.loadTestsFromTestCase(
        ChangePreviewTests)
