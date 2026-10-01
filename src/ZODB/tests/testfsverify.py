##############################################################################
#
# Copyright (c) 2026 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AND FITNESS FOR A PARTICULAR
# PURPOSE
#
##############################################################################
"""Tests for ZODB.FileStorage.fsverify."""

import hashlib
import os
import shutil
import struct

import transaction
from persistent.mapping import PersistentMapping

import ZODB
import ZODB.tests.util
from ZODB.FileStorage import FileStorage
from ZODB.FileStorage.format import DATA_HDR_LEN
from ZODB.FileStorage.format import TRANS_HDR
from ZODB.FileStorage.format import TRANS_HDR_LEN
from ZODB.FileStorage.format import DataHeaderFromString
from ZODB.FileStorage.format import TxnHeaderFromString
from ZODB.FileStorage.fsverify import Checkpoint
from ZODB.FileStorage.fsverify import CheckpointRefused
from ZODB.FileStorage.fsverify import VerifyError
from ZODB.FileStorage.fsverify import verify_file
from ZODB.utils import p64


# Offsets of fields inside a data record header (DATA_HDR == ">8s8sQQHQ").
PREV_OFFSET = 16
PLEN_OFFSET = 34


def walk(path):
    """Yield (tpos, txn_header, dpos, data_header) for every data record."""
    with open(path, 'rb') as f:
        data = f.read()
    pos = 4
    while pos < len(data):
        th = TxnHeaderFromString(data[pos:pos + TRANS_HDR_LEN])
        dpos = pos + TRANS_HDR_LEN + th.ulen + th.dlen + th.elen
        tend = pos + th.tlen
        while dpos < tend:
            dh = DataHeaderFromString(data[dpos:dpos + DATA_HDR_LEN])
            yield pos, th, dpos, dh
            dpos += dh.recordlen()
        pos = tend + 8


def digest(path):
    with open(path, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()


class FileStorageVerifyTests(ZODB.tests.util.TestCase):

    def setUp(self):
        super().setUp()
        self.path = 'verify.fs'
        # The storage stays open for the whole test: verification must
        # work while the ZODB service is running.
        self.storage = FileStorage(self.path)
        self.db = ZODB.DB(self.storage)
        self.populate()
        self.original_digest = digest(self.path)
        self.original_mtime = os.stat(self.path).st_mtime_ns
        self.original_listing = sorted(os.listdir('.'))

    def tearDown(self):
        self.db.close()
        super().tearDown()

    def populate(self):
        conn = self.db.open()
        root = conn.root()
        for i in range(5):
            root['obj'] = PersistentMapping({'n': i})
            transaction.commit()
        # Undo the last transaction to create records with back
        # pointers.
        self.db.undo(self.db.undoLog(0, 1)[0]['id'])
        transaction.commit()
        conn.close()

    def copy(self, name='copy.fs'):
        shutil.copyfile(self.path, name)
        return name

    def assert_original_untouched(self):
        self.assertEqual(digest(self.path), self.original_digest)
        self.assertEqual(os.stat(self.path).st_mtime_ns,
                         self.original_mtime)

    def test_clean_storage_passes_while_open(self):
        report = verify_file(self.path)
        self.assertTrue(report.ok)
        self.assertEqual(report.problems, [])
        self.assertEqual(report.transactions, 7)
        self.assertGreater(report.records, 7)
        size = os.path.getsize(self.path)
        self.assertEqual(report.boundary, size)
        self.assertEqual(report.file_size, size)
        self.assertEqual(report.checkpoint.pos, size)

    def test_verify_is_read_only(self):
        report = verify_file(self.path)
        self.assertTrue(report.ok)
        self.assert_original_untouched()
        self.assertEqual(sorted(os.listdir('.')), self.original_listing)

    def test_not_a_filestorage(self):
        with open('junk.fs', 'wb') as f:
            f.write(b'no way this is a file storage')
        self.assertRaises(VerifyError, verify_file, 'junk.fs')

    def test_corrupt_data_length_on_copy(self):
        copy = self.copy()
        # Damage the payload length of the last data record so the
        # record extends past the end of its transaction.
        tpos, th, dpos, dh = [r for r in walk(copy) if r[3].plen][-1]
        self.assertGreater(dh.plen, 0)
        tend = tpos + th.tlen
        with open(copy, 'r+b') as f:
            f.seek(dpos + PLEN_OFFSET)
            f.write(p64(tend - dpos))
        before = digest(copy)
        report = verify_file(copy)
        self.assertFalse(report.ok)
        (problem,) = [p for p in report.problems
                      if p.kind == 'data-length']
        self.assertEqual(problem.pos, dpos)
        self.assertEqual(problem.tid, th.tid)
        self.assertEqual(problem.oid, dh.oid)
        # Verification must not repair (or otherwise touch) the file.
        self.assertEqual(digest(copy), before)
        # The original database is untouched and still verifies clean.
        self.assert_original_untouched()
        self.assertTrue(verify_file(self.path).ok)

    def test_corrupt_txn_trailer_on_copy(self):
        copy = self.copy()
        tpos, th, dpos, dh = next(iter(walk(copy)))
        tend = tpos + th.tlen
        with open(copy, 'r+b') as f:
            f.seek(tend)
            f.write(p64(th.tlen + 8))
        report = verify_file(copy)
        self.assertFalse(report.ok)
        (problem,) = [p for p in report.problems
                      if p.kind == 'txn-trailer']
        self.assertEqual(problem.pos, tend)
        self.assertEqual(problem.tid, th.tid)
        self.assert_original_untouched()

    def test_corrupt_prev_pointer_on_copy(self):
        copy = self.copy()
        records = list(walk(copy))
        # Find a record with a previous-revision pointer such that an
        # earlier record of a different object exists.
        tpos, th, dpos, dh = next(
            r for r in records
            if r[3].prev
            and any(o[2] < r[2] and o[3].oid != r[3].oid for o in records))
        # Point the previous-revision pointer at an earlier record of
        # a different object.
        target = next(r for r in records
                      if r[2] < dpos and r[3].oid != dh.oid)
        with open(copy, 'r+b') as f:
            f.seek(dpos + PREV_OFFSET)
            f.write(p64(target[2]))
        before = digest(copy)
        report = verify_file(copy)
        self.assertFalse(report.ok)
        (problem,) = [p for p in report.problems
                      if p.kind == 'prev-pointer']
        self.assertEqual(problem.pos, dpos)
        self.assertEqual(problem.tid, th.tid)
        self.assertEqual(problem.oid, dh.oid)
        self.assertEqual(digest(copy), before)
        self.assert_original_untouched()
        self.assertTrue(verify_file(self.path).ok)

    def test_corrupt_back_pointer_on_copy(self):
        copy = self.copy()
        records = list(walk(copy))
        backs = []
        with open(copy, 'rb') as f:
            data = f.read()
        for tpos, th, dpos, dh in records:
            if dh.plen == 0:
                back = struct.unpack('>Q', data[dpos + DATA_HDR_LEN:
                                                dpos + DATA_HDR_LEN + 8])[0]
                if back:
                    backs.append((tpos, th, dpos, dh, back))
        self.assertTrue(backs, "expected undo records with back pointers")
        tpos, th, dpos, dh, back = backs[0]
        # Corrupt the back pointer to reference a different object.
        target = next(r for r in records
                      if r[2] < dpos and r[3].oid != dh.oid)
        with open(copy, 'r+b') as f:
            f.seek(dpos + DATA_HDR_LEN)
            f.write(p64(target[2]))
        report = verify_file(copy)
        self.assertFalse(report.ok)
        (problem,) = [p for p in report.problems
                      if p.kind == 'back-pointer']
        self.assertEqual(problem.pos, dpos)
        self.assertEqual(problem.tid, th.tid)
        self.assertEqual(problem.oid, dh.oid)
        self.assert_original_untouched()

    def test_appended_tail_is_not_corruption(self):
        # A partially written transaction header.
        copy = self.copy()
        size = os.path.getsize(copy)
        with open(copy, 'ab') as f:
            f.write(b'\0' * 10)
        report = verify_file(copy)
        self.assertTrue(report.ok)
        self.assertEqual(report.boundary, size)
        # A checkpoint record of a transaction in progress.
        copy = self.copy('copy2.fs')
        header = struct.pack(TRANS_HDR, b'\xff' * 8, 100, b'c', 0, 0, 0)
        with open(copy, 'ab') as f:
            f.write(header)
            f.write(b'\0' * 50)
        report = verify_file(copy)
        self.assertTrue(report.ok)
        self.assertEqual(report.boundary, size)
        # A complete header whose data has not been written yet.
        copy = self.copy('copy3.fs')
        header = struct.pack(TRANS_HDR, b'\xff' * 8, 1000, b' ', 0, 0, 0)
        with open(copy, 'ab') as f:
            f.write(header)
        report = verify_file(copy)
        self.assertTrue(report.ok)
        self.assertEqual(report.boundary, size)
        self.assert_original_untouched()

    def test_checkpoint_resume(self):
        report = verify_file(self.path)
        checkpoint = Checkpoint.from_dict(report.checkpoint.to_dict())
        self.assertEqual(checkpoint, report.checkpoint)
        # Append another transaction through the running storage.
        conn = self.db.open()
        conn.root()['more'] = PersistentMapping()
        transaction.commit()
        conn.close()
        report = verify_file(self.path, checkpoint=checkpoint)
        self.assertTrue(report.ok)
        self.assertEqual(report.transactions, 1)
        self.assertGreater(report.checkpoint.pos, checkpoint.pos)
        self.assertEqual(report.boundary, os.path.getsize(self.path))
        # Resuming again without new transactions is a no-op.
        report = verify_file(self.path, checkpoint=report.checkpoint)
        self.assertTrue(report.ok)
        self.assertEqual(report.transactions, 0)

    def test_checkpoint_refused_when_truncated(self):
        copy = self.copy()
        checkpoint = verify_file(copy).checkpoint
        with open(copy, 'r+b') as f:
            f.truncate(checkpoint.pos - 8)
        self.assertRaises(CheckpointRefused, verify_file, copy,
                          checkpoint=checkpoint)
        self.assert_original_untouched()

    def test_checkpoint_refused_when_replaced(self):
        copy = self.copy()
        checkpoint = verify_file(copy).checkpoint
        os.rename(copy, 'replaced.fs')
        shutil.copyfile('replaced.fs', copy)
        self.assertRaises(CheckpointRefused, verify_file, copy,
                          checkpoint=checkpoint)
        self.assert_original_untouched()

    def test_checkpoint_refused_when_verified_region_changed(self):
        copy = self.copy()
        checkpoint = verify_file(copy).checkpoint
        # Flip a byte inside the already verified region.
        with open(copy, 'r+b') as f:
            f.seek(checkpoint.pos - 1)
            byte = f.read(1)
            f.seek(checkpoint.pos - 1)
            f.write(bytes([byte[0] ^ 0xff]))
        self.assertRaises(CheckpointRefused, verify_file, copy,
                          checkpoint=checkpoint)
        self.assert_original_untouched()

    def test_blob_references(self):
        from ZODB.blob import Blob
        from ZODB.blob import BlobStorage
        storage = BlobStorage('blobs', FileStorage('blob.fs'))
        db = ZODB.DB(storage)
        try:
            conn = db.open()
            blob = Blob()
            conn.root()['blob'] = blob
            with blob.open('w') as fp:
                fp.write(b'blob data')
            transaction.commit()
            conn.close()
            report = verify_file('blob.fs', blob_dir='blobs')
            self.assertTrue(report.ok)
            # Remove the committed blob file; verification must
            # report the dangling reference.
            committed = []
            for dirpath, dirnames, filenames in os.walk('blobs'):
                if os.path.basename(dirpath) == 'tmp':
                    continue
                committed.extend(
                    os.path.join(dirpath, name) for name in filenames)
            committed = [name for name in committed
                         if name.endswith('.blob')]
            self.assertEqual(len(committed), 1)
            os.remove(committed[0])
            report = verify_file('blob.fs', blob_dir='blobs')
            self.assertFalse(report.ok)
            (problem,) = [p for p in report.problems
                          if p.kind == 'blob-reference']
            self.assertEqual(problem.oid, blob._p_oid)
        finally:
            db.close()


def test_suite():
    import unittest
    return unittest.defaultTestLoader.loadTestsFromTestCase(
        FileStorageVerifyTests)
