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
"""Tests for ZODB.FileStorage.fsverify."""

import hashlib
import os
import shutil
import struct
import unittest

import transaction

import ZODB.FileStorage
from ZODB.blob import Blob
from ZODB.DB import DB
from ZODB.FileStorage.format import DATA_HDR_LEN
from ZODB.FileStorage.format import TRANS_HDR_LEN
from ZODB.FileStorage.fsverify import BACK_POINTER
from ZODB.FileStorage.fsverify import BLOB_REFERENCE
from ZODB.FileStorage.fsverify import RECORD_LENGTH
from ZODB.FileStorage.fsverify import TXN_TRAILER
from ZODB.FileStorage.fsverify import VerifyCheckpoint
from ZODB.FileStorage.fsverify import VerifyResumeError
from ZODB.FileStorage.fsverify import verify_file_storage
from ZODB.tests.StorageTestBase import StorageTestBase
from ZODB.utils import p64
from ZODB.utils import u64


def walk_records(path):
    """Yield (tpos, tid, dpos, oid, prev, plen) for each data record."""
    with open(path, 'rb') as f:
        f.seek(0, 2)
        size = f.tell()
        pos = 4
        while pos + TRANS_HDR_LEN <= size:
            f.seek(pos)
            tid, tlen, status, ulen, dlen, elen = struct.unpack(
                '>8sQcHHH', f.read(TRANS_HDR_LEN))
            if pos + tlen + 8 > size:
                break
            f.seek(pos + tlen)
            if u64(f.read(8)) != tlen:
                break
            dpos = pos + TRANS_HDR_LEN + ulen + dlen + elen
            tend = pos + tlen
            while dpos < tend:
                f.seek(dpos)
                oid, rtid, prev, tloc, vlen, plen = struct.unpack(
                    '>8s8sQQHQ', f.read(DATA_HDR_LEN))
                yield pos, tid, dpos, oid, prev, plen
                dpos += DATA_HDR_LEN + (plen or 8)
            pos = tend + 8


def file_state(path):
    """A (size, mtime, digest) triple to detect any write to a file."""
    with open(path, 'rb') as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    st = os.stat(path)
    return st.st_size, st.st_mtime_ns, digest


class FileStorageVerifyTests(StorageTestBase):

    def setUp(self):
        StorageTestBase.setUp(self)
        self._storage = ZODB.FileStorage.FileStorage('Data.fs', create=1)

    def _do_stores(self, noids=3, nrevs=3):
        oids = []
        for i in range(noids):
            oid = self._storage.new_oid()
            revid = None
            for j in range(nrevs):
                revid = self._dostore(oid, revid=revid, data=i * 10 + j)
            oids.append(oid)
        return oids

    def test_verify_clean_storage_while_open(self):
        # The storage stays open (the service is running) and is not
        # written to by the verification.
        self._do_stores()
        before = file_state('Data.fs')
        report = self._storage.verify()
        self.assertTrue(report.ok, report.issues)
        self.assertEqual(report.scanned_from, 4)
        self.assertEqual(report.scanned_to, os.path.getsize('Data.fs'))
        self.assertEqual(report.checkpoint.pos, report.scanned_to)
        self.assertEqual(report.checkpoint.tid, self._storage._ltid)
        self.assertEqual(file_state('Data.fs'), before)

    def test_verify_file_storage_function(self):
        self._do_stores()
        before = file_state('Data.fs')
        report = verify_file_storage('Data.fs')
        self.assertTrue(report.ok, report.issues)
        self.assertEqual(file_state('Data.fs'), before)

    def test_corrupt_record_length(self):
        self._do_stores()
        shutil.copy('Data.fs', 'copy.fs')
        tpos, tid, dpos, oid, prev, plen = next(iter(list(walk_records(
            'copy.fs'))))
        # Inflate the data length of the first record.
        with open('copy.fs', 'r+b') as f:
            f.seek(dpos + 34)  # plen field of the data header
            f.write(p64(plen + 0x1000))
        before = file_state('Data.fs')
        report = verify_file_storage('copy.fs')
        self.assertFalse(report.ok)
        matches = [i for i in report.issues if i.kind == RECORD_LENGTH]
        self.assertTrue(matches, report.issues)
        self.assertEqual(matches[0].pos, dpos)
        self.assertEqual(matches[0].tid, tid)
        # The original database was not written to.
        self.assertEqual(file_state('Data.fs'), before)

    def test_corrupt_back_pointer(self):
        self._do_stores()
        shutil.copy('Data.fs', 'copy.fs')
        records = list(walk_records('copy.fs'))
        first_dpos, first_oid = records[0][2], records[0][3]
        # Find the last record with a revision back pointer and make it
        # point to an earlier record of a different object.
        tpos, tid, dpos, oid, prev, plen = [
            r for r in records if r[4]][-1]
        self.assertNotEqual(oid, first_oid)
        self.assertNotEqual(prev, first_dpos)
        with open('copy.fs', 'r+b') as f:
            f.seek(dpos + 16)  # prev field of the data header
            f.write(p64(first_dpos))
        before = file_state('Data.fs')
        report = verify_file_storage('copy.fs')
        self.assertFalse(report.ok)
        matches = [i for i in report.issues if i.kind == BACK_POINTER]
        self.assertEqual(len(matches), 1, report.issues)
        self.assertEqual(matches[0].pos, dpos)
        self.assertEqual(matches[0].tid, tid)
        self.assertEqual(matches[0].oid, oid)
        # The original database was not written to.
        self.assertEqual(file_state('Data.fs'), before)

    def test_corrupt_txn_trailer(self):
        self._do_stores()
        shutil.copy('Data.fs', 'copy.fs')
        tpos, tid, dpos, oid, prev, plen = next(iter(list(walk_records(
            'copy.fs'))))
        with open('copy.fs', 'r+b') as f:
            f.seek(tpos + 8)
            tlen = u64(f.read(8))
            f.seek(tpos + tlen)  # the redundant trailer
            f.write(p64(tlen + 1))
        report = verify_file_storage('copy.fs')
        self.assertFalse(report.ok)
        matches = [i for i in report.issues if i.kind == TXN_TRAILER]
        self.assertEqual(len(matches), 1, report.issues)
        self.assertEqual(matches[0].pos, tpos + tlen)
        self.assertEqual(matches[0].tid, tid)

    def test_tail_being_appended_is_not_corruption(self):
        self._do_stores()
        size = os.path.getsize('Data.fs')

        # A partially appended transaction header.
        shutil.copy('Data.fs', 'tail1.fs')
        with open('tail1.fs', 'ab') as f:
            f.write(b'\x00' * 10)
        report = verify_file_storage('tail1.fs')
        self.assertTrue(report.ok, report.issues)
        self.assertEqual(report.scanned_to, size)

        # A transaction header whose data has not been appended yet.
        shutil.copy('Data.fs', 'tail2.fs')
        with open('tail2.fs', 'ab') as f:
            f.write(struct.pack('>8sQcHHH', self._storage._ltid,
                                10 ** 6, b' ', 0, 0, 0))
        report = verify_file_storage('tail2.fs')
        self.assertTrue(report.ok, report.issues)
        self.assertEqual(report.scanned_to, size)

        # A checkpoint of a transaction in progress.
        shutil.copy('Data.fs', 'tail3.fs')
        with open('tail3.fs', 'ab') as f:
            f.write(struct.pack('>8sQcHHH', self._storage._ltid,
                                23 + 8, b'c', 0, 0, 0))
            f.write(p64(23 + 8))
        report = verify_file_storage('tail3.fs')
        self.assertTrue(report.ok, report.issues)
        self.assertEqual(report.scanned_to, size)

    def test_checkpoint_resume(self):
        self._do_stores(2, 2)
        report = self._storage.verify()
        self.assertTrue(report.ok, report.issues)
        checkpoint = report.checkpoint
        # Checkpoints survive a serialization round trip.
        checkpoint = VerifyCheckpoint.from_dict(checkpoint.to_dict())
        # More transactions are appended by the running service.
        self._do_stores(2, 2)
        report = self._storage.verify(checkpoint=checkpoint)
        self.assertTrue(report.ok, report.issues)
        self.assertEqual(report.scanned_from, checkpoint.pos)
        self.assertEqual(report.scanned_to, os.path.getsize('Data.fs'))

    def test_resume_refused_when_truncated(self):
        self._do_stores(2, 2)
        checkpoint = self._storage.verify().checkpoint
        self._storage.close()
        self._storage = None
        with open('Data.fs', 'r+b') as f:
            f.truncate(checkpoint.pos - 20)
        with self.assertRaises(VerifyResumeError):
            verify_file_storage('Data.fs', checkpoint=checkpoint)

    def test_resume_refused_when_replaced(self):
        self._do_stores(2, 2)
        checkpoint = self._storage.verify().checkpoint
        self._storage.close()
        self._storage = None
        replacement = ZODB.FileStorage.FileStorage('Other.fs', create=1)
        replacement.close()
        os.remove('Data.fs')
        os.rename('Other.fs', 'Data.fs')
        with self.assertRaises(VerifyResumeError):
            verify_file_storage('Data.fs', checkpoint=checkpoint)

    def test_blob_references(self):
        self._storage.close()
        self._storage = ZODB.FileStorage.FileStorage(
            'Data.fs', create=True, blob_dir='blobs')
        db = DB(self._storage)
        conn = db.open()
        conn.root()['blob'] = Blob(b'blob data')
        transaction.commit()
        db.close()
        self._storage = None

        report = verify_file_storage('Data.fs', blob_dir='blobs')
        self.assertTrue(report.ok, report.issues)

        # A copy of the data file without its blob directory reports
        # the dangling blob reference.
        shutil.copy('Data.fs', 'copy.fs')
        report = verify_file_storage('copy.fs', blob_dir='missing-blobs')
        self.assertFalse(report.ok)
        matches = [i for i in report.issues if i.kind == BLOB_REFERENCE]
        self.assertEqual(len(matches), 1, report.issues)
        self.assertIsNotNone(matches[0].tid)
        self.assertGreater(matches[0].pos, 4)


def test_suite():
    return unittest.defaultTestLoader.loadTestsFromName(__name__)


if __name__ == '__main__':
    unittest.main()
