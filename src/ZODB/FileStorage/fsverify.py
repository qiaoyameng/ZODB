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
"""Read-only structural verification of FileStorage data files.

This module provides :func:`verify_file`, a read-only checker that can
be run against a FileStorage data file while the storage is still in
use by a running ZODB service.  The file is only ever opened for
reading; verification never modifies, truncates, or repairs the
database.

The checker first pins a complete transaction boundary: the file size
is sampled once, and only transactions that were completely written at
that point are examined.  A transaction that is still being appended
(a partial header, a checkpoint record with status ``'c'``, or a
record extending beyond the pinned size) simply ends the verified
region and is never reported as corruption.

Within the verified region every transaction is checked record by
record:

- transaction and data record lengths must be consistent and must
  tile the transaction exactly,
- the redundant transaction length trailer must match the header,
- each data record must point back at its transaction, and its
  previous-revision (``prev``) and back (``back``) pointers must
  reference earlier records of the same object,
- optionally, blob records must reference existing files in a blob
  directory.

Every problem is reported with the file offset and the transaction
(and object, for record-level problems) it belongs to.

Verification returns a :class:`Checkpoint` describing the verified
boundary.  A later call may pass the checkpoint back to verify only
the transactions appended since.  Resuming is refused with
:class:`CheckpointRefused` if the file was replaced or truncated in
the meantime.

Typical usage::

    from ZODB.FileStorage.fsverify import Checkpoint, verify_file

    report = verify_file('Data.fs')
    if not report.ok:
        for problem in report.problems:
            print(problem)

    saved = report.checkpoint.to_dict()
    ...
    report = verify_file('Data.fs', checkpoint=Checkpoint.from_dict(saved))
"""

import os

from ZODB._compat import FILESTORAGE_MAGIC
from ZODB.FileStorage.format import DATA_HDR_LEN
from ZODB.FileStorage.format import TRANS_HDR_LEN
from ZODB.FileStorage.format import DataHeaderFromString
from ZODB.FileStorage.format import TxnHeaderFromString
from ZODB.utils import oid_repr
from ZODB.utils import u64
from ZODB.utils import z64


class VerifyError(Exception):
    """Base class for verification errors."""


class CheckpointRefused(VerifyError):
    """A checkpoint could not be used to resume verification.

    Raised when the file was replaced or truncated since the
    checkpoint was taken, or when the checkpoint itself is invalid.
    """


class Problem:
    """A single structural problem found in a FileStorage file.

    Attributes:

    - ``kind``: one of ``'txn-header'``, ``'txn-order'``,
      ``'txn-trailer'``, ``'data-header'``, ``'data-length'``,
      ``'prev-pointer'``, ``'back-pointer'`` or ``'blob-reference'``.
    - ``pos``: file offset of the offending structure.
    - ``tid``: id of the transaction the problem belongs to
      (``None`` if it could not be determined).
    - ``oid``: id of the object the problem belongs to
      (``None`` for transaction-level problems).
    - ``message``: human-readable description.
    """

    def __init__(self, kind, pos, message, tid=None, oid=None):
        self.kind = kind
        self.pos = pos
        self.tid = tid
        self.oid = oid
        self.message = message

    def __repr__(self):
        return "Problem(%r, %d, %r)" % (self.kind, self.pos, self.message)

    def __str__(self):
        parts = ["%s at offset %d" % (self.kind, self.pos)]
        if self.tid is not None:
            parts.append("in transaction %s" % oid_repr(self.tid))
        if self.oid is not None:
            parts.append("for object %s" % oid_repr(self.oid))
        return "%s: %s" % (" ".join(parts), self.message)


class Checkpoint:
    """A verified transaction boundary in a FileStorage file.

    Everything before ``pos`` was verified.  ``tid`` is the id of the
    last verified transaction.  ``file_size``, ``device`` and ``inode``
    identify the file the checkpoint was taken from; they are used to
    refuse resuming from a file that was replaced or truncated.
    """

    def __init__(self, pos, tid, file_size, device, inode):
        self.pos = pos
        self.tid = tid
        self.file_size = file_size
        self.device = device
        self.inode = inode

    def __eq__(self, other):
        if not isinstance(other, Checkpoint):
            return NotImplemented
        return vars(self) == vars(other)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return ("Checkpoint(pos=%d, tid=%r, file_size=%d, device=%d, "
                "inode=%d)" % (self.pos, self.tid, self.file_size,
                               self.device, self.inode))

    def to_dict(self):
        """Return a JSON-serializable representation of the checkpoint."""
        return {
            'pos': self.pos,
            'tid': None if self.tid is None else self.tid.hex(),
            'file_size': self.file_size,
            'device': self.device,
            'inode': self.inode,
        }

    @classmethod
    def from_dict(cls, state):
        """Rebuild a checkpoint from :meth:`to_dict` output."""
        tid = state['tid']
        return cls(
            pos=state['pos'],
            tid=None if tid is None else bytes.fromhex(tid),
            file_size=state['file_size'],
            device=state['device'],
            inode=state['inode'],
        )


class VerifyReport:
    """The outcome of a :func:`verify_file` run.

    Attributes:

    - ``path``: the verified file.
    - ``file_size``: the pinned file size used for this run.
    - ``boundary``: offset of the end of the last complete
      transaction; nothing at or after this offset was examined.
    - ``problems``: list of :class:`Problem` instances found within
      the verified region.
    - ``transactions`` / ``records``: number of transactions and data
      records examined in this run.
    - ``checkpoint``: a :class:`Checkpoint` usable to resume
      verification after the verified boundary.
    """

    def __init__(self, path, file_size, boundary, problems,
                 transactions, records, checkpoint):
        self.path = path
        self.file_size = file_size
        self.boundary = boundary
        self.problems = problems
        self.transactions = transactions
        self.records = records
        self.checkpoint = checkpoint

    @property
    def ok(self):
        """True if no problems were found."""
        return not self.problems

    def __repr__(self):
        return ("VerifyReport(%r, ok=%r, transactions=%d, records=%d, "
                "boundary=%d)" % (self.path, self.ok, self.transactions,
                                  self.records, self.boundary))


def verify_file(path, checkpoint=None, blob_dir=None):
    """Verify the structural integrity of a FileStorage data file.

    The file is opened read-only and is never modified.

    ``checkpoint`` may be a :class:`Checkpoint` returned by an earlier
    run to verify only the transactions appended since.  Resuming is
    refused with :class:`CheckpointRefused` if the file was replaced
    or truncated.

    If ``blob_dir`` is given, records that reference blobs are checked
    for the existence of the corresponding blob file below that
    directory.

    Returns a :class:`VerifyReport`.
    """
    file = open(path, 'rb')
    try:
        return _Scanner(file, path, checkpoint, blob_dir).run()
    finally:
        file.close()


class _Scanner:

    def __init__(self, file, path, checkpoint, blob_dir):
        self.path = path
        self._file = file
        stat = os.fstat(file.fileno())
        self.device = stat.st_dev
        self.inode = stat.st_ino
        # Pin the file size: transactions beyond this point are still
        # being written and are not our business.
        self.size = stat.st_size
        self.problems = []
        self.transactions = 0
        self.records = 0
        self._blob_helper = None
        self._is_blob_record = None
        if blob_dir is not None:
            from ZODB.blob import FilesystemHelper
            from ZODB.blob import is_blob_record
            self._blob_helper = FilesystemHelper(blob_dir)
            self._is_blob_record = is_blob_record
        if checkpoint is None:
            self.start = self._check_magic()
            self.ltid = z64
        else:
            self.start = self._check_resume(checkpoint)
            self.ltid = checkpoint.tid if checkpoint.tid is not None else z64

    def _read_at(self, pos, length):
        self._file.seek(pos)
        return self._file.read(length)

    def _check_magic(self):
        magic = self._read_at(0, 4)
        if magic != FILESTORAGE_MAGIC:
            raise VerifyError(
                "%r is not a FileStorage data file (bad magic %r)"
                % (self.path, magic))
        return 4

    def _check_resume(self, checkpoint):
        if (checkpoint.device, checkpoint.inode) != (self.device, self.inode):
            raise CheckpointRefused(
                "%s was replaced since the checkpoint was taken" % self.path)
        if checkpoint.pos < 4:
            raise CheckpointRefused(
                "invalid checkpoint position %d" % checkpoint.pos)
        if self.size < checkpoint.pos:
            raise CheckpointRefused(
                "%s was truncated since the checkpoint was taken" % self.path)
        if checkpoint.pos == 4:
            self._check_magic()
            return 4
        # The transaction ending exactly at the checkpoint must still
        # be intact; otherwise the verified region was tampered with.
        trailer = self._read_at(checkpoint.pos - 8, 8)
        tlen = u64(trailer)
        tpos = checkpoint.pos - 8 - tlen
        if tpos < 4:
            raise CheckpointRefused(
                "checkpoint does not fall on a transaction boundary")
        header = self._read_at(tpos, TRANS_HDR_LEN)
        if len(header) < TRANS_HDR_LEN:
            raise CheckpointRefused(
                "checkpoint does not fall on a transaction boundary")
        try:
            th = TxnHeaderFromString(header)
        except (UnicodeDecodeError, ValueError):
            raise CheckpointRefused(
                "checkpoint does not fall on a transaction boundary")
        if th.tlen != tlen or th.tid != checkpoint.tid:
            raise CheckpointRefused(
                "%s changed in the already verified region" % self.path)
        return checkpoint.pos

    def _problem(self, kind, pos, message, tid=None, oid=None):
        self.problems.append(Problem(kind, pos, message, tid=tid, oid=oid))

    def run(self):
        pos = self.start
        ltid = self.ltid
        size = self.size
        while pos < size:
            header = self._read_at(pos, TRANS_HDR_LEN)
            if len(header) < TRANS_HDR_LEN:
                # Partial transaction header: an append is in
                # progress.  Not corruption.
                break
            try:
                th = TxnHeaderFromString(header)
            except (UnicodeDecodeError, ValueError):
                self._problem('txn-header', pos,
                              "unparseable transaction header")
                break
            if th.status == 'c':
                # Checkpoint of a transaction that is still in
                # progress.  Not corruption.
                break
            if th.status not in ' pu':
                self._problem('txn-header', pos,
                              "invalid transaction status %r" % th.status,
                              tid=th.tid)
                break
            headerlen = TRANS_HDR_LEN + th.ulen + th.dlen + th.elen
            if th.tlen < headerlen:
                self._problem(
                    'txn-header', pos,
                    "transaction length %d is smaller than header length %d"
                    % (th.tlen, headerlen), tid=th.tid)
                break
            tend = pos + th.tlen
            if tend + 8 > size:
                # The transaction extends beyond the pinned file
                # size: it is still being appended.  Not corruption.
                break
            if th.tid <= ltid:
                self._problem(
                    'txn-order', pos,
                    "time-stamp reduction: %s <= %s"
                    % (oid_repr(th.tid), oid_repr(ltid)), tid=th.tid)
            ltid = th.tid
            self._check_records(th, pos, tend)
            trailer = u64(self._read_at(tend, 8))
            if trailer != th.tlen:
                self._problem(
                    'txn-trailer', tend,
                    "redundant transaction length %d does not match "
                    "header length %d" % (trailer, th.tlen), tid=th.tid)
            self.transactions += 1
            pos = tend + 8
        self.boundary = pos
        self.ltid = ltid
        return VerifyReport(
            self.path, self.size, self.boundary, self.problems,
            self.transactions, self.records,
            Checkpoint(self.boundary, None if self.boundary == 4 else ltid,
                       self.size, self.device, self.inode))

    def _check_records(self, th, tpos, tend):
        pos = tpos + TRANS_HDR_LEN + th.ulen + th.dlen + th.elen
        while pos < tend:
            if pos + DATA_HDR_LEN > tend:
                self._problem(
                    'data-header', pos,
                    "data record header extends past end of transaction",
                    tid=th.tid)
                return
            raw = self._read_at(pos, DATA_HDR_LEN)
            try:
                dh = DataHeaderFromString(raw)
            except ValueError as error:
                self._problem('data-header', pos, str(error), tid=th.tid)
                return
            if dh.tid != th.tid:
                self._problem(
                    'data-header', pos,
                    "data record tid %s does not match transaction tid %s"
                    % (oid_repr(dh.tid), oid_repr(th.tid)),
                    tid=th.tid, oid=dh.oid)
            if dh.tloc != tpos:
                self._problem(
                    'data-header', pos,
                    "data record points at transaction offset %d, not %d"
                    % (dh.tloc, tpos), tid=th.tid, oid=dh.oid)
            reclen = dh.recordlen()
            if pos + reclen > tend:
                self._problem(
                    'data-length', pos,
                    "data record of %d bytes extends past end of "
                    "transaction at %d" % (reclen, tend),
                    tid=th.tid, oid=dh.oid)
                return
            self._check_prev(dh, pos, th)
            if dh.plen:
                self._check_blob(dh, pos)
            else:
                # The back pointer follows the header of records
                # without data.
                dh.back = u64(self._read_at(pos + DATA_HDR_LEN, 8))
                self._check_back(dh, pos, th)
            self.records += 1
            pos += reclen

    def _check_prev(self, dh, pos, th):
        prev = dh.prev
        if not prev:
            return
        if prev < 4 or prev >= pos:
            self._problem('prev-pointer', pos,
                          "invalid previous pointer %d" % prev,
                          tid=th.tid, oid=dh.oid)
            return
        other = self._read_record_oid(prev)
        if other != dh.oid:
            self._problem(
                'prev-pointer', pos,
                "previous pointer %d does not reference an earlier "
                "record of the same object" % prev,
                tid=th.tid, oid=dh.oid)

    def _check_back(self, dh, pos, th):
        back = dh.back
        if not back:
            # A zero back pointer undoes the object's creation.
            return
        if back < 4 or back >= pos:
            self._problem('back-pointer', pos,
                          "invalid back pointer %d" % back,
                          tid=th.tid, oid=dh.oid)
            return
        other = self._read_record_oid(back)
        if other != dh.oid:
            self._problem(
                'back-pointer', pos,
                "back pointer %d does not reference an earlier "
                "record of the same object" % back,
                tid=th.tid, oid=dh.oid)

    def _read_record_oid(self, pos):
        raw = self._read_at(pos, DATA_HDR_LEN)
        if len(raw) < DATA_HDR_LEN:
            return None
        try:
            return DataHeaderFromString(raw).oid
        except ValueError:
            return None

    def _check_blob(self, dh, pos):
        if self._blob_helper is None:
            return
        data = self._read_at(pos + DATA_HDR_LEN, dh.plen)
        if not self._is_blob_record(data):
            return
        filename = self._blob_helper.getBlobFilename(dh.oid, dh.tid)
        if not os.path.exists(filename):
            self._problem('blob-reference', pos,
                          "missing blob file %s" % filename,
                          tid=dh.tid, oid=dh.oid)
