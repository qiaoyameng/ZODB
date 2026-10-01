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
"""Read-only structural verification of FileStorage data files.

This module provides :func:`verify_file_storage`, a read-only checker
that can be used while a ZODB service has the storage open and is
appending to it.  The verifier

- opens the data file in binary read-only mode and never writes to it,
- first pins a *complete transaction boundary*: the offset just past
  the last transaction whose header, length and redundant trailer are
  fully present and consistent.  A tail that is still being appended
  (short header, checkpoint status, transaction extending past the
  current end of file, or a file that grows during the scan) is
  excluded from the boundary and is never reported as corruption,
- then checks, record by record, the data-record lengths, the
  transaction trailer, the OID revision back pointers (``prev`` and
  backpointers of empty data records) and, when a blob directory is
  given, that every blob record has its blob file,
- reports every problem found inside the boundary as a
  :class:`VerifyIssue` locating the transaction id and the file
  offset,
- returns a :class:`VerifyCheckpoint` describing the verified prefix
  of the file.  A later call may pass the checkpoint back to resume
  verification where it stopped.  Resuming is refused with
  :class:`VerifyResumeError` if the file was replaced, truncated or
  no longer matches the checkpoint.

Nothing is ever repaired; the file is only read.
"""

import os

from ZODB._compat import FILESTORAGE_MAGIC
from ZODB.FileStorage.format import DATA_HDR_LEN
from ZODB.FileStorage.format import TRANS_HDR_LEN
from ZODB.FileStorage.format import DataHeaderFromString
from ZODB.FileStorage.format import TxnHeaderFromString
from ZODB.utils import oid_repr
from ZODB.utils import u64


# VerifyIssue kinds
TXN_HEADER = 'txn-header'
TXN_TRAILER = 'txn-trailer'
RECORD_HEADER = 'record-header'
RECORD_LENGTH = 'record-length'
BACK_POINTER = 'back-pointer'
BLOB_REFERENCE = 'blob-reference'
FILE_HEADER = 'file-header'


class VerifyResumeError(Exception):
    """A checkpoint cannot be used to resume verification.

    Raised when the data file was replaced, truncated or otherwise
    changed since the checkpoint was taken.
    """


class VerifyIssue:
    """A single structural problem found in a data file."""

    def __init__(self, kind, message, pos, tid=None, oid=None):
        self.kind = kind
        self.message = message
        self.pos = pos
        self.tid = tid
        self.oid = oid

    def __repr__(self):
        parts = ['%s at offset %d' % (self.kind, self.pos)]
        if self.tid is not None:
            parts.append('tid %s' % oid_repr(self.tid))
        if self.oid is not None:
            parts.append('oid %s' % oid_repr(self.oid))
        return '<VerifyIssue %s: %s>' % (', '.join(parts), self.message)


class VerifyCheckpoint:
    """Describes a verified prefix of a data file.

    Attributes:

    ``pos``
        Offset just past the last verified transaction.
    ``tid``
        Transaction id of the last verified transaction (or None).
    ``tlen``
        Redundant trailer length of the last verified transaction,
        used to recognize the file when resuming (or None).
    ``file_size``, ``device``, ``inode``
        Identity of the verified file.
    ``magic``
        The 4-byte file identifier.
    """

    def __init__(self, pos, tid, tlen, file_size, device, inode, magic):
        self.pos = pos
        self.tid = tid
        self.tlen = tlen
        self.file_size = file_size
        self.device = device
        self.inode = inode
        self.magic = magic

    def to_dict(self):
        """Return a JSON-serializable representation."""
        return {
            'pos': self.pos,
            'tid': self.tid.hex() if self.tid is not None else None,
            'tlen': self.tlen,
            'file_size': self.file_size,
            'device': self.device,
            'inode': self.inode,
            'magic': self.magic.hex(),
        }

    @classmethod
    def from_dict(cls, data):
        """Rebuild a checkpoint from :meth:`to_dict` output."""
        tid = data['tid']
        return cls(
            pos=data['pos'],
            tid=bytes.fromhex(tid) if tid is not None else None,
            tlen=data['tlen'],
            file_size=data['file_size'],
            device=data['device'],
            inode=data['inode'],
            magic=bytes.fromhex(data['magic']),
        )

    def __repr__(self):
        return '<VerifyCheckpoint pos=%d tid=%s>' % (
            self.pos, oid_repr(self.tid) if self.tid is not None else None)


class VerifyReport:
    """Result of verifying a FileStorage data file."""

    def __init__(self, path):
        self.path = path
        self.scanned_from = None
        self.scanned_to = None
        self.file_size = None
        self.issues = []
        self.checkpoint = None

    @property
    def ok(self):
        """True when no problem was found."""
        return not self.issues

    def add_issue(self, kind, message, pos, tid=None, oid=None):
        issue = VerifyIssue(kind, message, pos, tid=tid, oid=oid)
        self.issues.append(issue)
        return issue

    def __repr__(self):
        return '<VerifyReport %s ok=%r issues=%d boundary=%d>' % (
            self.path, self.ok, len(self.issues), self.scanned_to or 0)


def _read_at(f, pos, size):
    f.seek(pos)
    return f.read(size)


def _check_checkpoint(f, st, checkpoint, magic):
    """Raise VerifyResumeError if the file doesn't match the checkpoint."""
    if (st.st_dev, st.st_ino) != (checkpoint.device, checkpoint.inode):
        raise VerifyResumeError(
            'data file was replaced since the checkpoint was taken')
    if st.st_size < checkpoint.pos:
        raise VerifyResumeError(
            'data file was truncated: size %d is before the '
            'checkpoint position %d' % (st.st_size, checkpoint.pos))
    if magic != checkpoint.magic:
        raise VerifyResumeError('data file identifier changed')
    if checkpoint.tlen is not None:
        trailer = _read_at(f, checkpoint.pos - 8, 8)
        if len(trailer) != 8 or u64(trailer) != checkpoint.tlen:
            raise VerifyResumeError(
                'data file no longer matches the checkpoint trailer')


def _check_back_pointer(f, ptr, oid, pos, tid, report, label):
    """Check that a prev/back pointer refers to a record for oid."""
    if ptr < 4 or ptr >= pos:
        report.add_issue(
            BACK_POINTER,
            'invalid %s pointer %d' % (label, ptr), pos, tid=tid, oid=oid)
        return
    raw = _read_at(f, ptr, DATA_HDR_LEN)
    if len(raw) != DATA_HDR_LEN:
        report.add_issue(
            BACK_POINTER,
            '%s pointer %d does not point to a data record' % (label, ptr),
            pos, tid=tid, oid=oid)
        return
    try:
        target = DataHeaderFromString(raw)
    except ValueError:
        report.add_issue(
            BACK_POINTER,
            '%s pointer %d does not point to a data record' % (label, ptr),
            pos, tid=tid, oid=oid)
        return
    if target.oid != oid:
        report.add_issue(
            BACK_POINTER,
            '%s pointer %d points to a record of a different object'
            % (label, ptr), pos, tid=tid, oid=oid)


def _check_records(f, th, tpos, report, blob_helper, is_blob_record):
    """Check the data records of the transaction at tpos."""
    tend = tpos + th.tlen
    pos = tpos + th.headerlen()
    while pos < tend:
        if pos + DATA_HDR_LEN > tend:
            report.add_issue(
                RECORD_LENGTH,
                'data record header extends past end of transaction',
                pos, tid=th.tid)
            return
        raw = _read_at(f, pos, DATA_HDR_LEN)
        try:
            dh = DataHeaderFromString(raw)
        except ValueError as e:
            report.add_issue(
                RECORD_HEADER, 'invalid data record header: %s' % e,
                pos, tid=th.tid)
            return
        if dh.tid != th.tid:
            report.add_issue(
                RECORD_HEADER,
                'data record tid does not match transaction id',
                pos, tid=th.tid, oid=dh.oid)
        if dh.tloc != tpos:
            report.add_issue(
                RECORD_HEADER,
                'data record does not point to its transaction: '
                '%d != %d' % (dh.tloc, tpos), pos, tid=th.tid, oid=dh.oid)
        recordlen = dh.recordlen()
        if pos + recordlen > tend:
            report.add_issue(
                RECORD_LENGTH,
                'data record of length %d extends past end of transaction'
                % recordlen, pos, tid=th.tid, oid=dh.oid)
            return
        if dh.prev:
            _check_back_pointer(
                f, dh.prev, dh.oid, pos, th.tid, report, 'previous')
        if dh.plen:
            if blob_helper is not None:
                data = _read_at(f, pos + DATA_HDR_LEN, dh.plen)
                if is_blob_record(data):
                    blob_path = blob_helper.getBlobFilename(dh.oid, dh.tid)
                    if not os.path.exists(blob_path):
                        report.add_issue(
                            BLOB_REFERENCE,
                            'missing blob file %s' % blob_path,
                            pos, tid=th.tid, oid=dh.oid)
        else:
            back = u64(_read_at(f, pos + DATA_HDR_LEN, 8))
            if back:
                _check_back_pointer(
                    f, back, dh.oid, pos, th.tid, report, 'back')
        pos += recordlen
    if pos != tend:
        report.add_issue(
            RECORD_LENGTH,
            'data records do not exactly fill the transaction: '
            '%d != %d' % (pos, tend), pos, tid=th.tid)


def verify_file_storage(path, checkpoint=None, blob_dir=None):
    """Verify the structural integrity of a FileStorage data file.

    ``path`` is the data file to check.  It is opened read-only and is
    never written to, so this may be used while a ZODB service has the
    storage open.

    ``checkpoint`` may be a :class:`VerifyCheckpoint` returned by an
    earlier call; verification then resumes at the checkpoint position.
    :class:`VerifyResumeError` is raised if the file was replaced,
    truncated or no longer matches the checkpoint.

    ``blob_dir``, when given, is the storage's blob directory; blob
    records are then checked for the existence of their blob file.

    Returns a :class:`VerifyReport`.  Nothing is ever repaired.
    """
    from ZODB.blob import is_blob_record
    if blob_dir is not None:
        from ZODB.blob import FilesystemHelper
        blob_helper = FilesystemHelper(blob_dir)
    else:
        blob_helper = None

    report = VerifyReport(path)
    with open(path, 'rb') as f:
        st = os.fstat(f.fileno())
        size = st.st_size
        report.file_size = size
        magic = _read_at(f, 0, 4)
        if len(magic) != 4 or magic != FILESTORAGE_MAGIC:
            report.add_issue(
                FILE_HEADER, 'not a FileStorage data file', 0)
            report.scanned_from = report.scanned_to = 0
            return report

        pos = 4
        last_tid = None
        last_tlen = None
        if checkpoint is not None:
            _check_checkpoint(f, st, checkpoint, magic)
            pos = checkpoint.pos
            last_tid = checkpoint.tid
            last_tlen = checkpoint.tlen

        report.scanned_from = pos
        boundary = pos

        def file_changed():
            return os.fstat(f.fileno()).st_size != size

        while True:
            raw = _read_at(f, pos, TRANS_HDR_LEN)
            if len(raw) != TRANS_HDR_LEN:
                # Clean end of file, or a header that is still being
                # appended.  Either way, this is the boundary.
                break
            try:
                th = TxnHeaderFromString(raw)
            except Exception as e:
                if file_changed():
                    break  # tail is being appended concurrently
                report.add_issue(
                    TXN_HEADER, 'unreadable transaction header: %s' % e,
                    pos, tid=last_tid)
                break
            if th.status == 'c':
                # Checkpoint of a transaction still in progress.
                break
            if th.status not in (' ', 'u', 'p'):
                if file_changed():
                    break
                report.add_issue(
                    TXN_HEADER, 'invalid transaction status: %r'
                    % th.status, pos, tid=th.tid)
                break
            if th.tlen < th.headerlen():
                if file_changed():
                    break
                report.add_issue(
                    TXN_HEADER,
                    'invalid transaction length: %d < header length %d'
                    % (th.tlen, th.headerlen()), pos, tid=th.tid)
                break
            tend = pos + th.tlen
            if tend + 8 > size:
                # The transaction extends past the end of the file:
                # it is still being appended.
                break
            trailer = u64(_read_at(f, tend, 8))
            if trailer != th.tlen:
                if file_changed():
                    break
                report.add_issue(
                    TXN_TRAILER,
                    'redundant transaction length check failed: '
                    '%d != %d' % (trailer, th.tlen), tend, tid=th.tid)
                break
            # The transaction is structurally complete and lies
            # inside the boundary; check its records.
            _check_records(f, th, pos, report, blob_helper, is_blob_record)
            boundary = tend + 8
            last_tid = th.tid
            last_tlen = th.tlen
            pos = boundary

        report.scanned_to = boundary
        st = os.fstat(f.fileno())
        report.checkpoint = VerifyCheckpoint(
            pos=boundary, tid=last_tid, tlen=last_tlen,
            file_size=st.st_size, device=st.st_dev, inode=st.st_ino,
            magic=magic)
    return report
