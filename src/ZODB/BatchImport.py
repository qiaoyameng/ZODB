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
"""Idempotent, batch-identified cross-database imports.

Retrying an import after a lost response must not copy the object
graph a second time.  ``import_batch`` makes an import exactly-once by
recording the batch summary together with the root OID mapping in the
same transaction that imports the data.  A repeated batch ID returns
the original mapping; a batch ID reused with different content is
rejected.  Concurrent imports of one batch converge through the normal
storage conflict machinery -- no process-local caches are involved.
"""

import hashlib

from BTrees.OOBTree import OOBTree
from persistent import Persistent
from persistent.mapping import PersistentMapping

from ZODB.POSException import ConflictError
from ZODB.POSException import ExportError
from ZODB.utils import u64


# Root key under which the batch registry lives.  It is a persistent
# object reachable from the database root, so its state is shared by
# every connection/process using the same storage.
REGISTRY_KEY = 'zodb.import_batches'

_ZEXP_MAGIC = b'ZEXP'


class BatchMismatchError(ExportError):
    """A batch ID was reused for a batch with different content.

    The batch was not imported.
    """


class BatchImportRecord(Persistent):
    """Result of a completed batch import.

    Attributes:

    ``digest``
        Hex SHA-256 digest of the exact export stream bytes.
    ``root``
        The first (root) imported object.  Keeping the reference makes
        the imported object graph reachable from the database root.
    ``root_oid``
        Destination OID of the root object.
    ``oid_map``
        Mapping of source OIDs to destination OIDs.
    """

    def __init__(self, digest, root, oid_map):
        self.digest = digest
        self.root = root
        self.root_oid = root._p_oid if root is not None else None
        mapping = PersistentMapping()
        mapping.update(oid_map)
        self.oid_map = mapping

    def oid_mapping(self):
        """Return a plain ``dict`` copy of the source->dest OID mapping."""
        return dict(self.oid_map)


def batch_digest(stream):
    """Return the hex SHA-256 digest of an export stream's bytes.

    The stream position is restored afterwards.
    """
    pos = stream.tell()
    digest = hashlib.sha256()
    while True:
        chunk = stream.read(1 << 20)
        if not chunk:
            break
        digest.update(chunk)
    stream.seek(pos)
    return digest.hexdigest()


def _validate_export_stream(data):
    """Raise ``ExportError`` unless ``data`` looks like a ZEXP stream.

    A structural pre-check (magic + at least one complete object
    record) ensures malformed input fails before the connection joins
    a transaction, so no import state is left behind.  The authorita-
    tive parse, including blob handling, still happens during the
    import itself.
    """
    if data[:4] != _ZEXP_MAGIC:
        raise ExportError("Invalid export header")
    end_marker = b'\377' * 16
    size = len(data)
    first_record = data[4:20]
    if len(first_record) != 16 or first_record == end_marker:
        raise ExportError("Empty export file")
    length = u64(first_record[8:16])
    if length == 0 or 20 + length > size:
        raise ExportError("Truncated export file")


def _get_registry(connection, create=False):
    root = connection.root()
    try:
        registry = root[REGISTRY_KEY]
    except KeyError:
        registry = None
    if registry is None and create:
        registry = OOBTree()
        root[REGISTRY_KEY] = registry
    return registry


def import_batch(connection, batch_id, stream, retries=10):
    """Import an export stream under an idempotent ``batch_id``.

    Returns a :class:`BatchImportRecord`.

    * The first call for a ``batch_id`` imports the stream and, in one
      transaction, stores both the batch summary (a digest of the
      stream) and the source-to-destination OID mapping; the imported
      graph is made reachable from the record.
    * A later call with the same ``batch_id`` and identical content
      imports nothing and returns the stored mapping.
    * A later call with the same ``batch_id`` but different content
      raises :class:`BatchMismatchError` and changes nothing.
    * A failed or aborted attempt leaves no "already imported"
      marker behind.
    * Two connections racing to import the same batch converge via
      storage conflicts: one transaction loses, retries, and then sees
      the winner's record.  Coordination relies solely on committed
      storage state, never on process-local data structures.

    ``stream`` must be a seekable binary export stream (as produced by
    ``Connection.exportFile``).  ``retries`` bounds the number of
    conflict-induced attempts.
    """
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError("batch_id must be a non-empty string")

    pos = stream.tell()
    data = stream.read()
    _validate_export_stream(data)
    digest = hashlib.sha256(data).hexdigest()
    stream.seek(pos)

    manager = connection.transaction_manager
    attempts_left = retries
    while attempts_left > 0:
        attempts_left -= 1
        registry = _get_registry(connection, create=False)
        if registry is not None and batch_id in registry:
            record = registry[batch_id]
            if record.digest != digest:
                raise BatchMismatchError(
                    "Batch %r was already imported with different "
                    "content" % (batch_id,))
            return record

        try:
            # Each attempt consumes the stream, so always rewind first.
            stream.seek(pos)
            root_obj, oid_map = connection.importFileWithOIDMap(stream)
            registry = _get_registry(connection, create=True)
            registry[batch_id] = BatchImportRecord(
                digest, root_obj, oid_map)
            manager.commit()
        except Exception as exc:
            # Discard every failed attempt in full -- whether a lost
            # storage conflict or any other error -- so no "already
            # imported" marker can survive.
            manager.abort()
            connection.sync()
            stream.seek(pos)
            if isinstance(exc, ConflictError) and attempts_left > 0:
                # Lost the storage race: retry against a fresh view.
                continue
            raise
        break

    return _get_registry(connection)[batch_id]
