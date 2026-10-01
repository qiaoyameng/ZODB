##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Support for database export and import."""

import hashlib
import logging
import os
import time
from io import BytesIO
from tempfile import TemporaryFile

from BTrees.OOBTree import OOBTree
from persistent import Persistent

from ZODB._compat import PersistentPickler
from ZODB._compat import Unpickler
from ZODB._compat import _protocol
from ZODB.blob import Blob
from ZODB.interfaces import IBlobStorage
from ZODB.POSException import ConflictError
from ZODB.POSException import ExportError
from ZODB.POSException import ImportBatchMismatchError
from ZODB.serialize import referencesf
from ZODB.utils import cp
from ZODB.utils import mktemp
from ZODB.utils import p64
from ZODB.utils import u64


logger = logging.getLogger('ZODB.ExportImport')

#: Name of the root-object key holding the registry of idempotent
#: import batches.
IMPORT_BATCHES_KEY = '_zodb_import_batches'

#: Maximum number of times an idempotent import is retried while the
#: storage reports write conflicts caused by concurrent imports.
IMPORT_BATCH_MAX_ATTEMPTS = 10


class ImportBatchRecord(Persistent):
    """Persistent record describing a completed idempotent import.

    A record is stored in the batch registry in the *same* transaction as
    the imported object graph.  It therefore either becomes visible
    together with the graph or not at all.

    ``digest`` is a hex digest of the exact export stream that was
    imported; reusing the batch ID with a different digest is rejected.
    ``oid_map`` maps the OIDs of the exported objects to the OIDs
    allocated in this database.  ``root_oid`` is the OID of the root of
    the imported graph and a ghost of that object is also held as a
    direct reference, so the graph stays reachable from the database
    root.
    """

    def __init__(self, digest, oid_map, root_oid, root_obj=None,
                 digest_name='sha256', size=0, nobjects=0):
        self.digest_name = digest_name
        self.digest = digest
        self.oid_map = OOBTree()
        for old_oid, new_oid in oid_map.items():
            if isinstance(new_oid, tuple):
                new_oid = new_oid[0]
            self.oid_map[old_oid] = new_oid
        self.root_oid = root_oid
        self.root = root_obj
        self.size = size
        self.nobjects = nobjects
        self.ctime = time.time()


def _plain_oid(oid):
    if isinstance(oid, tuple):
        return oid[0]
    return oid


class ExportImport:

    def exportFile(self, oid, f=None, bufsize=64 * 1024):
        if f is None:
            f = TemporaryFile(prefix="EXP")
        elif isinstance(f, str):
            f = open(f, 'w+b')
        f.write(b'ZEXP')
        oids = [oid]
        done_oids = {}
        load = self._storage.load
        supports_blobs = IBlobStorage.providedBy(self._storage)
        while oids:
            oid = oids.pop(0)
            if oid in done_oids:
                continue
            done_oids[oid] = True
            try:
                p, serial = load(oid)
            except:  # noqa: E722 do not use bare 'except'
                logger.debug("broken reference for oid %s", repr(oid),
                             exc_info=True)
            else:
                referencesf(p, oids)
                f.writelines([oid, p64(len(p)), p])

                if supports_blobs:
                    if not isinstance(self._reader.getGhost(p), Blob):
                        continue  # not a blob

                    blobfilename = self._storage.loadBlob(oid, serial)
                    f.write(blob_begin_marker)
                    f.write(p64(os.stat(blobfilename).st_size))
                    blobdata = open(blobfilename, "rb")
                    cp(blobdata, f, bufsize=bufsize)
                    blobdata.close()

        f.write(export_end_marker)
        return f

    def _readExportRecords(self, f, materialize_blobs=False):
        """Yield the records of an export stream.

        Each yielded item is a dict with ``oid`` (the exported OID),
        ``data`` (the source pickle) and, for blob objects,
        ``blob_filename`` (``None`` when the object is not a blob or
        blobs are not materialized).

        The stream must be positioned directly after the ``ZEXP`` header.
        """
        supports_blobs = IBlobStorage.providedBy(self._storage)
        while 1:
            header = f.read(16)
            if header == export_end_marker:
                break
            if len(header) != 16:
                raise ExportError("Truncated export file")

            ooid = header[:8]
            length = u64(header[8:16])
            data = f.read(length)
            if len(data) != length:
                raise ExportError("Truncated export file")

            blob_filename = None
            if supports_blobs and (b'blob' in data and
                                   isinstance(self._reader.getGhost(data),
                                              Blob)):
                if f.read(len(blob_begin_marker)) != blob_begin_marker:
                    raise ValueError("No data for blob object")
                blob_len = u64(f.read(8))
                if materialize_blobs:
                    blob_filename = mktemp(self._storage.temporaryDirectory())
                    blob_file = open(blob_filename, "wb")
                    try:
                        cp(f, blob_file, blob_len)
                    finally:
                        blob_file.close()
                else:
                    f.seek(blob_len, 1)

            yield {'oid': ooid, 'data': data,
                   'blob_filename': blob_filename}

    def importFile(self, f, clue='', customImporters=None):
        # This is tricky, because we need to work in a transaction!

        if isinstance(f, str):
            with open(f, 'rb') as fp:
                return self.importFile(fp, clue=clue,
                                       customImporters=customImporters)

        magic = f.read(4)
        if magic != b'ZEXP':
            if customImporters and magic in customImporters:
                f.seek(0)
                return customImporters[magic](self, f, clue)
            raise ExportError("Invalid export header")

        t = self.transaction_manager.get()
        if clue:
            t.note(clue)

        return_oid_list = []
        self._import = f, return_oid_list
        self._register()
        t.savepoint(optimistic=True)
        # Return the root imported object.
        if return_oid_list:
            return self.get(return_oid_list[0])
        else:
            return None

    def importFileBatch(self, f, batch_id, clue=''):
        """Import an export stream exactly once for ``batch_id``.

        The export stream ``f`` is imported idempotently: the same
        ``batch_id`` always returns the mapping of the *original*
        successful import, while reusing ``batch_id`` with different
        content raises
        :class:`~ZODB.POSException.ImportBatchMismatchError`.

        The batch summary, the mapping of exported OIDs to newly
        allocated OIDs and the imported object graph are all written in
        a single transaction.  If the import fails or the transaction is
        aborted, no "already imported" marker is left behind.

        Concurrent imports of the same batch converge through the
        storage's conflict mechanism (the batch registry is a
        persistent BTree), not through in-process state.

        Returns a dict mapping each exported OID to the OID allocated
        for it in this database.
        """
        if isinstance(f, str):
            with open(f, 'rb') as fp:
                return self.importFileBatch(fp, batch_id, clue=clue)

        # Spool the stream so it can be re-read on conflict retries and
        # fingerprint the exact bytes before anything is written.
        spool = TemporaryFile(prefix="BATCHEXP")
        try:
            hasher = hashlib.sha256()
            size = 0
            while 1:
                chunk = f.read(64 * 1024)
                if not chunk:
                    break
                spool.write(chunk)
                hasher.update(chunk)
                size += len(chunk)
            digest = hasher.hexdigest()

            spool.seek(0)
            magic = spool.read(4)
            if magic != b'ZEXP':
                raise ExportError("Invalid export header")

            for attempt in range(IMPORT_BATCH_MAX_ATTEMPTS):
                try:
                    return self._importFileBatchAttempt(
                        spool, batch_id, digest, size, clue)
                except ConflictError:
                    if attempt + 1 >= IMPORT_BATCH_MAX_ATTEMPTS:
                        raise
        finally:
            spool.close()

    def _importFileBatchAttempt(self, spool, batch_id, digest, size, clue):
        tm = self.transaction_manager
        t = tm.get()
        if clue:
            t.note(clue)

        try:
            root = self.root()
            registry = root.get(IMPORT_BATCHES_KEY)
            existing = None if registry is None else registry.get(batch_id)
            if existing is not None:
                # Idempotent replay: return the original mapping.
                if existing.digest != digest:
                    raise ImportBatchMismatchError(
                        "Batch ID %r was already imported with different "
                        "content (%s != %s)" % (
                            batch_id, existing.digest, digest))
                return dict(existing.oid_map)

            # Determine the OID mapping up front (OIDs are allocated by
            # the storage, not by the transaction) and remember the
            # first object as the root of the imported graph.
            spool.seek(4)
            oid_map = {}
            root_oid = None
            root_data = None
            nobjects = 0
            for record in self._readExportRecords(spool):
                old_oid = record['oid']
                new_oid = self._storage.new_oid()
                oid_map[old_oid] = new_oid
                if root_oid is None:
                    root_oid = new_oid
                    root_data = record['data']
                nobjects += 1

            if registry is None:
                registry = OOBTree()
                root[IMPORT_BATCHES_KEY] = registry

            # Hold a direct persistent reference to the imported root so
            # the whole graph is reachable from the database root.
            root_obj = None
            if root_oid is not None:
                root_obj = self._reader.getGhost(root_data)
                self._cache.new_ghost(root_oid, root_obj)

            registry[batch_id] = ImportBatchRecord(
                digest, oid_map, root_oid, root_obj=root_obj,
                size=size, nobjects=nobjects)

            return_oid_list = []
            # _importDuringCommit runs during commit, in the same
            # transaction as the registry update.
            spool.seek(4)
            self._import = spool, return_oid_list, oid_map
            self._register()
            t.savepoint(optimistic=True)
            tm.commit()
            return dict(oid_map)
        except ImportBatchMismatchError:
            tm.abort()
            raise
        except BaseException:
            tm.abort()
            raise

    def _importDuringCommit(self, transaction, f, return_oid_list,
                            oid_map=None):
        """Import data during two-phase commit.

        Invoked by the transaction manager mid commit.
        Appends one item, the OID of the first object created,
        to return_oid_list.
        """
        oids = {}
        if oid_map is not None:
            for old_oid, new_oid in oid_map.items():
                oids[old_oid] = new_oid

        # IMPORTANT: This code should be consistent with the code in
        # serialize.py. It is currently out of date and doesn't handle
        # weak references.

        def persistent_load(ooid):
            """Remap a persistent id to a new ID and create a ghost for it."""

            klass = None
            if isinstance(ooid, tuple):
                ooid, klass = ooid

            if not isinstance(ooid, bytes):
                assert isinstance(ooid, str)
                # this happens when all bytes in the oid are < 0x80
                ooid = ooid.encode('ascii')

            if ooid in oids:
                oid = oids[ooid]
            else:
                if klass is None:
                    oid = self._storage.new_oid()
                else:
                    oid = self._storage.new_oid(), klass
                oids[ooid] = oid

            return Ghost(oid)

        first = True
        for record in self._readExportRecords(f, materialize_blobs=True):
            ooid = record['oid']
            data = record['data']
            blob_filename = record['blob_filename']

            oid = oids.get(ooid)
            if oid is None:
                oid = self._storage.new_oid()
                oids[ooid] = oid
            store_oid = _plain_oid(oid)
            if first:
                return_oid_list.append(store_oid)
                first = False

            pfile = BytesIO(data)
            unpickler = Unpickler(pfile)
            unpickler.persistent_load = persistent_load

            newp = BytesIO()
            pickler = PersistentPickler(persistent_id, newp, _protocol)

            pickler.dump(unpickler.load())
            pickler.dump(unpickler.load())
            data = newp.getvalue()

            if blob_filename is not None:
                self._storage.storeBlob(store_oid, None, data, blob_filename,
                                        '', transaction)
            else:
                self._storage.store(store_oid, None, data, '', transaction)


export_end_marker = b'\377' * 16
blob_begin_marker = b'\000BLOBSTART'


class Ghost:
    __slots__ = ("oid",)

    def __init__(self, oid):
        self.oid = oid


def persistent_id(obj):
    if isinstance(obj, Ghost):
        return obj.oid
