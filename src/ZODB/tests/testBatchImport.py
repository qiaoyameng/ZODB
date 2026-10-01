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
"""Tests for idempotent, batch-identified imports."""

import os
import threading
from io import BytesIO

import transaction
from persistent.mapping import PersistentMapping

import ZODB
from ZODB.BatchImport import BatchImportRecord
from ZODB.BatchImport import BatchMismatchError
from ZODB.BatchImport import REGISTRY_KEY
from ZODB.BatchImport import batch_digest
from ZODB.BatchImport import import_batch
from ZODB.FileStorage import FileStorage
from ZODB.POSException import ConflictError
from ZODB.POSException import ExportError
from ZODB.serialize import referencesf
from ZODB.tests.util import TestCase
from ZODB.utils import u64
from ZODB.utils import z64


def make_graph(size=5):
    """Return a small persistent object graph (a chain of mappings)."""
    root = PersistentMapping()
    cursor = root
    for i in range(size):
        child = PersistentMapping(index=i)
        cursor["child"] = child
        cursor["side_%d" % i] = PersistentMapping(leaf=i)
        cursor = child
    return root


def count_export_records(data):
    count = 0
    pos = 4
    end = b"\377" * 16
    while pos < len(data):
        header = data[pos:pos + 16]
        if header == end:
            break
        count += 1
        pos += 16 + u64(header[8:16])
    return count


def reachable_oids(storage):
    """Return the OIDs reachable from the root OID in ``storage``."""
    reachable = set()
    stack = [z64]
    while stack:
        oid = stack.pop()
        if oid in reachable:
            continue
        reachable.add(oid)
        pickle, _serial = storage.load(oid)
        stack.extend(referencesf(pickle))
    return reachable


class BatchImportTestBase(TestCase):

    def setUp(self):
        super().setUp()
        # Absolute paths so any index side files are always written
        # here in the temporary directory, regardless of cwd changes
        # during teardown.
        self._source_db = ZODB.DB(
            FileStorage(os.path.abspath("Source.fs"), create=True))
        self._storage = FileStorage(
            os.path.abspath("Destination.fs"), create=True)
        self._db = ZODB.DB(self._storage)

    def tearDown(self):
        self._source_db.close()
        self._db.close()
        super().tearDown()

    def _export_bytes(self, obj=None, commit=True):
        conn = self._source_db.open()
        export = None
        try:
            if obj is None:
                obj = make_graph()
                conn.root()["graph"] = obj
                transaction.commit()
            export = conn.exportFile(obj._p_oid)
            source_root_oid = obj._p_oid
            export.seek(0)
            data = export.read()
        finally:
            conn.close()
            if export is not None:
                export.close()
        return data, source_root_oid

    def _fresh_export_bytes(self, size=5):
        """Export a graph built in a dedicated source storage."""
        storage = FileStorage(
            os.path.abspath("Source-%d.fs" % size), create=True)
        source_db = ZODB.DB(storage)
        try:
            conn = source_db.open()
            export = None
            try:
                obj = make_graph(size)
                conn.root()["graph"] = obj
                transaction.commit()
                export = conn.exportFile(obj._p_oid)
                source_root_oid = obj._p_oid
                export.seek(0)
                data = export.read()
            finally:
                conn.close()
                if export is not None:
                    export.close()
        finally:
            source_db.close()
        return data, source_root_oid


class BatchImportTests(BatchImportTestBase):

    def test_import_returns_record_with_mapping(self):
        data, source_oid = self._export_bytes()
        conn = self._db.open()
        try:
            record = import_batch(conn, "b1", BytesIO(data))
            self.assertIsInstance(record, BatchImportRecord)
            self.assertEqual(record.root_oid, record.root._p_oid)
            self.assertIn(source_oid, record.oid_map)
            self.assertEqual(record.oid_map[source_oid], record.root_oid)
            self.assertEqual(len(record.oid_map),
                             count_export_records(data))
            self.assertEqual(record.root["child"]["index"], 0)
            leaf = record.root
            for _ in range(5):  # traverse the full chain (indexes 0..4)
                leaf = leaf["child"]
            self.assertEqual(leaf["index"], 4)
        finally:
            conn.close()

    def test_repeat_same_batch_returns_same_mapping(self):
        data, _oid = self._export_bytes()
        conn = self._db.open()
        try:
            first = import_batch(conn, "b1", BytesIO(data))
            total_after_first = len(self._storage)
            second = import_batch(conn, "b1", BytesIO(data))
            self.assertEqual(second.root_oid, first.root_oid)
            self.assertEqual(second.oid_mapping(), first.oid_mapping())
            self.assertEqual(second.digest, first.digest)
            # Nothing was copied a second time.
            self.assertEqual(len(self._storage), total_after_first)
        finally:
            conn.close()

    def test_repeat_seen_from_another_connection(self):
        data, _oid = self._export_bytes()
        writer = self._db.open()
        import_batch(writer, "b1", BytesIO(data))
        writer.close()

        reader = self._db.open()
        try:
            record = import_batch(reader, "b1", BytesIO(data))
            registry = reader.root()[REGISTRY_KEY]
            self.assertEqual(record.root_oid, registry["b1"].root_oid)
            self.assertEqual(record.root["child"]["index"], 0)
        finally:
            reader.close()

    def test_same_id_different_content_rejected(self):
        data1, _oid = self._fresh_export_bytes(3)
        data2, _oid = self._fresh_export_bytes(4)
        self.assertNotEqual(batch_digest(BytesIO(data1)),
                            batch_digest(BytesIO(data2)))

        conn = self._db.open()
        try:
            first = import_batch(conn, "b1", BytesIO(data1))
            total = len(self._storage)
            with self.assertRaises(BatchMismatchError):
                import_batch(conn, "b1", BytesIO(data2))
            registry = conn.root()[REGISTRY_KEY]
            self.assertEqual(registry["b1"].digest, first.digest)
            self.assertEqual(list(registry.keys()), ["b1"])
            self.assertEqual(len(self._storage), total)
        finally:
            conn.close()

    def test_distinct_batches_coexist(self):
        data1, _oid = self._fresh_export_bytes(2)
        data2, _oid = self._fresh_export_bytes(3)
        conn = self._db.open()
        try:
            r1 = import_batch(conn, "b1", BytesIO(data1))
            r2 = import_batch(conn, "b2", BytesIO(data2))
            self.assertNotEqual(r1.root_oid, r2.root_oid)
            registry = conn.root()[REGISTRY_KEY]
            self.assertEqual(set(registry.keys()), {"b1", "b2"})
        finally:
            conn.close()

    def test_invalid_header_rejected_before_join(self):
        conn = self._db.open()
        try:
            with self.assertRaises(ExportError):
                import_batch(conn, "b1", BytesIO(b"NOPE"))
            self.assertNotIn(REGISTRY_KEY, conn.root())
            # Connection is untouched and still commits normally.
            conn.root()["after"] = 1
            transaction.commit()
            self.assertNotIn(REGISTRY_KEY, conn.root())
        finally:
            transaction.abort()
            conn.close()

    def test_truncated_stream_leaves_no_marker(self):
        data, _oid = self._export_bytes()
        conn = self._db.open()
        try:
            with self.assertRaises(ExportError):
                import_batch(conn, "bad", BytesIO(data[:-40]))
            self.assertNotIn("bad",
                             conn.root().get(REGISTRY_KEY, {}))
            # Connection remains usable for a subsequent good import.
            record = import_batch(conn, "good", BytesIO(data))
            self.assertEqual(record.root["child"]["index"], 0)
        finally:
            conn.close()

    def test_conflict_retry_exhaustion_is_raised(self):
        data, _oid = self._export_bytes()
        conn = self._db.open()
        original_commit = conn.transaction_manager.commit

        def always_conflicting():
            raise ConflictError("forced")

        try:
            conn.transaction_manager.commit = always_conflicting
            with self.assertRaises(ConflictError):
                import_batch(conn, "b1", BytesIO(data), retries=3)
        finally:
            conn.transaction_manager.commit = original_commit
            conn.close()

        # Nothing was committed despite the failed attempts.
        verifier = self._db.open()
        try:
            self.assertNotIn(REGISTRY_KEY, verifier.root())
        finally:
            verifier.close()

    def test_explicit_abort_leaves_no_marker(self):
        data, _oid = self._export_bytes()
        conn = self._db.open()
        try:
            # Simulate a lost response: the application aborts an
            # in-flight import instead of committing it.
            root_obj, _oid_map = conn.importFileWithOIDMap(BytesIO(data))
            conn.root()["would_be"] = root_obj
            transaction.abort()

            # Nothing reachable was written; importing the same batch
            # afterwards starts from scratch.
            self.assertNotIn("would_be", conn.root())
            record = import_batch(conn, "b1", BytesIO(data))
            self.assertEqual(record.root["child"]["index"], 0)
            registry = conn.root()[REGISTRY_KEY]
            self.assertEqual(list(registry.keys()), ["b1"])
        finally:
            conn.close()

    def test_summary_and_mapping_committed_together(self):
        data, _oid = self._export_bytes()
        conn = self._db.open()
        try:
            record = import_batch(conn, "b1", BytesIO(data))
            registry = conn.root()[REGISTRY_KEY]
            self.assertIs(registry["b1"], record)
            self.assertEqual(registry["b1"].root._p_oid,
                             record.root_oid)
            for dest_oid in record.oid_map.values():
                self._storage.load(dest_oid)
        finally:
            conn.close()

    def test_repeated_import_results_in_single_reachable_graph(self):
        data, _oid = self._export_bytes()
        conn = self._db.open()
        try:
            record = import_batch(conn, "b1", BytesIO(data))
            import_batch(conn, "b1", BytesIO(data))
            import_batch(conn, "b1", BytesIO(data))

            reachable = reachable_oids(self._storage)
            graph_oids = set(record.oid_map.values())
            self.assertTrue(graph_oids <= reachable)
            # No orphaned duplicate objects from repeats.
            self.assertEqual(len(self._storage), len(reachable))
        finally:
            conn.close()


class ConcurrentBatchImportTests(BatchImportTestBase):
    """Two independent connections racing on one FileStorage."""

    def test_two_connections_converge_to_one_graph(self):
        data, source_oid = self._export_bytes()

        results = {}
        errors = {}

        # Deterministic interleaving:
        #   1. Both connections establish an MVCC snapshot before
        #      either transaction writes.
        #   2. Worker A imports and commits the batch.
        #   3. Worker B imports against its stale snapshot; its commit
        #      collides on the root OID, loses the storage conflict,
        #      aborts, retries, and returns A's already-committed
        #      mapping.
        both_started = threading.Barrier(2)
        a_committed = threading.Event()

        # Two independent DBs sharing the same physical storage, like
        # two clients connected to the same FileStorage file.
        second_db = ZODB.DB(self._storage)
        self.addCleanup(second_db.close)

        def worker(name, db, result_slot):
            try:
                conn = db.open()
                try:
                    # Take an MVCC snapshot before any commit happens.
                    conn.root()
                    both_started.wait(timeout=30)

                    conflict_wins = {"count": 0}
                    if name == "a":
                        record = import_batch(conn, "race-batch",
                                              BytesIO(data), retries=30)
                        a_committed.set()
                    else:
                        self.assertTrue(a_committed.wait(timeout=30))
                        manager = conn.transaction_manager
                        original_commit = manager.commit

                        def counting_commit():
                            try:
                                return original_commit()
                            except ConflictError:
                                conflict_wins["count"] += 1
                                raise
                        manager.commit = counting_commit
                        record = import_batch(conn, "race-batch",
                                              BytesIO(data), retries=30)
                        # The losing client really did hit a storage
                        # conflict and converged via retry.
                        self.assertGreaterEqual(conflict_wins["count"], 1)

                    result_slot.append((record.root_oid,
                                        record.oid_mapping(),
                                        record.digest))
                finally:
                    conn.close()
            except BaseException as exc:  # pragma: no cover
                errors[name] = exc

        results_a = []
        results_b = []

        t1 = threading.Thread(
            target=worker, args=("a", self._db, results_a))
        t2 = threading.Thread(
            target=worker, args=("b", second_db, results_b))
        t1.start()
        t2.start()
        t1.join(30)
        t2.join(30)
        self.assertFalse(t1.is_alive())
        self.assertFalse(t2.is_alive())
        self.assertEqual(errors, {})

        results["a"] = results_a[0]
        results["b"] = results_b[0]

        oid_a, map_a, digest_a = results["a"]
        oid_b, map_b, digest_b = results["b"]
        self.assertEqual(oid_a, oid_b)
        self.assertEqual(map_a, map_b)
        self.assertEqual(digest_a, digest_b)
        self.assertEqual(map_a[source_oid], oid_a)

        # Exactly one batch record exists and exactly one graph is
        # reachable from the root in the shared storage.
        verifier = self._db.open()
        try:
            registry = verifier.root()[REGISTRY_KEY]
            self.assertEqual(list(registry.keys()), ["race-batch"])
            self.assertEqual(registry["race-batch"].root_oid, oid_a)

            reachable = reachable_oids(self._storage)
            graph_oids = set(map_a.values())
            self.assertTrue(graph_oids <= reachable)
            self.assertEqual(len(self._storage), len(reachable))
            self.assertEqual(
                count_export_records(data),
                sum(1 for oid in reachable if oid in graph_oids))
            for dest_oid in graph_oids:
                self._storage.load(dest_oid)
        finally:
            verifier.close()

def test_suite():
    import unittest
    suite = unittest.TestSuite()
    loader = unittest.defaultTestLoader
    suite.addTest(loader.loadTestsFromTestCase(BatchImportTests))
    suite.addTest(loader.loadTestsFromTestCase(
        ConcurrentBatchImportTests))
    return suite
