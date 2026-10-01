"""Tests for idempotent, batch-ID based imports."""

import io
import os
import tempfile
import threading

import transaction
from persistent.mapping import PersistentMapping

import ZODB
from ZODB.ExportImport import IMPORT_BATCHES_KEY
from ZODB.FileStorage import FileStorage
from ZODB.POSException import ImportBatchMismatchError
from ZODB.serialize import referencesf
from ZODB.utils import u64
from ZODB.utils import z64
from ZODB.tests.util import TestCase


def reachable_oids(storage):
    """Return the OIDs reachable from the database root oid."""
    seen = set()
    stack = [z64]
    while stack:
        oid = stack.pop()
        if oid in seen:
            continue
        seen.add(oid)
        data, _ = storage.load(oid)
        refs = []
        referencesf(data, refs)
        stack.extend(refs)
    return seen


def make_graph(conn, width=20):
    """Create and commit a small object graph, return its root object."""
    root = conn.root()
    graph = PersistentMapping()
    for i in range(width):
        graph[i] = PersistentMapping({'index': i})
    root['graph'] = graph
    transaction.commit()
    return graph


def export_bytes(conn, obj):
    f = conn.exportFile(obj._p_oid)
    f.seek(0)
    return f.read()


def export_oid_list(blob):
    f = io.BytesIO(blob)
    assert f.read(4) == b'ZEXP'
    oids = []
    while 1:
        header = f.read(16)
        if header == b'\377' * 16:
            break
        oids.append(header[:8])
        f.seek(u64(header[8:16]), 1)
    return oids


class ImportBatchTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self._storage = FileStorage('ImportBatchTests.fs', create=1)
        self._db = ZODB.DB(self._storage)
        self._conn = self._db.open()
        self._graph = make_graph(self._conn)
        self._export = export_bytes(self._conn, self._graph)
        self._export_oids = export_oid_list(self._export)

    def tearDown(self):
        self._db.close()
        TestCase.tearDown(self)

    def _control_db_with_single_import(self):
        path = tempfile.mktemp(suffix='.fs', dir=os.getcwd())
        db = ZODB.DB(FileStorage(path))
        conn = db.open()
        make_graph(conn)
        conn.importFileBatch(io.BytesIO(self._export), 'once')
        conn.close()
        return db

    def test_batch_import_stores_summary_and_mapping(self):
        oid_map = self._conn.importFileBatch(
            io.BytesIO(self._export), 'batch-1', clue='import batch-1')

        self.assertEqual(sorted(oid_map), sorted(self._export_oids))
        self.assertEqual(len(set(oid_map.values())), len(oid_map))

        registry = self._conn.root()[IMPORT_BATCHES_KEY]
        record = registry['batch-1']
        self.assertEqual(record.nobjects, len(self._export_oids))
        self.assertEqual(record.size, len(self._export))
        self.assertEqual(dict(record.oid_map), oid_map)
        self.assertEqual(record.root_oid, oid_map[self._graph._p_oid])

        conn2 = self._db.open()
        try:
            record2 = conn2.root()[IMPORT_BATCHES_KEY]['batch-1']
            self.assertEqual(dict(record2.oid_map), oid_map)
            imported_root = record2.root
            self.assertEqual(len(imported_root), len(self._graph))
            self.assertEqual(imported_root[7]['index'], 7)
        finally:
            conn2.close()

    def test_repeated_batch_returns_original_mapping(self):
        first = self._conn.importFileBatch(io.BytesIO(self._export), 'once')
        second = self._conn.importFileBatch(io.BytesIO(self._export), 'once')
        third = self._conn.importFileBatch(io.BytesIO(self._export), 'once')

        self.assertEqual(first, second)
        self.assertEqual(first, third)

        registry = self._conn.root()[IMPORT_BATCHES_KEY]
        self.assertEqual(list(registry.keys()), ['once'])

        control = self._control_db_with_single_import()
        try:
            self.assertEqual(len(reachable_oids(self._storage)),
                             len(reachable_oids(control.storage)))
        finally:
            control.close()

    def test_batch_id_same_but_content_different_rejected(self):
        self._conn.importFileBatch(io.BytesIO(self._export), 'id')

        different = export_bytes(self._conn, self._graph[0])
        with self.assertRaises(ImportBatchMismatchError):
            self._conn.importFileBatch(io.BytesIO(different), 'id')

        original = self._conn.importFileBatch(
            io.BytesIO(self._export), 'id')
        record = self._conn.root()[IMPORT_BATCHES_KEY]['id']
        self.assertEqual(dict(record.oid_map), original)

    def test_failed_import_leaves_no_marker(self):
        truncated = io.BytesIO(self._export[:len(self._export) // 2])
        with self.assertRaises(Exception):
            self._conn.importFileBatch(truncated, 'broken')

        conn = self._db.open()
        try:
            self.assertIsNone(conn.root().get(IMPORT_BATCHES_KEY))
        finally:
            conn.close()

        mapping = self._db.open().importFileBatch(
            io.BytesIO(self._export), 'broken')
        self.assertEqual(len(mapping), len(self._export_oids))

    def test_import_itself_commits_and_is_visible(self):
        # The batch import manages its own transaction, so a replay from
        # another connection that did not take part sees the marker.
        self._conn.importFileBatch(io.BytesIO(self._export), 'visible')
        other = self._db.open()
        try:
            mapping = other.importFileBatch(
                io.BytesIO(self._export), 'visible')
            self.assertEqual(len(mapping), len(self._export_oids))
        finally:
            other.close()

    def test_different_batches_both_succeed(self):
        one = self._conn.importFileBatch(io.BytesIO(self._export), 'one')
        two = self._conn.importFileBatch(io.BytesIO(self._export), 'two')
        self.assertNotEqual(one, two)
        registry = self._conn.root()[IMPORT_BATCHES_KEY]
        self.assertEqual(set(registry.keys()), {'one', 'two'})

    def test_concurrent_same_batch_single_reachable_graph(self):
        results = {}
        errors = {}
        barrier = threading.Barrier(2)

        def worker(name):
            conn = self._db.open()
            try:
                barrier.wait()
                results[name] = conn.importFileBatch(
                    io.BytesIO(self._export), 'race')
            except Exception as exc:  # pragma: no cover - failure path
                errors[name] = exc
            finally:
                conn.close()

        threads = [
            threading.Thread(target=worker, args=('a',)),
            threading.Thread(target=worker, args=('b',)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertFalse(errors, errors)
        self.assertEqual(results['a'], results['b'])
        self.assertEqual(len(results['a']), len(self._export_oids))

        control = self._control_db_with_single_import()
        try:
            self.assertEqual(len(reachable_oids(self._storage)),
                             len(reachable_oids(control.storage)))
        finally:
            control.close()

        registry = self._db.open().root()[IMPORT_BATCHES_KEY]
        self.assertEqual(list(registry.keys()), ['race'])

    def test_concurrent_different_content_rejected(self):
        other_export = export_bytes(self._conn, self._graph[0])
        results = {}
        barrier = threading.Barrier(2)

        def worker(name, payload):
            conn = self._db.open()
            try:
                barrier.wait()
                conn.importFileBatch(io.BytesIO(payload), 'race')
                results[name] = 'ok'
            except ImportBatchMismatchError:
                results[name] = 'mismatch'
            except Exception as exc:  # pragma: no cover - failure path
                results[name] = ('error', repr(exc))
            finally:
                conn.close()

        threads = [
            threading.Thread(target=worker, args=('a', self._export)),
            threading.Thread(target=worker, args=('b', other_export)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertIn('ok', results.values())
        self.assertIn('mismatch', results.values())
        registry = self._db.open().root()[IMPORT_BATCHES_KEY]
        self.assertEqual(len(registry), 1)
