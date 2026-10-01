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
"""Read-only previews of the changes a connection will write.

The :class:`~ZODB.Connection.Connection` method
:meth:`~ZODB.Connection.Connection.change_preview` computes the set of
object records that the current transaction would write, *without*
starting two-phase commit, clearing any dirty flags, or permanently
assigning object ids.

New objects that have not been assigned a stable oid are reported with
:class:`PreviewOID` instances.  These identities are only valid within a
single preview: rerunning a preview may assign different identities and a
real commit allocates the actual oids.
"""

from persistent import Persistent
from persistent.wref import WeakRef
from persistent.wref import WeakRefMarker
from zodbpickle import binary

from ZODB.blob import Blob
from ZODB.POSException import ConnectionStateError
from ZODB.POSException import InvalidObjectReference
from ZODB.serialize import ObjectWriter
from ZODB.utils import get_pickle_metadata
from ZODB.utils import p64
from ZODB.utils import z64


class PreviewOID(bytes):
    """A temporary object identity used only inside one change preview.

    Instances behave like regular 8-byte oids so that previews can
    pickle references between not-yet-stored objects exactly as a real
    commit would.  They are never written to a storage and the objects
    they were temporarily assigned to lose them as soon as the preview
    has been computed.

    ``token`` is a small, human-readable integer (starting at 1) that is
    stable for the lifetime of the preview; equal tokens mean the same
    not-yet-stored object.
    """

    def __new__(cls, token):
        self = super().__new__(cls, p64(token))
        self.token = token
        return self

    def __repr__(self):
        return f"PreviewOID({self.token})"


class ObjectChange(tuple):
    """Description of a single object record a commit would write.

    Attributes:

    oid
        The object's oid.  For objects that do not have a stable oid yet
        (objects discovered implicitly rather than added through
        :meth:`~ZODB.interfaces.IConnection.add`) this is a
        :class:`PreviewOID`.

    class_name
        Dotted ``module.qualname`` of the object's class.

    klass
        The object's class, or ``None`` if the class could not be
        resolved (for example for records checkpointed in a savepoint
        whose class can no longer be imported).

    old_serial
        The revision the record is based on (``_p_serial``); ``z64`` for
        new objects.

    new_size
        The size in bytes of the serialized record that would be
        written.

    blob_changed
        True if the object is a blob whose payload would be (re)written.
    """

    __slots__ = ()

    def __new__(cls, oid, class_name, klass, old_serial, new_size,
                blob_changed):
        return tuple.__new__(cls, (oid, class_name, klass, old_serial,
                                   new_size, blob_changed))

    oid = property(lambda self: self[0])
    class_name = property(lambda self: self[1])
    klass = property(lambda self: self[2])
    old_serial = property(lambda self: self[3])
    new_size = property(lambda self: self[4])
    blob_changed = property(lambda self: self[5])

    def __repr__(self):
        return (f"ObjectChange(oid={self.oid!r}, class_name="
                f"{self.class_name!r}, old_serial={self.old_serial!r}, "
                f"new_size={self.new_size}, "
                f"blob_changed={self.blob_changed!r})")


def _class_name(klass):
    name = getattr(klass, "__qualname__", None) or klass.__name__
    return f"{klass.__module__}.{name}"


class _PreviewIdentityAllocator:
    """Allocates 8-byte preview identities that cannot be confused with
    any real oid reachable from the connection.

    Preview identities temporarily occupy ``_p_oid`` while records are
    serialized, which is what gives previews byte-identical reference
    encoding (and therefore exact record sizes) compared with a real
    commit.  Because the bytes act as ordinary oids, a preview token
    must never equal an oid that an existing object already has; the
    allocator seeds itself with every oid reachable from the connection
    cache, its explicitly added objects, and any savepoint storage.
    """

    def __init__(self, connection):
        taken = set(connection._cache.cache_data)
        taken.update(connection._added)
        taken.update(connection._readCurrent)
        savepoint_storage = connection._savepoint_storage
        if savepoint_storage is not None:
            taken.update(savepoint_storage.index)
            taken.update(savepoint_storage.creating)
        self._taken = taken
        self._next = 0

    def new_identity(self):
        taken = self._taken
        while True:
            self._next += 1
            oid = PreviewOID(self._next)
            if oid not in taken:
                # Reserve it so a second new object never reuses it and
                # so it can't collide with a later existing object.
                taken.add(oid)
                return oid


class _PreviewObjectWriter(ObjectWriter):
    """Object writer that hands out temporary oids during a preview.

    The default :class:`~ZODB.serialize.ObjectWriter` allocates real
    oids from the connection and mutates ``_p_oid``/``_p_jar`` of
    objects it reaches.  During a preview we allocate
    :class:`PreviewOID` values instead and record every object that
    receives one so the connection can restore the original state.
    """

    def __init__(self, obj, allocated, allocator):
        ObjectWriter.__init__(self, obj)
        self._allocated = allocated
        self._allocator = allocator

    def _allocate_oid(self, obj):
        oid = self._allocator.new_identity()
        obj._p_oid = oid
        obj._p_jar = self._jar
        self._allocated.append(obj)
        self._stack.append(obj)
        return oid

    def persistent_id(self, obj):
        # Mirrors ObjectWriter.persistent_id, but new objects receive
        # preview oids and nothing is permanently attached to them.
        if not isinstance(obj, (Persistent, type, WeakRef)):
            return None

        try:
            oid = obj._p_oid
        except AttributeError:
            return None

        if not (oid is None or isinstance(oid, bytes)):
            if hasattr(oid, '__get__'):
                return None

            if oid is WeakRefMarker:
                oid = obj.oid
                if oid is None:
                    target = obj()
                    oid = target._p_oid
                    if oid is None:
                        oid = self._allocate_oid(target)
                    obj.oid = oid
                    obj.dm = target._p_jar
                    obj.database_name = obj.dm.db().database_name

                oid = binary(oid)
                if obj.dm is self._jar:
                    return ['w', (oid, )]
                return ['w', (oid, obj.database_name)]

        database_name = None

        if oid is None:
            oid = self._allocate_oid(obj)
        elif obj._p_jar is not self._jar:
            if not self._jar.db().xrefs:
                raise InvalidObjectReference(
                    "Database %r doesn't allow implicit cross-database "
                    "references" % self._jar.db().database_name,
                    self._jar, obj)

            otherdb = obj._p_jar.db()
            database_name = otherdb.database_name

            if self._jar.db().databases.get(database_name) is not otherdb:
                raise InvalidObjectReference(
                    "Attempt to store an object from a foreign "
                    "database connection", self._jar, obj,
                )

            if self._jar.get_connection(database_name) is not obj._p_jar:
                raise InvalidObjectReference(
                    "Attempt to store a reference to an object from "
                    "a separate connection to the same database or "
                    "multidatabase", self._jar, obj,
                )

            if obj._p_jar._implicitlyAdding(oid):
                raise InvalidObjectReference(
                    "A new object is reachable from multiple databases. "
                    "Won't try to guess which one was correct!",
                    self._jar, obj,
                )

        oid = binary(oid)
        klass = type(obj)
        if hasattr(klass, '__getnewargs__'):
            if database_name is not None:
                return ['n', (database_name, oid)]

            return oid

        if database_name is not None:
            return ['m', (database_name, oid, klass)]

        return oid, klass


def change_preview(connection):
    """Return the records *connection* would write if it committed now.

    See :meth:`ZODB.interfaces.IConnection.change_preview` for the
    contract.  This function performs a dry run of the connection's
    commit logic and then restores every piece of state it touched.
    """
    if connection.opened is None:
        raise ConnectionStateError("The database connection is closed")

    # State snapshots that must be restored no matter how the dry run
    # ends.  Implicitly discovered new objects are remembered via the
    # writer's allocated list.
    saved_registered = connection._registered_objects
    saved_added_during_commit = connection._added_during_commit
    saved_needs_to_join = connection._needs_to_join

    # The serializer calls _add() for objects added as a side effect of
    # __getstate__.  Redirect those into a temporary list, just like a
    # real commit does.
    added_during_commit = []
    connection._added_during_commit = added_during_commit
    # The dry run must not join the transaction manager.
    connection._needs_to_join = True
    # A private registered list keeps connection._registered_objects
    # (the dirty set) untouched.
    connection._registered_objects = list(saved_registered)

    allocated = []
    allocator = _PreviewIdentityAllocator(connection)
    # oid -> record for everything that would be written
    records = {}

    savepoint_storage = connection._savepoint_storage
    creating = set(connection._creating)
    if savepoint_storage is not None:
        creating.update(savepoint_storage.creating)

        # Everything already checkpointed into a savepoint is part of
        # the write set; records produced below override these entries
        # for objects modified after the savepoint.
        for oid in savepoint_storage.index:
            data, serial = savepoint_storage.load(oid)
            module, name = get_pickle_metadata(data)
            is_blob = isinstance(connection._reader.getGhost(data), Blob)
            records[oid] = ObjectChange(
                oid,
                f"{module}.{name}" if module else name,
                None,
                serial,
                len(data),
                is_blob,
            )

    modified = []
    try:
        def store_objects(writer):
            for obj in writer:
                oid = obj._p_oid
                serial = getattr(obj, "_p_serial", z64)

                if ((serial == z64)
                        and
                        ((savepoint_storage is None)
                         or (oid not in savepoint_storage.creating)
                         or savepoint_storage.creating[oid])):
                    # Mirrors Connection._store_objects: the object is
                    # new in the part of the transaction being previewed.
                    creating.add(oid)
                else:
                    modified.append(oid)

                data = writer.serialize(obj)

                blob_changed = True
                if isinstance(obj, Blob):
                    if obj.opened():
                        raise ValueError("Can't commit with opened blobs.")
                    blobfilename = obj._p_blob_uncommitted
                    if blobfilename is None and serial != z64:
                        # Mirrors the ``self._modified.pop()`` skip for
                        # existing blobs without new payload data.
                        modified.pop()
                        continue
                else:
                    blob_changed = False

                records[oid] = ObjectChange(
                    oid,
                    _class_name(type(obj)),
                    type(obj),
                    serial,
                    len(data),
                    blob_changed,
                )

        for obj in list(connection._registered_objects):
            oid = obj._p_oid
            if oid is None:
                continue
            if obj._p_jar is not connection:
                raise InvalidObjectReference(obj, obj._p_jar)
            if oid in connection._added:
                assert obj._p_serial == z64
            elif oid in creating or not obj._p_changed:
                continue

            store_objects(_PreviewObjectWriter(obj, allocated, allocator))

        for obj in list(added_during_commit):
            store_objects(_PreviewObjectWriter(obj, allocated, allocator))
    finally:
        connection._registered_objects = saved_registered
        connection._added_during_commit = saved_added_during_commit
        connection._needs_to_join = saved_needs_to_join

        # Every object in *allocated* was reached implicitly (including
        # weakref targets); it had neither oid nor jar before the dry
        # run.  Objects explicitly added through add() received real
        # oids and never entered this list, so their identities stick.
        for obj in allocated:
            obj._p_oid = None
            obj._p_jar = None

    # Stable ordering makes repeated previews easy to diff: records
    # with a real oid first (sorted by oid), followed by records whose
    # identity only exists within this preview (sorted by token).
    real_oids = sorted(oid for oid in records
                       if not isinstance(oid, PreviewOID))
    preview_oids = sorted((oid for oid in records
                           if isinstance(oid, PreviewOID)),
                          key=lambda oid: oid.token)
    return [records[oid] for oid in real_oids + preview_oids]
