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
"""Transaction-level read-only mode.

Marking a transaction as read-only makes every ZODB connection joined
to it reject writes as they are attempted to be registered, rather than
letting changes accumulate and compensating at commit time:

- registering a modified object raises ``ZODB.POSException.ReadOnlyError``,
- adding a new object raises ``ReadOnlyError``, and
- opening a blob for writing raises ``ReadOnlyError``.

Reading, including activating ghosts, is unaffected, and the restriction
is local to the transaction: other transactions may still write to the
database.  The flag lives on the transaction object, so aborting (or
committing) and beginning the next transaction clears it automatically.
"""

import contextlib

import transaction as _transaction

from ZODB.POSException import ReadOnlyError


_READ_ONLY_ATTRIBUTE = '_zodb_read_only'


def is_read_only(txn):
    """Return whether the transaction *txn* is in read-only mode."""
    return bool(getattr(txn, _READ_ONLY_ATTRIBUTE, False))


def set_read_only(txn):
    """Put the transaction *txn* into read-only mode.

    All ZODB connections joined to *txn*, now or later, refuse to
    register modifications, add objects, or open blobs for writing.

    Raises ``ZODB.POSException.ReadOnlyError`` if a connection joined to
    *txn* already has uncommitted changes, because such a transaction
    can no longer become read-only.
    """
    from ZODB.Connection import Connection
    for resource in getattr(txn, '_resources', ()):
        if isinstance(resource, Connection) and (
                resource._registered_objects or resource._added):
            raise ReadOnlyError(
                "Cannot enter read-only mode: "
                "a connection has uncommitted changes")
    setattr(txn, _READ_ONLY_ATTRIBUTE, True)


@contextlib.contextmanager
def read_only(transaction_manager=None):
    """Context manager marking the current transaction read-only.

    *transaction_manager* defaults to the thread-local transaction
    manager.  The current transaction is put into read-only mode for the
    duration of the ``with`` block; the previous state is restored on
    exit.  See :func:`set_read_only` for the error raised when the
    transaction already has uncommitted changes.
    """
    if transaction_manager is None:
        transaction_manager = _transaction.manager
    txn = transaction_manager.get()
    previous = is_read_only(txn)
    set_read_only(txn)
    try:
        yield txn
    finally:
        setattr(txn, _READ_ONLY_ATTRIBUTE, previous)
