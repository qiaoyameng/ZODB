"""Transaction-level read-only mode.

Marking a transaction read-only makes every ZODB ``Connection`` joined
(or about to join) the transaction refuse changes as they are attempted,
rather than failing at commit time:

- registering a modified persistent object,
- adding a new object to a connection, and
- opening a blob for writing

all raise ``ZODB.POSException.ReadOnlyError`` immediately.

Reading is unaffected: loading objects, activating ghosts and opening
blobs for reading work as usual, and other transactions can still write
to the same database.

The restriction lives on the transaction object, so it ends with the
transaction: aborting or committing clears it and the next transaction
is unrestricted.  Savepoints neither remove nor survive the restriction
-- they simply don't interact with it.
"""

from ZODB.POSException import ReadOnlyError


_ATTRIBUTE = '_zodb_read_only'


def set_read_only(transaction):
    """Mark ``transaction`` read-only for all ZODB connections.

    Raises ``ZODB.POSException.ReadOnlyError`` if a ZODB connection
    joined to the transaction already has changes, because such a
    transaction can no longer complete successfully.
    """
    if getattr(transaction, _ATTRIBUTE, False):
        return
    for resource in getattr(transaction, '_resources', ()):
        if (getattr(resource, '_registered_objects', None)
                or getattr(resource, '_added', None)):
            raise ReadOnlyError(
                "Cannot make a transaction with changes read-only")
    setattr(transaction, _ATTRIBUTE, True)


def is_read_only(transaction):
    """Return whether ``transaction`` has been marked read-only."""
    return bool(getattr(transaction, _ATTRIBUTE, False))
