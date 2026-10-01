"""Historical read leases.

A historical lease records the lower bound (a ``before`` transaction
id) of the revisions that an historical connection still needs to be
able to read.  Storages that pack (currently FileStorage) consult the
registry of active leases when computing the pack retention range, so
that a background pack cannot collect revisions a live historical
connection depends on.

Leases expire: a crashed process must not block garbage collection
forever.  Holders must renew a lease explicitly to keep it alive, and
reading through an expired lease raises
:class:`~ZODB.POSException.HistoricalLeaseExpired` rather than
returning a mix of revisions.

All registry operations are serialized with a lock, so a renewal (or
registration) and the start of a pack have a deterministic order: a
lease that is still valid when the pack computes its retention limit
is honored by that pack.
"""

import threading
import time

from .POSException import HistoricalLeaseExpired


class HistoricalLease:
    """A lease protecting a lower bound of historical revisions.

    ``before`` is the 8-byte transaction id below which revisions must
    be retained.  ``expires`` is the lease expiry, in units of the
    registry's clock.
    """

    def __init__(self, registry, before):
        self._registry = registry
        self.before = before
        self.expires = registry._clock() + registry.timeout
        self._released = False

    def expired(self):
        """Return True if the lease is no longer valid."""
        return self._released or self._registry._clock() >= self.expires

    def renew(self):
        """Extend the lease, raising HistoricalLeaseExpired if too late."""
        self._registry.renew(self)

    def release(self):
        """Release the lease.  Idempotent."""
        self._registry.release(self)


class HistoricalLeaseRegistry:
    """Registry of the historical leases of a single storage.

    ``timeout`` is the lease lifetime in seconds and ``clock`` is a
    callable returning the current time in seconds (injectable for
    tests).
    """

    def __init__(self, timeout=300.0, clock=time.time):
        self.timeout = timeout
        self._clock = clock
        self._lock = threading.Lock()
        self._leases = set()

    def register(self, before):
        """Register a lease protecting revisions below ``before``."""
        with self._lock:
            lease = HistoricalLease(self, before)
            self._leases.add(lease)
            return lease

    def renew(self, lease):
        """Extend a lease, unless it has already expired."""
        with self._lock:
            now = self._clock()
            if lease._released or lease not in self._leases \
                    or now >= lease.expires:
                lease._released = True
                self._leases.discard(lease)
                raise HistoricalLeaseExpired(
                    "cannot renew an expired historical lease")
            lease.expires = now + self.timeout

    def release(self, lease):
        """Drop a lease from the registry.  Idempotent."""
        with self._lock:
            lease._released = True
            self._leases.discard(lease)

    def pack_retention_limit(self):
        """Return the lowest ``before`` protected by a valid lease.

        Called by a storage when a pack starts.  Expired leases are
        pruned, so crashed processes cannot block cleanup forever.
        Serialized with registration and renewal, giving a
        deterministic order between a renewal and the start of a pack.
        Returns None if no lease is active.
        """
        with self._lock:
            now = self._clock()
            for lease in list(self._leases):
                if now >= lease.expires:
                    lease._released = True
                    self._leases.discard(lease)
            if not self._leases:
                return None
            return min(lease.before for lease in self._leases)

    def __len__(self):
        with self._lock:
            return len(self._leases)
