##############################################################################
#
# Copyright (c) 2026 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Time-limited retention leases for historical FileStorage reads."""

import itertools
import time

from persistent.TimeStamp import TimeStamp

from ZODB.POSException import HistoryLeaseExpiredError
from ZODB.POSException import HistoryLeaseReleasedError


class HistoryLease:

    def __init__(self, lease_id, before, expires):
        self.id = lease_id
        self.before = before
        self.expires = expires
        self.released = False

    @property
    def lower_bound(self):
        seconds = int.from_bytes(self.before[:4], 'big')
        fraction = int.from_bytes(self.before[4:], 'big')
        if fraction > 0:
            return self.before[:4] + (fraction - 1).to_bytes(4, 'big')
        if seconds == 0:
            return b'\0' * 8
        return ((seconds - 1).to_bytes(4, 'big') +
                (2 ** 32 - 1).to_bytes(4, 'big'))


class HistoryLeaseManager:
    """Track historical reads.

    FileStorage calls methods in this class while holding its own lock,
    so renewals and pack start have a single, deterministic ordering.
    """

    def __init__(self, time=time.time):
        self._time = time
        self._leases = {}
        self._next_id = itertools.count(1)
        self._pack_in_progress = False
        self._packed_through = None

    def acquire(self, before, timeout):
        now = self._time()
        lease = HistoryLease(next(self._next_id), before, now + timeout)
        self._leases[lease.id] = lease
        return lease

    def renew(self, lease, timeout=None):
        if timeout is None:
            timeout = 300
        now = self._time()
        if lease.released:
            raise HistoryLeaseReleasedError('history retention lease released')
        self._check(lease, now)
        lease.expires = now + timeout
        return lease

    def check(self, lease):
        self._check(lease, self._time())

    def _check(self, lease, now):
        if lease.released:
            raise HistoryLeaseReleasedError('history retention lease released')
        if lease.expires <= now:
            raise HistoryLeaseExpiredError('history retention lease expired')
        if (self._packed_through is not None and
                before_unpacked(lease.before, self._packed_through)):
            raise HistoryLeaseExpiredError(
                'historical snapshot was removed by pack')

    def release(self, lease):
        lease.released = True
        self._leases.pop(lease.id, None)

    def begin_pack(self, stop):
        now = self._time()
        lower_bound = stop
        for lease in self._leases.values():
            if lease.expires <= now:
                continue
            if lease.lower_bound < lower_bound:
                lower_bound = lease.lower_bound
        self._pack_in_progress = True
        return lower_bound

    def finish_pack(self, stop):
        if (self._packed_through is None or
                stop > self._packed_through):
            self._packed_through = stop
        self._pack_in_progress = False

    def cancel_pack(self):
        self._pack_in_progress = False


def before_unpacked(before, packed_through):
    before = TimeStamp(before)
    packed_through = TimeStamp(packed_through)
    return before.raw() <= packed_through.raw()
