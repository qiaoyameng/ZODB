============
Transactions
============

Transaction support is provided by the `transaction
<http://transaction.readthedocs.io/en/latest/>`_ package
[#transaction-package-can-be-used-wo-ZODB]_, which is installed
automatically when you install ZODB.  There are two important APIs
provided by the transaction package, ``ITransactionManager`` and
``ITransaction``, described below.

ITransactionManager
===================

.. autointerface:: transaction.interfaces.ITransactionManager
   :members: begin, get, commit, abort, doom, isDoomed, savepoint

ITransaction
============

.. autointerface:: transaction.interfaces.ITransaction
   :members: user, description, commit, abort, doom, savepoint, note,
             setUser, setExtendedInfo,
             addBeforeCommitHook, getBeforeCommitHooks,
             addAfterCommitHook, getAfterCommitHooks

Read-only transactions
======================

ZODB can put a transaction into read-only mode, which is useful for
code that must not modify the database, such as reporting.  All
connections joined to a read-only transaction reject registered
modifications, object additions, and blob writes with
``ZODB.POSException.ReadOnlyError`` as they happen, rather than
failing at commit time.  Reads, including ghost activation, are
unaffected, and other transactions may still write to the database.
The restriction ends with the transaction, so aborting and beginning
the next transaction clears it.

.. automodule:: ZODB.readonly
   :members: set_read_only, is_read_only, read_only


.. [#transaction-package-can-be-used-wo-ZODB] The :mod:`transaction`
   package is a general purpose package for managing `distributed
   transactions
   <https://en.wikipedia.org/wiki/Distributed_transaction>`_ with a
   `two-phase commit protocol
   <https://en.wikipedia.org/wiki/Two-phase_commit_protocol>`_.  It
   can and occasionally is used with packages other than ZODB.
