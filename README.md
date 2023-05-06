# tpcollections

Python Transactional Persistent Collections, backed by sqlite.

## Descripton

This is a package that gives you a set of Python collections (Mappings, Lists,
Sequence, and Deques) that are backed by SQLite.  This allows you to 

* Share collections between threads or processes safely.
* Maintain transactional safety across one or more collections.
* Persist collections efficiently, safely, and easily to disk, across one or
  more file.
* Manage collections that have expiring elements.

And do it all in any combination you wish without needing to worry about
specific details.

## Supported collection types

* Mapping, ordered by key.
* OrderedMapping, remembering insertion order.
* Set, ordered by key.
* OrderedSet, remembering insertion order.
* ExpiringMapping, with key entries that automatically time out and remove themselves.
* ExpiringOrderedMapping.
* ExpiringSet.
* ExpiringOrderedSet.
* Sequence.
* Deque.

There may be more in the future, if needed.

## Features

* Supports transactions, nestable through savepoints.
* Supports intelligent WAL mode, making decisions automatically that achieve
  the maximum performance while preserving durability even in the face of power
  failure.
  * When not forced one way or the other, this means enabling WAL mode when no
    databases are attached, and disabling it upon attaching databases.
* Supports many collections in one database file, ensuring all the used tables
  are the right type and that there are no collisions.
* Supports multiple attached database files, maintaining transactions,
  consistency, and durability across the entire set.
* Supports 
* Supported and tested on every reasonable version of Python and SQLite.
  * Tests are run all supported versions of Python, and on all in-support
    versions of RHEL, CentOS, Fedora, Debian, Ubuntu, and Alpine Linux.
    * This includes versions of Python or SQLite otherwise considered out of
      support by upstream.  At the time of this writing, this includes CentOS 7
      and therefore Python 3.6.8 and SQLite 3.7.17.
    * This does not include extended support (i.e. Centos 6 and Debian Jessie),
      because I haven't found a free and easy way to automate testing for them.
      I would be happy to accept contributions to add this support.
* SQLite features are version-tested, selecting the most appropriate features
  available on the platform.  This does mean that database files might not be
  able to be moved from a newer version of SQLite to an older one, particularly
  if you move from a version that supports STRICT and WITHOUT ROWID to one that
  doesn't

## Prior art

TBC.

## Feature comparison to other libraries
