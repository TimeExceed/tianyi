################
Architecture
################

Structure of dedicated directories
###################################

*   Several raw pack files to store objects, whose name are of ``$VERSION.pack``.
*   Several tag files to store tags, whose name are of ``$VERSION.tag``.
    A tag file ``$VERSION.tag`` contains most recent tags about objects in ``$VERSION.pack``.
*   A file of tag schema, whose name is of ``$VERSION.thrift``.
*   A manifest file, whose name is ``$VERSION.manifest``.
    This file indicates all pack files, tag files and optional tag schema in this commit.
    So, all files not in the latest manifest files can be safely removed.

Structure of cloud buckets
###########################

Files in a cloud bucket are the same as those in the dedicated directory,
except that pack files can be compressed by `XZ`_.
If pack files are compressed, their names are of ``$VERSION.pack.xz``,
and their names in the manifest must also be changed respectively.

..  _XZ: https://tukaani.org/xz/

Representation of the linear commit history
##################################################

Concurrent operations are forbidden.
It is reasonable for a personal library.
To achieve this, each commit is assigned a unique version, an universally monotonic 64-bit number.

Versions are generated locally.
Suppose the current version is A and the current local wall time in microseconds is B.
Then the generated version is :math:`\max(A+1, B)`.
So, for a machine with well calibrated system time, versions look very close to the wall time.
This feature will help a lot in debugging.
For example, versions in file names, because of their close relation to the wall time, are represented in RFC-3339, UTC time.

With uniqueness of versions, it is easy to achieve a linear history by a compare-and-set scheme.
Each commit contains both version of itself and that of its predecessor.
While publishing it to the remote, ``tianyi`` program will check whether the version of the remote is exactly the same as the predecessor version recorded in the local commit.
The commit will be written only if the check is passed.

..  note::

    I know when two programs write at the same time, the scheme is going to fail.
    But for a personal library, this won't be a big thing.

Format of object packs
###############################

An object pack is a file consisting of a pack of objects.
Its structure is:

+   A header of "TIANYI OBJPACK" followed by a single character '\\x1A'.
+   Followed by a series of objects. For each,

    +   An unsigned 64-bit, network byte order, size of the object,
    +   The raw content of the object,

*   Followed by a list of object anchors, sorted in the increasing order of object's SHA-1.
    Each anchor consists of

    +   the object's SHA-1,
    +   An unsigned 64-bit, network byte order, offset to the beginning of this file,
        pointed to the first byte of the object (i.e., the first byte of its size).

*   Followed by a 32-bit, network byte order number indicating the number of objects stored in this file.
*   Followed by a CRC64 checksum of the entire file except this checksum.

Initial tag schema
###################

Directory structure of the result of queries
##############################################

