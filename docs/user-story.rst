===============
User Stories
===============

``tianyi`` is a CLI tool to manipulate and query libraries.

A **library** consists of objects and tags.
Objects are raw files of a book/paper/..., each of which is represented by its SHA-1 digest.

A tag is attributes adhered to a object.
All tags share a common evolvable schema.
Objects can be queried via tags.

All changes about objects and tags are stored in a local (but persistent) transaction buffer until they are committed.

In the local side, a library is a dedicated directory,
which will be called ``$TIANYI`` later.
Copying the entire directory will just form another copy.

The dedicated directory may or may not be human readable.
That is to say, for any particular file in the library, there may or may not be a same file in the directory.

In the cloud side, a library is a S3 bucket (or anything compatible with S3 API).

Settings
==============

There are no system-wide or user-wide settings.

There is a config file ``$TIANYI/config.toml``.
Here is an example.

..  code:: toml

    [upstream]
    endpoint = "URL to a remote library"
    bucket = "a bucket name" # Optional. In case that bucket name can not be encoded in endpoint.

    [upstream.credential]
    access_id = "the access ID"
    secret_key = "the secret key to the access ID"


Commands
=========

``tianyi`` consists of many commands.
By executing ``tianyi help`` or ``tianyi --help``, a list of commands and their descriptions will be shown.
For detailed explanations, it is required to execute ``tianyi $COMMAND --help``.

Except ``init`` and ``clone``, all commands must be executed under the dedicated directory.

init
+++++

``tianyi init $BUCKET [$DIR]`` accepts an existent and empty S3 bucket and a nonexistent directory as the dedicated one.
``init`` will automatically create the directory and put required files into the bucket.

Name of the local directory is by default the same as that of the cloud bucket.

clone
++++++

``tianyi clone $BUCKET [$DIR]`` clones a library stored in a cloud S3 bucket into a local and nonexistent directory as the dedicated one.
``clone`` will automatically create the directory.

Name of the local directory is by default the same as that of the cloud bucket.

pull
++++++

``tianyi pull`` pulls all changes from the cloud storage.

push
+++++++

``tianyi push`` will form a commit from the local transaction buffer and push to the cloud bucket.

compact
+++++++

``tianyi compact`` will compact loose objects into a compact format, both locally and remotely.
Beneath the surface, this operation will form a new commit.
So, each copy can pull this compaction to its dedicated directory.

query
++++++

``tianyi query`` accepts a condition which is compliant with SQL-WHERE syntax.
All objects are filtered with this condition by their tags.
And the result is mounted as a directory.
The result directory can be specified by ``--mountpoint`` option.

As the nature of FUSE, ``tianyi query`` will not automatically exit.
To exit, ``CTRL-C`` is required and the result directory will then be unmounted.

add
++++++

``tianyi add`` adds one or several objects to a local (but persistent) transaction buffer.

schema show
++++++++++++

Shows the current schema of all tags in `thrift format`_.

..  _thrift format: https://thrift.apache.org/docs/idl.html

schema update
++++++++++++++

Updates the schema of all tags to a specified file.
If the file is not compatible with the current schema, ``schema update`` will fail.

show
+++++

*   ``show $OBJECT`` will show the content of the specified object.
*   ``show --tag $OBJECT`` will show tag of the specified object.

status
+++++++
