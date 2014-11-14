=================================
VSQS: Very Simple Queueing System
=================================

VSQS is a message queuing system that is inspired on Amazon's SQS API, uses
the file system as persistent storage and does not require active broker
daemons.

Message delivery is strictly FIFO. Consumed messages must explicitly be deleted
to prevent automatic requeuing.


File State Transitions
----------------------

Each queue is represented by a directory on the file system. All messages on a
queue are individual files within this directory. There is no queue nesting.

Messages are stored as individual files that transition through the following
state diagram from creation to deletion.

File names are the millisecond unix timestamp of creation, followed by an
optional extension to indicate the file's current state. Transitions between
states are implemented as atomic file system operations so that multiple
processes can safely use a shared queue.


New
---

When a new message is published a new file is created whose name is the current
unix timestamp with the extension ``.new`` (e.g. ``1415776871123.new``).

Files are created atomically: ``open(fn, O_CREAT|O_EXCL|O_WRONLY)``

If creation fails because a concurrent process just wrote a message at the
exact same time, the publisher enters a retry loop, updating the file name with
the current time, until it succeeds.

If the call is successful, the client then performs a directory listing to
check for the presence of files with the same name, but with an extension. If
one is found, that indicates the existence of a message that was published at
the same time, but is currently in a different state. To avoid clashes, the
client then deleted it temporary files and loops back.

Only once a new file is successfully created and there are no other files with
the same timestamp in their filename can the message's payload be written to
the new file, after which the file is closed and moved into its final
destination by dropping the .new extension.


Ready
-----

Files without extension are ready for consumption. Queues are FIFO and so to
consume a message a client performs a directory listing, orders the file names
numerically ("99" comes before "100"), ignores all files that have extensions
and selects the oldest message.

To consume the message, the client then renames the file by adding the
expiration time as extension to the file (e.g. ``1415776871123.1415776879654``).

The expiration file is calculated by adding the visibility timeout to the
current time and expressing it as another unix timestamp with millisecond
precision.

The message is now marked as being in the processes of getting consumed. No
other consumer will touch the message.

If renaming failed it indicates a race with another client. Just loop back in
and select the next message.

Messages are returned to the user application accompanied by their unique
message id. This id can be their filename (minus any extensions).


Deleted
-------

When a consumer is done processing a message they must actively delete it by
specifying its id.

To delete a message, vsqs then performs a directory listing, looking for files
with that name, regardless of extension. This should match zero or one file
which is then deleted.


Requeuing
---------

Vsqs uses visibility timeouts similar to SQS. Visibility timeouts essentially
hide messages while they're being processed. If a client dies during
processing, the message will be placed back in the queue after its visibility
timeout expires.

Requeuing is a synchronous process performed by vsqs consumers while waiting
for new messages to consume.

After performing a directory listing, but before dequeuing the next message
from the queue, a consumer looks for message files that have a visibility
timeout extension that lies in the past (indicating expiration). For each one
found, the client then requeues it by simple dropping the extension from the
file name (implemented as an atomic rename operation). ``ENOENT`` failures
indicate a race and can safely be ignored.

After requeuing vsqs loops back, does a new directory listing and starts over.
Only when there are no more expired messages can the next message be consumed.
