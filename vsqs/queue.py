import errno
import os
import threading
import time
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

millis = lambda: int(time.time() * 1000)


class QueueManager(object):
    def __init__(self, path):
        self.path = path
        self.observer = Observer()
        self.observer.start()

    def get_queue(self, name):
        return Queue(self, os.path.join(self.path, name))

    def close(self):
        self.observer.stop()


class DirectoryWatcher(FileSystemEventHandler):
    def __init__(self, cond):
        self.cond = cond

    def on_any_event(self, event):
        with self.cond:
            self.cond.notifyAll()


class Queue(object):
    def __init__(self, manager, path):
        self.manager = manager
        if not os.path.exists(path):
            os.makedirs(path)
        self.path = path

    def fn(self, m_id, extension=None):
        base = os.path.join(self.path, m_id)
        if extension:
            return base + '.' + extension
        else:
            return base

    def publish(self, string):
        """Returns the message that can be used to delete the message."""
        while True:
            m_id = str(millis())
            try:
                fd = os.open(self.fn(m_id, 'new'),
                             os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except OSError, e:
                if e.errno == errno.EEXIST:
                    time.sleep(0.001)
                    continue
                raise e
            else:
                try:
                    if any(_fn.startswith(m_id) and _fn != '%s.new' % m_id
                           for _fn in os.listdir(self.path)):
                        os.unlink(self.fn(m_id, 'new'))
                        continue
                    else:
                        os.write(fd, string)
                        os.fsync(fd)
                        os.rename(self.fn(m_id, 'new'),
                                  self.fn(m_id))
                        return m_id
                finally:
                    os.close(fd)

    def receive(self, visibility_timeout=10, timeout=None):
        now = millis()
        remaining = lambda: (now + timeout * 1000) - millis()
        cond = threading.Condition()

        with cond:
            watch = self.manager.observer.schedule(DirectoryWatcher(cond),
                                                   self.path)
            try:
                while timeout is None or remaining() > 0:
                    m = self._get_oldest_message(
                        visibility_timeout=visibility_timeout)
                    if m is None:
                        cond.wait(None if timeout is None else
                                  remaining() / 1000.0)
                    else:
                        return m
            finally:
                self.manager.observer.unschedule(watch)

    def _get_oldest_message(self, visibility_timeout=10):
        """Returns a tuple containing the message id and its payload."""
        now = millis()
        expiration = now + visibility_timeout * 1000
        for m in sorted(self._list_messages(), key=lambda i: i[0]):
            m_id = str(m[0])
            if len(m) == 1:
                # attempt to consume this message
                try:
                    os.rename(self.fn(m_id), self.fn(m_id, str(expiration)))
                except OSError, e:
                    # ENOENT means someone else got here first, just move to
                    # the next file
                    if e.errno != errno.ENOENT:
                        raise e
                else:
                    with open(self.fn(m_id, str(expiration))) as f:
                        return m_id, f.read()
            elif m[1] <= now:
                # re-queue stale consumed message
                try:
                    os.rename(self.fn(m_id, str(m[1])), self.fn(m_id))
                except OSError, e:
                    # ENOENT is fine, it indicates someone else got there first
                    if e.errno != errno.ENOENT:
                        raise

    def _list_messages(self):
        """Generator yielding (message_id, expiration) tuples containing the
        integer message id (its file name).

        The second element in the tuple is only present for consumed messages
        that have not yet been deleted. It is an integer representing the
        expiration of its visibility
        """
        for _fn in os.listdir(self.path):
            try:
                yield map(int, _fn.split('.', 1))
            except ValueError:
                # non vsqs files are ignored
                pass

    def delete(self, m_id):
        map(os.unlink,
            (os.path.join(self.path, fn) for fn in os.listdir(self.path)
             if fn == str(m_id) or fn.startswith(str(m_id) + '.')))
