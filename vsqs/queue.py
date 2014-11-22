import errno
import os
import threading
import time
from collections import OrderedDict
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

micros = lambda: int(time.time() * 1000000)
MAX_CACHE_SIZE = 256


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
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        self.manager = manager
        self.path = path
        self._cond = threading.Condition()
        self._watch = manager.observer.schedule(
            DirectoryWatcher(self._cond), self.path)
        self._mid_cache = OrderedDict()

    def close(self):
        self.manager.observer.unschedule(self._watch)

    def _fn(self, m_id, extension=None):
        base = os.path.join(self.path, m_id)
        if extension:
            return base + '.' + extension
        else:
            return base

    def _cache(self, mid, fn):
        self._mid_cache[mid] = fn
        if len(self._mid_cache) > MAX_CACHE_SIZE:
            self._mid_cache.popitem(last=False)

    def size(self):
        """Returns the number of messages currently in the queue. Note that this
        includes messages that are being consumed, but have not yet been
        deleted.
        """
        return sum(1 for m in self._list_messages())

    def publish(self, data):
        """Returns the message id that can be used to delete the message."""
        while True:
            m_id = str(micros())
            try:
                fd = os.open(self._fn(m_id, 'new'),
                             os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    time.sleep(0.001)
                    continue
                raise e
            else:
                try:
                    if any(_fn.startswith(m_id) and _fn != '%s.new' % m_id
                           for _fn in os.listdir(self.path)):
                        os.unlink(self._fn(m_id, 'new'))
                        continue
                    else:
                        os.write(fd, data)
                        os.fsync(fd)
                        os.rename(self._fn(m_id, 'new'),
                                  self._fn(m_id))
                        self._cache(m_id, self._fn(m_id))
                        return m_id
                finally:
                    os.close(fd)

    def receive(self, visibility_timeout=10, timeout=None):
        now = micros()
        remaining = lambda: (now + timeout * 1000000) - micros()

        while timeout is None or remaining() > 0:
            with self._cond:
                m = self._get_oldest_message(
                    visibility_timeout=visibility_timeout)
                if m == (None, None):
                    self._cond.wait(None if timeout is None else
                                    remaining() / 1000000.0)
                else:
                    return m
        return None, None

    def _get_oldest_message(self, visibility_timeout=10):
        """Returns a tuple containing the message id and its payload."""
        now = micros()
        expiration = now + visibility_timeout * 1000000
        for m in sorted(self._list_messages(), key=lambda i: i[0]):
            m_id = str(m[0])
            if len(m) == 1:
                # attempt to consume this message
                try:
                    os.rename(self._fn(m_id), self._fn(m_id, str(expiration)))
                except OSError as e:
                    # ENOENT means someone else got here first, just move to
                    # the next file
                    if e.errno != errno.ENOENT:
                        raise e
                else:
                    fn = self._fn(m_id, str(expiration))
                    self._cache(m_id, fn)
                    with open(fn, 'rb') as f:
                        return m_id, f.read()
            elif m[1] <= now:
                # re-queue stale consumed message
                try:
                    os.rename(self._fn(m_id, str(m[1])), self._fn(m_id))
                except OSError as e:
                    # ENOENT is fine, it indicates someone else got there first
                    if e.errno != errno.ENOENT:
                        raise
        return None, None

    def _list_messages(self):
        """Generator yielding (message_id, expiration) tuples containing the
        integer message id (its file name).

        The second element in the tuple is only present for consumed messages
        that have not yet been deleted. It is an integer representing the
        expiration of its visibility
        """
        for _fn in os.listdir(self.path):
            try:
                yield list(map(int, _fn.split('.', 1)))
            except ValueError:
                # non vsqs files are ignored
                pass

    def delete(self, m_id):
        try:
            os.unlink(self._mid_cache.pop(m_id))
            return
        except KeyError:
            pass
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        for p in (os.path.join(self.path, fn) for fn in os.listdir(self.path)):
            if p == str(m_id) or p.startswith(str(m_id) + '.'):
                os.unlink(p)
