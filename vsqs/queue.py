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

    def _list_messages(self):
        """Generator yielding (message_id, expiration) tuples containing the
        integer message id (its file name).

        The second element in the tuple is only present for consumed messages
        that have not yet been deleted. It is an integer representing the
        expiration of its visibility
        """
        for _fn in os.listdir(self.path):
            try:
                yield map(int, _fn.split('-', 1))
            except ValueError:
                pass

    def publish(self, string):
        """Returns the message that can be used to delete the message."""
        tmp_fn = lambda name: os.path.join(self.path, '.tmp-%s' % name)
        fn = lambda name: os.path.join(self.path, name)

        while True:
            fname = str(millis())
            try:
                print 'open', tmp_fn(fname)
                fd = os.open(tmp_fn(fname), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except OSError, e:
                print 'open failed', e
                if e.errno == errno.EEXIST:
                    continue
                raise e
            else:
                try:
                    try:
                        with open(fn(fname)) as f:
                            continue
                    except IOError, e:
                        if e.errno != errno.ENOENT:
                            raise
                    os.write(fd, string)
                    os.rename(tmp_fn(fname), fn(fname))
                    return fname
                finally:
                    os.close(fd)

    def receive(self, visibility_timeout=10, timeout=None):
        now = millis()
        print 'receive'
        remaining = lambda: (now + timeout * 1000) - millis()
        cond = threading.Condition()

        with cond:
            watch = self.manager.observer.schedule(DirectoryWatcher(cond),
                                                   self.path)
            try:
                while timeout is None or remaining() > 0:
                    m = self._get_oldest_message(visibility_timeout=visibility_timeout)
                    if m is None:
                        cond.wait(None if timeout is None else remaining())
                    else:
                        return m
            finally:
                self.manager.observer.unschedule(watch)

    def _get_oldest_message(self, visibility_timeout=10):
        """Returns a tuple containing the message id and its payload."""
        now = millis()
        for m in sorted(self._list_messages(), key=lambda i: i[0]):
            if len(m) == 1:
                m_id = str(m[0])
                # attempt to consume this message
                try:
                    os.symlink(
                        str(os.getpid()),
                        os.path.join(self.path,
                                     '%s-%d' % (m_id, now +
                                                visibility_timeout * 1000)))
                except OSError, e:
                    if e.errno == errno.EEXIST:
                        print 'break', e, os.path.exists(os.path.join(self.path,
                                     '%s-%d' % (m_id, now +
                                                visibility_timeout * 1000))), os.path.join(self.path,
                                     '%s-%d' % (m_id, now +
                                                visibility_timeout * 1000))
                        time.sleep(300)
                        break
                    raise e
                else:
                    with open(os.path.join(self.path, m_id)) as f:
                        return m_id, f.read()
            elif m[1] < now:
                # re-queue stale consumed message
                try:
                    os.unlink(os.path.join(self.path, '%d-%d' % m))
                except OSError, e:
                    if e.errno != errno.ENOENT:
                        raise

    def delete(self, id):
        map(os.unlink,
            (os.path.join(self.path, fn) for fn in os.listdir(self.path)
             if fn == str(id) or fn.startswith(str(id) + '-')))


class AtomicFile(object):
    def __init__(self, base):
        self.base = base
        self.name = None
        self.f = None

    @property
    def path(self):
        return os.path.join(self.base, self.name)

    @property
    def tmppath(self):
        return os.path.join(self.base, '.tmp-%s' % self.name)

    def __enter__(self):
        while True:
            self.name = str(millis())
            try:
                print 'open', self.tmppath
                fd = os.open(self.tmppath, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except OSError, e:
                print 'open failed', e
                if e.errno == errno.EEXIST:
                    continue
                raise e
            else:
                self.f = os.fdopen(fd, 'w')
                return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f.close()
        if exc_val is None:
            # move into place
            os.rename(self.tmppath, self.path)
        else:
            os.unlink(self.tmppath)

    def __getattr__(self, item):
        return getattr(self.f, item)
