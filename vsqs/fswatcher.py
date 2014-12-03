from threading import Condition
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class FSWatcher(FileSystemEventHandler):
    watchers = set()
    registermutex = Condition()
    observer = None

    @classmethod
    def watch(cls, path):
        """Returns a watcher object that can be used to cancel the watch."""
        with cls.registermutex:
            if len(cls.watchers) == 0:
                cls.observer = Observer()
                cls.observer.start()
            watcher = cls(path)
            cls.watchers.add(watcher)
            return watcher

    def __init__(self, path):
        super(FSWatcher, self).__init__()
        self.pathmutex = Condition()
        self.watch = self.observer.schedule(self, path)

    def __enter__(self):
        return self.pathmutex.__enter__()

    def __exit__(self, *args):
        return self.pathmutex.__exit__(*args)

    def wait(self, timeout=None):
        return self.pathmutex.wait(timeout=timeout)

    def notify_all(self):
        return self.pathmutex.notify_all()

    def on_any_event(self, event):
        with self:
            self.notify_all()

    def stop(self):
        with self.registermutex:
            self.watchers.remove(self)
            self.observer.unschedule(self.watch)
            if len(self.watchers) == 0:
                self.observer.stop()
