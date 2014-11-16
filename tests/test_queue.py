import os
import shutil
import tempfile
import threading
import time
from unittest import TestCase
from vsqs import queue


class QueueTest(TestCase):
    def setUp(self):
        self.path = tempfile.mkdtemp()
        self.manager = queue.QueueManager(self.path)

    def tearDown(self):
        self.manager.close()
        shutil.rmtree(self.path)
        # reset the clock:
        queue.millis = lambda: int(time.time() * 1000)

    def test_publish(self):
        q = self.manager.get_queue('foo')
        queue.millis = lambda: 0

        self.assertItemsEqual([], os.listdir(q.path))

        self.assertEqual('0', q.publish('foo'))
        self.assertItemsEqual(['0'], os.listdir(q.path))
        with open(os.path.join(q.path, '0')) as f:
            self.assertEqual('foo', f.read())

        queue.millis = lambda: 1
        self.assertEqual('1', q.publish('bar'))
        with open(os.path.join(q.path, '1')) as f:
            self.assertEqual('bar', f.read())

        self.assertItemsEqual(['0', '1'], os.listdir(q.path))

    def test_receive(self):
        queue.millis = lambda: 0
        q = self.manager.get_queue('foo')
        self.assertEqual('0', q.publish('foo'))
        queue.millis = lambda: 1
        self.assertEqual('1', q.publish('bar'))
        self.assertEqual(('0', 'foo'), q.receive(visibility_timeout=10))
        self.assertEqual(('1', 'bar'), q.receive(visibility_timeout=10))

    def test_delete(self):
        q = self.manager.get_queue('foo')
        m_id = q.publish('foo')
        q.delete(m_id)
        self.assertEqual((None, None),
                         q.receive(visibility_timeout=10, timeout=0.1))

    def test_requeue(self):
        queue.millis = lambda: 0
        q = self.manager.get_queue('foo')
        q.publish('foo')
        self.assertEqual(('0', 'foo'), q.receive(visibility_timeout=1))

        # pretend a second has passed:
        queue.millis = lambda: 1000

        # message should have been requeued:
        self.assertEqual(('0', 'foo'), q.receive(visibility_timeout=1))

    def test_notify(self):
        """Wake up receivers when a new message gets published."""
        q = self.manager.get_queue('foo')
        queue.millis = lambda: 0

        # have a 2nd thread publish a message while we're blocked waiting
        def run():
            time.sleep(.1)
            q.publish('foo')

        t = threading.Thread(target=run)
        t.start()
        self.assertEqual(('0', 'foo'), q.receive(visibility_timeout=1))
        t.join()

    def test_size(self):
        q = self.manager.get_queue('foo')
        self.assertEqual(0, q.size())
        q.publish('foo')
        self.assertEqual(1, q.size())
        q.publish('foo')
        self.assertEqual(2, q.size())
        m_id, data = q.receive(visibility_timeout=10)
        self.assertEqual(2, q.size())
        q.delete(m_id)
        self.assertEqual(1, q.size())
