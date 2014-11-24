from contextlib import closing
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
        queue.micros = lambda: int(time.time() * 1000000)

    def test_publish(self):
        q = self.manager.get_queue('foo')
        queue.micros = lambda: 0

        self.assertListEqual([], os.listdir(q.path))

        self.assertEqual('0', q.publish('foo'.encode('ascii')))
        self.assertListEqual(['0'], os.listdir(q.path))
        with open(os.path.join(q.path, '0')) as f:
            self.assertEqual('foo', f.read())

        queue.micros = lambda: 1
        self.assertEqual('1', q.publish('bar'.encode('ascii')))
        with open(os.path.join(q.path, '1')) as f:
            self.assertEqual('bar', f.read())

        self.assertSetEqual({'0', '1'}, set(os.listdir(q.path)))

    def test_receive(self):
        queue.micros = lambda: 0
        with closing(self.manager.get_queue('foo')) as q:
            self.assertEqual('0', q.publish('foo'.encode('ascii')))
            queue.micros = lambda: 1
            self.assertEqual('1', q.publish('bar'.encode('ascii')))
            self.assertEqual(('0', 'foo'.encode('ascii')),
                             q.receive(visibility_timeout=10))
            self.assertEqual(('1', 'bar'.encode('ascii')),
                             q.receive(visibility_timeout=10))

    def test_delete(self):
        with closing(self.manager.get_queue('foo')) as q:
            m_id = q.publish('foo'.encode('ascii'))
            q.delete(m_id)
            self.assertEqual((None, None),
                             q.receive(visibility_timeout=10, timeout=0.1))

    def test_requeue(self):
        queue.micros = lambda: 0
        with closing(self.manager.get_queue('foo')) as q:
            q.publish('foo'.encode('ascii'))
            self.assertEqual(('0', 'foo'.encode('ascii')),
                             q.receive(visibility_timeout=1))

            # pretend a second has passed:
            queue.micros = lambda: 1000000

            # message should have been requeued:
            self.assertEqual(('0', 'foo'.encode('ascii')),
                             q.receive(visibility_timeout=1))

    def test_notify(self):
        """Wake up receivers when a new message gets published."""
        with closing(self.manager.get_queue('foo')) as q:
            queue.micros = lambda: 0

            # have a 2nd thread publish a message while we're blocked waiting
            def run():
                time.sleep(.1)
                q.publish('foo'.encode('ascii'))

            t = threading.Thread(target=run)
            t.start()
            self.assertEqual(('0', 'foo'.encode('ascii')),
                             q.receive(visibility_timeout=1))
            t.join()

    def test_capacity(self):
        m1 = 'foo'.encode('ascii')
        m2 = 'bar'.encode('ascii')

        with closing(self.manager.get_queue('foo', capacity=1)) as q:
            # fill up the queue
            q.publish(m1)

            # assert that publishing another message blocks:
            self.assertRaises(queue.QueueFullException,
                              q.publish, m2, timeout=.1)

            # have a 2nd thread block on publishing a second message
            t = threading.Thread(target=lambda: q.publish(m2))
            t.start()

            # make sure the publisher is blocked before freeing up capacity
            time.sleep(.1)

            mid, data = q.receive()
            self.assertEqual(m1, data)
            q.delete(mid)
            t.join()

            mid, data = q.receive()
            self.assertEqual(m2, data)
            q.delete(mid)

    def test_size(self):
        with closing(self.manager.get_queue('foo')) as q:
            self.assertEqual(0, q.size())
            q.publish('foo'.encode('ascii'))
            self.assertEqual(1, q.size())
            q.publish('foo'.encode('ascii'))
            self.assertEqual(2, q.size())
            m_id, data = q.receive(visibility_timeout=10)
            self.assertEqual(2, q.size())
            q.delete(m_id)
            self.assertEqual(1, q.size())
