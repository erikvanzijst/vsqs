import os
import shutil
import tempfile
import threading
import time
from unittest import TestCase
from vsqs import queue


class QueueTest(TestCase):
    def setUp(self):
        self.manager = queue.QueueManager()
        self.q = self.manager.get_queue(tempfile.mkdtemp())

    def tearDown(self):
        self.q.close()
        self.manager.close()
        shutil.rmtree(self.q.path)
        # reset the clock:
        queue.micros = lambda: int(time.time() * 1000000)

    def test_publish(self):
        queue.micros = lambda: 0

        self.assertListEqual([], os.listdir(self.q.path))

        self.assertEqual('0', self.q.publish('foo'.encode('ascii')))
        self.assertListEqual(['0'], os.listdir(self.q.path))
        with open(os.path.join(self.q.path, '0')) as f:
            self.assertEqual('foo', f.read())

        queue.micros = lambda: 1
        self.assertEqual('1', self.q.publish('bar'.encode('ascii')))
        with open(os.path.join(self.q.path, '1')) as f:
            self.assertEqual('bar', f.read())

        self.assertSetEqual({'0', '1'}, set(os.listdir(self.q.path)))

    def test_receive(self):
        queue.micros = lambda: 0
        self.assertEqual('0', self.q.publish('foo'.encode('ascii')))
        queue.micros = lambda: 1
        self.assertEqual('1', self.q.publish('bar'.encode('ascii')))
        self.assertEqual(('0', 'foo'.encode('ascii')),
                         self.q.receive(visibility_timeout=10))
        self.assertEqual(('1', 'bar'.encode('ascii')),
                         self.q.receive(visibility_timeout=10))

    def test_delete(self):
        m_id = self.q.publish('foo'.encode('ascii'))
        self.q.delete(m_id)
        self.assertEqual((None, None),
                         self.q.receive(visibility_timeout=10, timeout=0.1))

    def test_requeue(self):
        queue.micros = lambda: 0
        self.q.publish('foo'.encode('ascii'))
        self.assertEqual(('0', 'foo'.encode('ascii')),
                         self.q.receive(visibility_timeout=1))

        # pretend a second has passed:
        queue.micros = lambda: 1000000

        # message should have been requeued:
        self.assertEqual(('0', 'foo'.encode('ascii')),
                         self.q.receive(visibility_timeout=1))

    def test_notify(self):
        """Wake up receivers when a new message gets published."""
        queue.micros = lambda: 0

        # have a 2nd thread publish a message while we're blocked waiting
        def run():
            time.sleep(.1)
            self.q.publish('foo'.encode('ascii'))

        t = threading.Thread(target=run)
        t.start()
        self.assertEqual(('0', 'foo'.encode('ascii')),
                         self.q.receive(visibility_timeout=1))
        t.join()

    def test_capacity(self):
        m1 = 'foo'.encode('ascii')
        m2 = 'bar'.encode('ascii')

        # fill up the queue
        self.q.capacity = 1
        self.q.publish(m1)

        # assert that publishing another message blocks:
        self.assertRaises(queue.QueueFullException,
                          self.q.publish, m2, timeout=.1)

        # have a 2nd thread block on publishing a second message
        t = threading.Thread(target=lambda: self.q.publish(m2))
        t.start()

        # make sure the publisher is blocked before freeing up capacity
        time.sleep(.1)

        mid, data = self.q.receive()
        self.assertEqual(m1, data)
        self.q.delete(mid)
        t.join()

        mid, data = self.q.receive()
        self.assertEqual(m2, data)
        self.q.delete(mid)

    def test_size(self):
        self.assertEqual(0, self.q.size())
        self.q.publish('foo'.encode('ascii'))
        self.assertEqual(1, self.q.size())
        self.q.publish('foo'.encode('ascii'))
        self.assertEqual(2, self.q.size())
        m_id, data = self.q.receive(visibility_timeout=10)
        self.assertEqual(2, self.q.size())
        self.q.delete(m_id)
        self.assertEqual(1, self.q.size())
