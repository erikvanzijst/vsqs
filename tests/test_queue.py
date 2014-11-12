import os
import shutil
import tempfile
from unittest import TestCase
from vsqs import queue


class QueueTest(TestCase):
    def setUp(self):
        self.path = tempfile.mkdtemp()
        self.manager = queue.QueueManager(self.path)

    def tearDown(self):
        self.manager.close()
        shutil.rmtree(self.path)

    def test_publish(self):
        q = self.manager.get_queue('foo')
        queue.millis = lambda: 0

        self.assertItemsEqual([], os.listdir(q.path))
        self.assertEqual('0', q.publish('foo'))
        self.assertItemsEqual(['0'], os.listdir(q.path))

        queue.millis = lambda: 1
        self.assertEqual('1', q.publish('bar'))
        self.assertItemsEqual(['0', '1'], os.listdir(q.path))

        self.assertEqual(('0', 'foo'), q._get_oldest_message())
        print 'foo'
        self.assertEqual(('1', 'bar'), q._get_oldest_message())
