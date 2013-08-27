# Copyright 2011-2013 Fluidinfo Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

from twisted.trial import unittest
from twisted.internet import defer
from txrdq.dpq import _Task, DeferredPriorityQueue
import random


class TestTask(unittest.TestCase):
    def testAttrs(self):
        """
        Test that the basic attributes set by the _Task class
        actually work.
        """
        obj = 'x'
        priority = 10
        count = 5
        task = _Task(obj=obj, priority=priority, count=count)
        self.assertEqual(obj, task.obj)
        self.assertEqual(priority, task.priority)
        self.assertEqual(count, task.count)

    def testLessThan(self):
        """
        Test that a simple less-than comparison works.
        """
        t1 = _Task(obj='z', priority=5, count=0)
        t2 = _Task(obj='a', priority=10, count=0)
        self.assertTrue(t1 < t2)


class TestDeferredPriorityQueue(unittest.TestCase):
    def testSimplePriorities(self):
        """
        Objects added to the queue with different priorities should
        come off the queue in their correct priority order.
        """
        q = DeferredPriorityQueue()
        q.put('a', 20)
        q.put('b', 10)
        q.put('c', 15)
        gotten = []
        q.get().addCallback(gotten.append)
        q.get().addCallback(gotten.append)
        q.get().addCallback(gotten.append)
        self.assertEqual(['b', 'c', 'a'], gotten)

    def testRepeatedPut(self):
        """
        Add the same object to the queue twice and make sure
        C{RuntimeError} is raised.
        """
        q = DeferredPriorityQueue()
        q.put(self, 20)
        self.assertRaises(RuntimeError, q.put, self, 10)

    def testSimpleReprioritize(self):
        """
        Add 3 objects to the queue with different priorities, change one of
        the priorities and make sure they come out of the queue in their
        correct priority order.
        """
        q = DeferredPriorityQueue()
        q.put('a', 20)
        q.put('b', 10)
        q.put('c', 15)
        q.reprioritize('c', 7)
        q.reprioritize('a', 8)
        gotten = []
        q.get().addCallback(gotten.append)
        q.get().addCallback(gotten.append)
        q.get().addCallback(gotten.append)
        self.assertEqual(['c', 'a', 'b'], gotten)

    def testReprioritizeToSameValue(self):
        """
        Add 3 objects to the queue with different priorities, change one of
        the priorities to be the same as it was and make sure they come out
        of the queue in their correct priority order.
        """
        q = DeferredPriorityQueue()
        q.put('a', 1)
        q.put('b', 2)
        q.put('c', 3)
        q.reprioritize('b', 2)
        gotten = []
        q.get().addCallback(gotten.append)
        q.get().addCallback(gotten.append)
        q.get().addCallback(gotten.append)
        self.assertEqual(['a', 'b', 'c'], gotten)

    def testReprioritizeNonexistent(self):
        """
        Trying to reprioritize a non-existent task should raise a
        C{KeyError}.
        """
        q = DeferredPriorityQueue()
        self.assertRaises(KeyError, q.reprioritize, 'c', 7)

    def testReprioritizeDeleted(self):
        """
        Trying to reprioritize a non-existent task should raise a
        C{KeyError}.
        """
        q = DeferredPriorityQueue()
        q.put('a', 10)
        q.delete('a')
        self.assertRaises(KeyError, q.reprioritize, 'a', 7)

    def testDelete(self):
        """
        Delete an object from the queue, and make sure it doesn't show up
        in a get or somehow survive in the queue.
        """
        q = DeferredPriorityQueue()
        q.put('a', 0)
        q.delete('a')
        q.put('b', 0)
        gotten = []
        q.get().addCallback(gotten.append)
        self.assertEqual(['b'], gotten)
        self.assertEqual(len(q), 0)

    def testDeleteNonExistent(self):
        """
        Deleting a non-existent object should raise a C{KeyError}.
        """
        q = DeferredPriorityQueue()
        self.assertRaises(KeyError, q.delete, 'a')

    def testDeleteAlreadyDeleted(self):
        """
        Trying to delete an object that has been deleted should raise a
        C{KeyError}.
        """
        q = DeferredPriorityQueue()
        q.put('a', 0)
        q.delete('a')
        self.assertRaises(KeyError, q.delete, 'a')

    def testStable(self):
        """
        Objects added to the queue with identical priorities should come
        off the list in the order they entered.
        """
        n = 10
        q = DeferredPriorityQueue()
        values = range(n)
        random.shuffle(values)
        for value in values:
            q.put(value, 0)
        gotten = []
        for _ in range(n):
            q.get().addCallback(gotten.append)
        self.assertEqual(values, gotten)


class LegacyTwistedDeferredPriorityQueueTests(unittest.TestCase):
    """
    These are tests on C{DeferredQueue} from twisted/test/test_defer.py
    adapted to test the same things in C{DeferredPriorityQueue}.

    Priorities are always zero in the queue puts.
    """
    def testQueue(self):
        N, M = 2, 2
        queue = DeferredPriorityQueue(N, M)

        gotten = []

        for i in range(M):
            queue.get().addCallback(gotten.append)
        self.assertRaises(defer.QueueUnderflow, queue.get)

        for i in range(M):
            queue.put(obj=i, priority=0)
            self.assertEquals(gotten, range(i + 1))
        for i in range(N):
            queue.put(obj=N + i, priority=0)
            self.assertEquals(gotten, range(M))
        self.assertRaises(defer.QueueOverflow, queue.put, obj=None, priority=0)

        gotten = []
        for i in range(N):
            queue.get().addCallback(gotten.append)
            self.assertEquals(gotten, range(N, N + i + 1))

        queue = DeferredPriorityQueue()
        gotten = []
        for i in range(N):
            queue.get().addCallback(gotten.append)
        for i in range(N):
            queue.put(obj=i, priority=0)
        self.assertEquals(gotten, range(N))

        queue = DeferredPriorityQueue(size=0)
        self.assertRaises(defer.QueueOverflow, queue.put, obj=None, priority=0)

        queue = DeferredPriorityQueue(backlog=0)
        self.assertRaises(defer.QueueUnderflow, queue.get)

    def test_cancelQueueAfterSynchronousGet(self):
        """
        When canceling a L{Deferred} from a L{DeferredQueue} that already has
        a result, the cancel should have no effect.
        """
        def _failOnErrback(_):
            self.fail("Unexpected errback call!")

        queue = DeferredPriorityQueue()
        d = queue.get()
        d.addErrback(_failOnErrback)
        queue.put(obj=None, priority=0)
        d.cancel()

    def test_cancelQueueAfterGet(self):
        """
        When canceling a L{Deferred} from a L{DeferredQueue} that does not
        have a result (i.e., the L{Deferred} has not fired), the cancel
        causes a L{defer.CancelledError} failure. If the queue has a result
        later on, it doesn't try to fire the deferred.
        """
        def cb(ignore):
            # If the deferred is still linked with the deferred queue, it will
            # fail with an AlreadyCalledError
            queue.put(obj=None, priority=0)
            return queue.get().addCallback(self.assertIdentical, None)

        queue = DeferredPriorityQueue()
        d = queue.get()
        self.assertFailure(d, defer.CancelledError)
        d.cancel()
        return d.addCallback(cb)
