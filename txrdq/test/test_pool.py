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

from twisted.internet.defer import Deferred
from twisted.trial import unittest
from txrdq.pool import DeferredPool


class TestPool(unittest.TestCase):
    """
    Test the L{DeferredPool} class used by L{ResizableDispatchQueue}.
    """

    def testEmptyPoolStatus(self):
        """
        A newly created (empty) pool should have no deferreds in progress
        and no deferreds waiting for the pool to drain.
        """
        pool = DeferredPool()
        self.assertEqual(pool.status(), (0, 0))

    def testPoolDeferredCount(self):
        """
        The pool must correctly report how many deferreds it has underway.
        """
        pool = DeferredPool()
        pool.add(Deferred())
        pool.add(Deferred())
        self.assertEqual(pool.status(), (2, 0))

    def testPoolWaitingCount(self):
        """
        The pool must correctly report how many deferreds are waiting
        for it to drain.
        """
        pool = DeferredPool()
        pool.notifyWhenEmpty(testImmediately=False)
        pool.notifyWhenEmpty(testImmediately=False)
        self.assertEqual(pool.status(), (0, 2))

    def testPoolStatus(self):
        """
        The pool must correctly report how many deferreds are underway
        and how many are waiting for it to drain.
        """
        pool = DeferredPool()
        pool.add(Deferred())
        pool.add(Deferred())
        pool.notifyWhenEmpty(testImmediately=False)
        self.assertEqual(pool.status(), (2, 1))

    def testPoolStatusAfterDeferredIsCallbacked(self):
        """
        The pool must correctly report how many deferreds are underway
        and how many are waiting for it to drain after one of its underway
        deferreds is callbacked.
        """
        pool = DeferredPool()
        d = Deferred()
        pool.add(d)
        pool.add(Deferred())
        self.assertEqual(pool.status(), (2, 0))
        d.callback(None)
        self.assertEqual(pool.status(), (1, 0))

    def testPoolStatusAfterDeferredIsErrbacked(self):
        """
        The pool must correctly report how many deferreds are underway
        and how many are waiting for it to drain after one of its underway
        deferreds is errbacked.
        """
        pool = DeferredPool()
        d = Deferred()
        d.addErrback(lambda _: True)
        pool.add(d)
        pool.add(Deferred())
        self.assertEqual(pool.status(), (2, 0))
        d.errback(Exception())
        self.assertEqual(pool.status(), (1, 0))

    def testCallbackedDeferredFiresWithTheRightResult(self):
        """
        The pool must correctly pass the original deferred callback result
        through any callbacks it might have added.
        """
        pool = DeferredPool()
        d = Deferred()
        pool.add(d)
        pool.notifyWhenEmpty()
        expectedValue = object()
        d.callback(expectedValue)
        self.assertIdentical(d.result, expectedValue)

    def testErrbackedDeferredFiresWithTheRightResult(self):
        """
        The pool must correctly pass the original deferred errback result
        through any callbacks it might have added.
        """
        expectedValue = Exception()
        pool = DeferredPool()
        d = Deferred()
        pool.add(d)
        pool.notifyWhenEmpty()
        d.errback(expectedValue)
        self.assertIdentical(d.result.value, expectedValue)
        return self.assertFailure(d, Exception)

    def testEmptyPoolWithTestImmediatelyTrue(self):
        """
        If the pool is empty and C{testImmediately} is C{True} when
        calling L{notifyWhenEmpty}, an already fired (with C{None} result)
        deferred must be returned and the pool should be empty.
        """
        pool = DeferredPool()
        d = pool.notifyWhenEmpty(testImmediately=True)
        self.assertEqual(d.called, True)
        self.assertEqual(d.result, None)
        self.assertEqual(pool.status(), (0, 0))

    def testEmptyPoolWithTestImmediatelyFalse(self):
        """
        If the pool is empty and C{testImmediately} is C{False} when
        calling L{notifyWhenEmpty}, the returned deferred must not have
        already fired and the pool should contain one waiting deferred.
        """
        pool = DeferredPool()
        d = pool.notifyWhenEmpty(testImmediately=False)
        self.assertEqual(d.called, False)
        self.assertEqual(pool.status(), (0, 1))

    def testNonEmptyPoolWithTestImmediatelyTrue(self):
        """
        If the pool is not empty and C{testImmediately} is C{True} when
        calling L{notifyWhenEmpty}, the returned deferred must not have
        already fired and the pool should contain one underway deferred
        and one waiting.
        """
        pool = DeferredPool()
        pool.add(Deferred())
        d = pool.notifyWhenEmpty(testImmediately=True)
        self.assertEqual(d.called, False)
        self.assertEqual(pool.status(), (1, 1))

    def testPoolEmpties(self):
        """
        If all the deferreds in a pool fire, its underway list should be empty.
        """
        pool = DeferredPool()
        d1 = Deferred()
        d2 = Deferred()
        d3 = Deferred()
        pool.add(d1)
        pool.add(d2)
        pool.add(d3)
        self.assertEqual(pool.status(), (3, 0))
        d1.callback(None)
        d2.callback(None)
        d3.callback(None)
        self.assertEqual(pool.status(), (0, 0))

    def testNotifyWhenPoolEmpties(self):
        """
        After all the deferreds in a pool fire, it should call all notify
        callbacks.
        """
        pool = DeferredPool()
        d1 = Deferred()
        d2 = Deferred()
        d3 = Deferred()
        pool.add(d1)
        pool.add(d2)
        pool.add(d3)
        # There must be 3 deferreds underway.
        self.assertEqual(pool.status(), (3, 0))
        wait1 = pool.notifyWhenEmpty()
        wait2 = pool.notifyWhenEmpty()
        # There must be 3 deferreds underway and 2 waiting.
        self.assertEqual(pool.status(), (3, 2))
        d1.callback(None)
        d2.callback(None)
        # There must be 1 deferreds underway and 2 waiting.
        self.assertEqual(pool.status(), (1, 2))
        # The waiters must not have fired yet.
        self.assertEqual(wait1.called, False)
        self.assertEqual(wait2.called, False)
        d3.callback(None)
        # Both waiters must have fired (with None) & the pool must be empty.
        self.assertEqual(wait1.called, True)
        self.assertEqual(wait2.called, True)
        self.assertEqual(pool.status(), (0, 0))
        self.assertEqual(wait1.result, None)
        self.assertEqual(wait2.result, None)
