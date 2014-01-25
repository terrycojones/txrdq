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
from twisted.internet import defer, reactor, task
from txrdq.rdq import ResizableDispatchQueue, QueueStopped
from txrdq.job import Job


class TestPendingStops(unittest.TestCase):
    """
    Tests that check that the number of pending stops gets set correctly
    following changes in queue width.
    """

    # These tests all return dq.stop() because if the queue is set to be
    # very wide, the reactor will be unclean when the test ends as many
    # Deferreds will be being created.

    def testZeroOnDefaultCreation(self):
        """
        A new queue whose width is unspecified should have no pending
        stops.
        """
        dq = ResizableDispatchQueue(None)
        self.assertEqual(0, dq.pendingStops)
        return dq.stop()

    def testZeroOnExplicitCreation(self):
        """
        A new queue whose width is 10 should have no pending stops.
        """
        dq = ResizableDispatchQueue(None, 10)
        self.assertEqual(0, dq.pendingStops)
        return dq.stop()

    def testNarrow(self):
        """
        Create a queue of width 5 and narrow it to width 3. There should
        then be 2 pending stops.
        """
        dq = ResizableDispatchQueue(None, 5)
        dq.width = 3
        self.assertEqual(2, dq.pendingStops)
        return dq.stop()

    def testNarrowWiden(self):
        """
        Create a queue of width 10. Narrowing to 7 means there will be 3
        pending stops, then widening to 8 will decrease the number of
        pending stops to 2.
        """
        dq = ResizableDispatchQueue(None, 10)
        dq.width = 7
        self.assertEqual(dq.pendingStops, 3)
        dq.width = 8
        self.assertEqual(dq.pendingStops, 2)
        return dq.stop()

    def testSetImmediatelyToZero(self):
        """
        Create a queue of width 10 and then set its width to 0.
        The number of pending stops will be 10.
        """
        dq = ResizableDispatchQueue(None, 10)
        dq.width = 0
        self.assertEqual(dq.pendingStops, 10)
        return dq.stop()

    def testIncrementToVeryHighValue(self):
        """
        Create a queue of width 10 and then set its width to 1000.
        The number of pending stops will be 0.
        """
        dq = ResizableDispatchQueue(None, 10)
        dq.width = 1000
        self.assertEqual(dq.pendingStops, 0)
        return dq.stop()

    def testIncrementToVeryHighValueThenSetToLowValue(self):
        """
        Create a queue of width 10 and then set its width to 1000 then set
        it to 100.  The number of pending stops will be 900.
        """
        dq = ResizableDispatchQueue(None, 10)
        dq.width = 1000
        dq.width = 100
        self.assertEqual(dq.pendingStops, 900)
        return dq.stop()

    def testIncrementToVeryHighValueThenSetToLowValueThenIncrement(self):
        """
        Create a queue of width 10 and then set its width to 1000 then set
        it to 100, then to 500.  The number of pending stops will be 500.
        """
        dq = ResizableDispatchQueue(None, 10)
        dq.width = 1000
        dq.width = 100
        dq.width = 500
        self.assertEqual(dq.pendingStops, 500)
        return dq.stop()


class TestSimple(unittest.TestCase):
    """
    Simple tests of the ResizableDispatchQueue class.
    """
    def testUnstarted(self):
        """
        Check basic facts about a fresh instance of a
        ResizableDispatchQueue.
        """
        dq = ResizableDispatchQueue(None)
        self.assertEqual(dq.pending(), [])
        self.assertEqual(dq.underway(), set())
        self.assertFalse(dq.stopped)
        self.assertFalse(dq.paused)
        self.assertEqual((0, 0), dq.size())
        return dq.stop()

    def testNumericStartWidth(self):
        """
        Check that passing a numeric width to the constructor works as
        expected.
        """
        dq = ResizableDispatchQueue(None, 10)
        self.assertEqual(dq.width, 10)

    def testNegativeStartWidth(self):
        """
        Passing a negative width to the constructor should raise C{ValueError}.
        """
        self.assertRaises(ValueError, ResizableDispatchQueue, None, -1)

    def testNonNumericStartWidth(self):
        """
        Passing a non-numeric width to the constructor should raise
        C{ValueError}.
        """
        self.assertRaises(ValueError, ResizableDispatchQueue, None, 'hi')

    def testSimplePending(self):
        """
        Putting 3 jobs onto a queue with zero width should result in a
        number of pending jobs of 3.
        """
        dq = ResizableDispatchQueue(None)
        map(dq.put, range(3))
        self.assertEqual((0, 3), dq.size())

    def testSetWidthToZeroAfterInitiallyNonZero(self):
        """
        Make sure that a queue whose width is initially non-zero and which
        is then set to zero width does not then begin to process any added
        jobs.
        """
        dq = ResizableDispatchQueue(None, 3)
        dq.width = 0
        dq.put('aaa')
        dq.put('bbb')
        self.assertEqual((0, 2), dq.size())
        return dq.stop()

    @defer.inlineCallbacks
    def testSetWidthToZeroAfterInitiallyNonZeroThenStop(self):
        """
        Make sure that a queue whose width is initially non-zero and which
        is then set to zero width returns all the added jobs when stopped.
        """
        dq = ResizableDispatchQueue(None, 3)
        dq.width = 0
        dq.put('aaa', 5)
        dq.put('bbb', 10)
        remaining = yield dq.stop()
        self.assertEqual(['aaa', 'bbb'], [job.jobarg for job in remaining])

    @defer.inlineCallbacks
    def testStopSetsStopped(self):
        dq = ResizableDispatchQueue(None, 5)
        yield dq.stop()
        self.assertEqual(True, dq.stopped)

    @defer.inlineCallbacks
    def testStopAfterNoPuts(self):
        dq = ResizableDispatchQueue(None, 5)
        pending = yield dq.stop()
        self.assertEqual(pending, [])

    def testClearQueue(self):
        dq = ResizableDispatchQueue(None)
        map(dq.put, range(3))
        self.assertEqual((0, 3), dq.size())
        dq.clearQueue(cancelPending=False)
        self.assertEqual((0, 0), dq.size())

    @defer.inlineCallbacks
    def testClearQueueCancelPending(self):
        """
        When the queue is cleared with cancelPending=True, the pending jobs
        should fail and receive a job whose state is CANCELLED.
        """
        def ok(result):
            self.fail('Unexpected success!')

        def checkCancel(failure):
            self.assertEqual(failure.value.state, Job.CANCELLED)

        dq = ResizableDispatchQueue(None, 2)
        dq.put(0).addCallbacks(ok, checkCancel)
        dq.put(1).addCallbacks(ok, checkCancel)
        self.assertEqual((0, 2), dq.size())
        dq.clearQueue(cancelPending=True)
        self.assertEqual((0, 0), dq.size())
        yield dq.stop()

    @defer.inlineCallbacks
    def testPutRaisesWhenStopped(self):
        """
        Putting something onto a stopped queue should raise
        C{QueueStopped}.
        """
        dq = ResizableDispatchQueue(None)
        yield dq.stop()
        self.assertRaises(QueueStopped, dq.put, None)

    def testWidenWithNegativeWidth(self):
        """
        Widening a queue with a negative width should raise C{ValueError}.
        """
        dq = ResizableDispatchQueue(None)
        self.assertRaises(ValueError, dq.setWidth, -3)

    def testWidenWithNonNumericWidth(self):
        """
        Widening a queue with a non-numeric width should raise
        C{ValueError}.
        """
        dq = ResizableDispatchQueue(None)
        self.assertRaises(ValueError, dq.setWidth, 'Wider, pussycat! Wider!')

    @defer.inlineCallbacks
    def testGetPausedWidth(self):
        dq = ResizableDispatchQueue(None, 1)
        yield dq.pause()
        self.assertEqual(dq.width, 1)
        dq.setWidth(3)
        self.assertEqual(dq.width, 3)

    @defer.inlineCallbacks
    def testPauseResumeWithZeroWidth(self):
        """
        Check that it is possible to resume with zero width.
        """
        dq = ResizableDispatchQueue(None, 1)
        yield dq.pause()
        dq.resume(0)
        self.assertEqual(dq.width, 0)

    @defer.inlineCallbacks
    def testPauseResumeWithNonZeroWidth(self):
        """
        Check that it is possible to resume with a non-zero width.
        """
        dq = ResizableDispatchQueue(None, 1)
        yield dq.pause()
        dq.resume(10)
        self.assertEqual(dq.width, 10)

    @defer.inlineCallbacks
    def testPauseResumeWithNegativeWidth(self):
        dq = ResizableDispatchQueue(None)
        yield dq.pause()
        self.assertRaises(ValueError, dq.resume, -5)

    @defer.inlineCallbacks
    def testPauseResumeWithNonNumericWidth(self):
        dq = ResizableDispatchQueue(None)
        yield dq.pause()
        self.assertRaises(ValueError, dq.resume, 'go!')

    @defer.inlineCallbacks
    def testDeferredFromPutFiresWithCorrectResult(self):
        """
        Make sure the C{Deferred} we get back from a put to the queue fires
        with the expected result.
        """
        dq = ResizableDispatchQueue(lambda x: 2 * x, width=1)
        job = yield dq.put(6)
        self.assertEqual(job.result, 12)

    def testReprioritizeNonExistentJob(self):
        """
        Reprioritizing a non-existent job must raise C{KeyError}.
        """
        dq = ResizableDispatchQueue(None, 1)
        self.assertRaises(KeyError, dq.reprioritize, Job('x', 1), 10)

    @defer.inlineCallbacks
    def testReprioritizeNonPendingJob(self):
        """
        Reprioritizing a job that is not pending must raise
        C{RuntimeError}.
        """
        dq = ResizableDispatchQueue(lambda x: x, 1)
        job = yield dq.put('a')
        self.assertRaises(RuntimeError, dq.reprioritize, job, 10)

    def testReprioritizePending(self):
        """
        Reprioritize a pending job.
        """
        dq = ResizableDispatchQueue(None, 0)
        dq.put('xxx', 5)
        pending = dq.pending()
        job = pending.pop()
        dq.reprioritize(job, 10)
        self.assertEqual(10, job.priority)


class TestTiming(unittest.TestCase):
    """
    Tests of the ResizableDispatchQueue class that involve testing the
    timing of events.
    """
    def slow(self, jobarg):
        return task.deferLater(reactor, 0.2, lambda x: x, jobarg)

    @defer.inlineCallbacks
    def _stopAndTest(self, when, dq, expectedPending):
        pendingJobs = yield task.deferLater(reactor, when, dq.stop)
        pendingArgs = [p.jobarg for p in pendingJobs]
        self.assertEqual(sorted(expectedPending), sorted(pendingArgs))
        self.assertEqual(dq.size(), (0, len(pendingJobs)))

    def _testPending(self, dq, expected):
        pendingJobs = dq.pending()
        pendingArgs = [p.jobarg for p in pendingJobs]
        self.assertEqual(sorted(expected), sorted(pendingArgs))

    def _testSize(self, dq, expected):
        self.assertEqual(expected, dq.size())

    def _testPaused(self, dq):
        self.assertTrue(dq.paused)

    def _testNotPaused(self, dq):
        self.assertFalse(dq.paused)

    def _testUnderway(self, dq, expected):
        self.assertEqual(expected,
                         set(job.jobarg for job in dq.underway()))

    def testJustOne(self):
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, range(3))
        # This should finish in about 0.2 seconds, with 2 things still on
        # the queue because we stop it after 0.001 so task 0 is already
        # executing but 1 and 2 are queued.
        return self._stopAndTest(0.001, dq, [1, 2])

    def testJustOneWithZeroInitialWidth(self):
        # Same as testJustOne (above), except we initialize with zero width
        # and start the dispatcher by explicitly setting its width.
        dq = ResizableDispatchQueue(self.slow)
        map(dq.put, range(3))
        self.assertEqual(0, dq.width)
        dq.width = 1
        return self._stopAndTest(0.0001, dq, [1, 2])

    def testOneUnderway(self):
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, range(3))
        reactor.callLater(0.01, self._testUnderway, dq, set([0]))
        return self._stopAndTest(0.02, dq, [1, 2])

    @defer.inlineCallbacks
    def testStopAndCancelWithOneUnderway(self):
        """
        Start a dispatch queue of width 2, and send it 3 jobs. Verify that
        2 of the jobs are underway. Then stop it before they can complete,
        telling it to cancel the underway jobs. The two jobs that were
        underway should both be cancelled and returned by the stop method.
        The first 2 jobs returned should have state CANCELLED, and the
        final one should still be PENDING.
        """
        def ok(result):
            self.fail('Unexpected success!')

        def checkCancel(failure):
            self.assertEqual(failure.value.state, Job.CANCELLED)

        dq = ResizableDispatchQueue(self.slow, 2)
        dq.put(0).addCallbacks(ok, checkCancel)
        dq.put(1).addCallbacks(ok, checkCancel)
        dq.put(2)
        reactor.callLater(0.01, self._testUnderway, dq, set([0, 1]))
        pendingJobs = yield task.deferLater(
            reactor, 0.1, dq.stop, cancelUnderway=True)
        pendingArgs = [p.jobarg for p in pendingJobs]
        self.assertEqual([0, 1, 2], sorted(pendingArgs))
        self.assertEqual(pendingJobs[0].state, Job.CANCELLED)
        self.assertEqual(pendingJobs[1].state, Job.CANCELLED)
        self.assertEqual(pendingJobs[2].state, Job.PENDING)

    def testSequential(self):
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, range(3))
        # This should finish in about 0.6 seconds, with nothing still on
        # the queue because we stop after 0.5 seconds so all three tasks
        # will have been dispatched one after another.
        return self._stopAndTest(0.5, dq, [])

    def testParallel(self):
        dq = ResizableDispatchQueue(self.slow, 3)
        map(dq.put, range(3))
        # This should finish in about 0.2 seconds, with nothing on the
        # queue.  We stop after just 0.001 seconds. All three tasks should
        # be dispatched immediately.
        return self._stopAndTest(0.001, dq, [])

    def testWiden(self):
        dq = ResizableDispatchQueue(self.slow, 2)
        map(dq.put, range(8))
        reactor.callLater(0.1, dq.setWidth, 10)
        # This should finish in about 0.4 seconds, with nothing on the
        # queue. We stop after 0.25 seconds. All tasks will have been
        # dispatched (width is initially 2 but is increased to 10 after the
        # first pair of jobs are underway).
        return self._stopAndTest(0.25, dq, [])

    def testNarrow(self):
        dq = ResizableDispatchQueue(self.slow, 2)
        map(dq.put, range(8))
        reactor.callLater(0.1, dq.setWidth, 1)
        # This should finish in about 0.4 seconds, with 3-7 still on the
        # queue. 0 and 1 are dispatched immediately, then we narrow to
        # width one, so only 2 is then dispatched before the stop happens.
        return self._stopAndTest(0.3, dq, [3, 4, 5, 6, 7])

    def testNarrowNarrow(self):
        dq = ResizableDispatchQueue(self.slow, width=3)
        map(dq.put, range(8))
        reactor.callLater(0.1, dq.setWidth, 2)
        reactor.callLater(0.3, dq.setWidth, 1)
        # This should finish in about 0.6 seconds, with 6 & 7 still on the
        # queue. 0, 1 and 2 are dispatched immediately, then we narrow to
        # width two, so 3 and 4 are dispatched in the next phase. Then we
        # narrow to one and 5 is dispatched before we stop at 0.5.
        return self._stopAndTest(0.5, dq, [6, 7])

    def testNarrowNarrowWiden(self):
        dq = ResizableDispatchQueue(self.slow, 3)
        map(dq.put, range(11))
        reactor.callLater(0.1, dq.setWidth, 2)
        reactor.callLater(0.3, dq.setWidth, 1)
        reactor.callLater(0.7, dq.setWidth, 3)
        return self._stopAndTest(0.9, dq, [10])

    def testNarrowNarrowWidenNarrow(self):
        items = range(14)
        dq = ResizableDispatchQueue(self.slow, 3)
        map(dq.put, items)
        # jobs 0, 1, 2 will be dispatched at time 0.00
        reactor.callLater(0.1, dq.setWidth, 2)
        reactor.callLater(
            0.1, self._testPending, dq, [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13])
        # At time 0.20 jobs 0, 1, and 2 will all finish, but the
        # queue now has width 2, so 3 and 4 will be dispatched.
        reactor.callLater(
            0.25, self._testPending, dq, [5, 6, 7, 8, 9, 10, 11, 12, 13])
        reactor.callLater(0.3, dq.setWidth, 1)
        # 3 and 4 will finish at 0.40, and the queue is now width 1, so 5
        # will be dispatched at 0.40. Then 6 will be dispatched at 0.60.
        reactor.callLater(0.7, dq.setWidth, 3)
        # At 0.7 the queue is widened to 3, so 2 more jobs (7, 8) will be
        # dispatched. 6 will finish at 0.80, so one more job (9) will be
        # dispatched at 0.8.
        reactor.callLater(0.85, dq.setWidth, 1)
        # 7 and 8 will have finished at 0.9, at which point the queue is
        # down to size 1, but 9 is still in progress. At 1.0, 9 will finish
        # and 10 will launch. At 1.20, 10 will finish and 11 will be
        # dispatched.  11 will finish at about 1.40.
        return self._stopAndTest(1.35, dq, [12, 13])

    @defer.inlineCallbacks
    def testPause(self):
        items = range(3)
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, items)
        reactor.callLater(0.2, self._testPaused, dq)
        yield task.deferLater(reactor, 0.1, dq.pause)
        pendingJobs = dq.pending()
        self.assertEqual([p.jobarg for p in pendingJobs], [1, 2])

    @defer.inlineCallbacks
    def testPauseStop(self):
        items = range(3)
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, items)
        d1 = task.deferLater(reactor, 0.1, dq.pause)
        d2 = self._stopAndTest(0.15, dq, [1, 2])
        yield d1
        yield d2

    @defer.inlineCallbacks
    def testPauseAddStop(self):
        items = range(3)
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, items)
        d1 = task.deferLater(reactor, 0.1, dq.pause)
        reactor.callLater(0.12, dq.put, None)
        reactor.callLater(0.14, dq.put, 'hello')
        reactor.callLater(0.16, dq.put, 'world')
        d2 = self._stopAndTest(0.18, dq, [1, 2, None, 'hello', 'world'])
        yield d1
        yield d2

    @defer.inlineCallbacks
    def testPauseResumeStop(self):
        """
        Pause a queue, then resume it, then stop it and check its contents.
        """
        items = range(3)
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, items)
        d1 = task.deferLater(reactor, 0.1, dq.pause)
        reactor.callLater(0.5, dq.resume)
        reactor.callLater(0.51, self._testNotPaused, dq)
        d2 = self._stopAndTest(0.6, dq, [2])
        yield d1
        yield d2

    @defer.inlineCallbacks
    def testPauseStopResume(self):
        """
        Pause a queue, then stop it, then check that trying to resume it
        gets a C{QueueStopped} exception.
        """
        items = range(3)
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, items)
        d1 = task.deferLater(reactor, 0.1, dq.pause)
        d2 = self._stopAndTest(0.2, dq, [1, 2])
        d3 = task.deferLater(reactor, 0.3, dq.resume)
        yield d1
        yield d2
        yield self.failUnlessFailure(d3, QueueStopped)

    @defer.inlineCallbacks
    def testPauseWidenResumeStop(self):
        items = range(3)
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, items)
        d1 = task.deferLater(reactor, 0.1, dq.pause)
        reactor.callLater(0.2, dq.setWidth, 2)
        reactor.callLater(0.5, dq.resume)
        d2 = self._stopAndTest(0.6, dq, [])
        yield d1
        yield d2

    @defer.inlineCallbacks
    def testPausePlayAroundResumeStop(self):
        # As above, with some playing around while paused.
        items = range(3)
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, items)
        d1 = task.deferLater(reactor, 0.1, dq.pause)
        reactor.callLater(0.12, dq.setWidth, 100)
        reactor.callLater(0.16, dq.setWidth, 0)
        reactor.callLater(0.2, dq.setWidth, 2)
        reactor.callLater(0.5, dq.resume)
        d2 = self._stopAndTest(0.6, dq, [])
        yield d1
        yield d2

    def testSizeAfterStarting(self):
        dq = ResizableDispatchQueue(self.slow, 1)
        map(dq.put, range(3))
        reactor.callLater(0.04, self._testSize, dq, (1, 2))
        return self._stopAndTest(0.1, dq, [1, 2])

    def testSizeAfterWiden(self):
        """
        Do some queue size testing while adjusting queue width.
        """
        dq = ResizableDispatchQueue(self.slow, 2)
        map(dq.put, range(8))
        reactor.callLater(0.02, self._testSize, dq, (2, 6))
        reactor.callLater(0.05, dq.setWidth, 4)
        reactor.callLater(0.07, self._testSize, dq, (4, 4))
        reactor.callLater(0.1, dq.setWidth, 10)
        reactor.callLater(0.15, self._testSize, dq, (8, 0))
        reactor.callLater(0.24, self._testSize, dq, (6, 0))
        reactor.callLater(0.28, self._testSize, dq, (4, 0))
        reactor.callLater(0.33, self._testSize, dq, (0, 0))
        # We stop after 0.35 seconds. All tasks will have been dispatched
        # (width is initially 2 but is increased to 10 after the first pair
        # of jobs are underway).
        return self._stopAndTest(0.35, dq, [])

    @defer.inlineCallbacks
    def testCancelRemovesFromQueue(self):
        """
        Cancelling a job should remove it from the queue.
        """
        def eb(failure):
            self.assertEqual(failure.value.state, Job.CANCELLED)

        dq = ResizableDispatchQueue(self.slow, 1)
        dq.put('a', 1)
        deferred = dq.put('b', 2)
        deferred.addErrback(eb)
        deferred.cancel()
        yield task.deferLater(reactor, 0.1, dq.pause)
        pendingJobs = dq.pending()
        self.assertEqual(pendingJobs, [])

    @defer.inlineCallbacks
    def testSetWidthToZeroAfterInitiallyNonZero(self):
        """
        Make sure that a queue whose width is initially non-zero and which
        is then set to zero width does not then begin to process any added
        jobs.
        """
        dq = ResizableDispatchQueue(self.slow, 3)
        yield task.deferLater(reactor, 0.01, dq.setWidth, 0)
        reactor.callLater(0.01, dq.put, 'aaa')
        reactor.callLater(0.01, dq.put, 'bbb')
        yield task.deferLater(reactor, 0.05, self._testSize, dq, (0, 2))
        remaining = yield dq.stop()
        self.assertEqual(['aaa', 'bbb'], [job.jobarg for job in remaining])

    @defer.inlineCallbacks
    def testPuttingMoreThanWidthOnlyDispatchesWidth(self):
        """
        Make sure that if we put 5 things onto a queue whose width is 3
        that shortly thereafter there are 3 jobs underway and 2 pending.
        """
        dq = ResizableDispatchQueue(self.slow, 3)
        for value in range(5):
            reactor.callLater(0.01, dq.put, value)
        yield task.deferLater(reactor, 0.1, self._testSize, dq, (3, 2))
        yield dq.stop()


class TestDeferredErrorHandling(unittest.TestCase):
    """
    Check that errors raised when processing a job are handled correctly. These
    tests were added in response to the problem desribed at
    http://stackoverflow.com/questions/9728781/\
    testing-a-failing-job-in-resizabledispatchqueue-with-trial
    """

    def testRaiseFailsWithJob(self):
        """
        Raising an exception in the job handler should result in a failure
        that contains a L{txrdq.job.Job} value.
        """
        def raiseException(jobarg):
            raise Exception()

        dq = ResizableDispatchQueue(raiseException, 1)
        d = dq.put('Some argument', 1)
        return self.assertFailure(d, Job)

    @defer.inlineCallbacks
    def testRaiseFailsWithCorrectJobArgument(self):
        """
        Raising an exception in the job handler should result in a failure
        that contains a job with the original job argument.
        """
        result = []

        def raiseException(jobarg):
            raise Exception()

        def extractJobarg(failure):
            result.append(failure.value.jobarg)

        dq = ResizableDispatchQueue(raiseException, 1)
        yield dq.put('Some argument', 1).addErrback(extractJobarg)
        self.assertEqual(['Some argument'], result)
