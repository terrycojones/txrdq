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
from txrdq.job import Job
from twisted.internet import reactor, task


class TestJob(unittest.TestCase):
    """
    Test the L{Job} class used by L{ResizableDispatchQueue}.

    Tests that involve cancellation come in almost-identical pairs. The
    first cancels the job by calling job.cancel() and the second cancels it
    by obtaining a watching C{Deferred} on the job and cancelling that.
    These two ways of cancelling a job will have an identical effect. The
    tests that cancel via cancelling the watching C{Deferred} have
    CancelDeferred in their names and follow immediately after the one that
    cancels the job directly.
    """

    def testInitiallyPending(self):
        """
        A new job should be in the PENDING state.
        """

        job = Job(None, 0)
        self.assertEqual(Job.PENDING, job.state)

    def testLaunchLaunchFails(self):
        """
        Launch a job twice and make sure the second attempt gets a
        C{RuntimeError}.
        """
        job = Job(None, 0)
        job.launch(lambda x: x)
        self.assertRaises(RuntimeError, job.launch, lambda x: x)

    def testWatchCancel(self):
        """
        Watch a job and check that when we cancel it before it has been
        launched the watcher gets an errback with a L{Job} instance whose
        state is CANCELLED.
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertTrue(isinstance(failure.value, Job))
            self.assertEqual(failure.value.state, Job.CANCELLED)

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        job.cancel()
        return deferred

    def testWatchCancelDeferred(self):
        """
        Watch a job and check that when we cancel the watching C{Deferred}
        before the job has been launched the watcher gets an errback with a
        L{Job} instance whose state is CANCELLED.
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertTrue(isinstance(failure.value, Job))
            self.assertEqual(failure.value.state, Job.CANCELLED)

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        deferred.cancel()
        return deferred

    def testWatchLaunchSucceedCheckResult(self):
        """
        Watch a job, launch it, and check the C{Deferred} receives a L{Job}
        instance whose result is correct.
        """

        def cb(result):
            self.assertEqual(result.result, 42)

        job = Job(6, 0)
        deferred = job.watch()
        deferred.addCallback(cb)
        job.launch(lambda x: x * 7)
        return deferred

    def testWatchLaunchSucceed(self):
        """
        Watch a job, launch it, and check the C{Deferred} receives a L{Job}
        instance whose state is FINISHED.
        """

        def cb(result):
            self.assertEqual(result.state, Job.FINISHED)

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallback(cb)
        job.launch(lambda x: x)
        return deferred

    def testLaunchSucceedWatch(self):
        """
        Launch a job that succeeds, then watch it and check the C{Deferred}
        receives a L{Job} instance whose state is FINISHED.
        """

        def cb(result):
            self.assertEqual(result.state, Job.FINISHED)

        job = Job(None, 0)
        job.launch(lambda x: x)
        deferred = job.watch()
        deferred.addCallback(cb)
        return deferred

    def testWatchLaunchFail(self):
        """
        Watch a job that launches and fails. The L{Job} instance in the
        failure must be in the FAILED state. Check the underlying failure
        is as expected (in this case a ZeroDivisionError).
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertEqual(failure.value.state, Job.FAILED)
            self.assertTrue(isinstance(failure.value.failure.value,
                                       ZeroDivisionError))

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        job.launch(lambda x: 1 / 0)
        return deferred

    def testLaunchFailWatch(self):
        """
        Launch a job that fails, then watch it. The L{Job} instance
        received in the failure must be in the FAILED state. Check the
        underlying failure is as expected (in this case a
        ZeroDivisionError).
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertEqual(failure.value.state, Job.FAILED)
            self.assertTrue(isinstance(failure.value.failure.value,
                                       ZeroDivisionError))

        job = Job(None, 0)
        job.launch(lambda x: 1 / 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        return deferred

    def testWatchLaunchSucceedCancel(self):
        """
        Watch a job that is launched and succeeds, and then call cancel on
        it.  The C{Deferred} will receive a Job instance whose state is
        FINISHED (i.e., the cancel will have no effect).
        """

        def cb(result):
            self.assertEqual(result.state, Job.FINISHED)

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallback(cb)
        job.launch(lambda x: x)
        job.cancel()
        return deferred

    def testWatchLaunchSucceedCancelDeferred(self):
        """
        Watch a job that is launched and succeeds, and then call cancel on
        the watching C{Deferred}.  The C{Deferred} will receive a Job instance
        whose state is FINISHED (i.e., the cancel will have no effect).
        """

        def cb(result):
            self.assertEqual(result.state, Job.FINISHED)

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallback(cb)
        job.launch(lambda x: x)
        deferred.cancel()
        return deferred

    def testWatchLaunchFailCancel(self):
        """
        Watch a job that is launched and fails, and then call cancel on
        it.  The C{Deferred} will receive a Job instance whose state is
        FAILED (i.e., the cancel will have no effect).
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertEqual(failure.value.state, Job.FAILED)
            self.assertTrue(isinstance(failure.value.failure.value,
                                       ZeroDivisionError))

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        job.launch(lambda x: 1 / 0)
        job.cancel()
        return deferred

    def testWatchLaunchFailCancelDeferred(self):
        """
        Watch a job that is launched and fails, and then call cancel on the
        watching C{Deferred}.  The C{Deferred} will receive a Job instance
        whose state is FAILED (i.e., the cancel will have no effect).
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertEqual(failure.value.state, Job.FAILED)
            self.assertTrue(isinstance(failure.value.failure.value,
                                       ZeroDivisionError))

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        job.launch(lambda x: 1 / 0)
        deferred.cancel()
        return deferred

    def testLaunchFailWatchCancel(self):
        """
        Launch a job that fails, then watch it, then cancel it.  The
        C{Deferred} will receive a Job instance whose state is FAILED (i.e.,
        the cancel will have no effect).
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertEqual(failure.value.state, Job.FAILED)
            self.assertTrue(isinstance(failure.value.failure.value,
                                       ZeroDivisionError))

        job = Job(None, 0)
        job.launch(lambda x: 1 / 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        job.cancel()
        return deferred

    def testLaunchFailWatchCancelDeferred(self):
        """
        Launch a job that fails, then watch it, then cancel the watching
        C{Deferred}.  The C{Deferred} will receive a Job instance whose
        state is FAILED (i.e., the cancel will have no effect).
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertEqual(failure.value.state, Job.FAILED)
            self.assertTrue(isinstance(failure.value.failure.value,
                                       ZeroDivisionError))

        job = Job(None, 0)
        job.launch(lambda x: 1 / 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        deferred.cancel()
        return deferred

    def testWatchLaunchUnderwayCancel(self):
        """
        Watch a job, launch it with a function that returns a C{Deferred}
        that is slow to fire, then cancel the job. Check the watching
        C{Deferred} receives a L{Job} instance whose state is CANCELLED.
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertEqual(failure.value.state, Job.CANCELLED)

        def run(x):
            return task.deferLater(reactor, 3, lambda x: x)

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        job.launch(run)
        job.cancel()
        return deferred

    def testWatchLaunchUnderwayCancelDeferred(self):
        """
        Watch a job, launch it with a function that returns a C{Deferred}
        that is slow to fire, then cancel the watching C{Deferred}. Check
        the watching C{Deferred} receives a L{Job} instance whose state is
        CANCELLED.
        """

        def cb(result):
            self.fail('Unexpectedly succeeded!')

        def eb(failure):
            self.assertEqual(failure.value.state, Job.CANCELLED)

        def run(x):
            return task.deferLater(reactor, 3, lambda x: x)

        job = Job(None, 0)
        deferred = job.watch()
        deferred.addCallbacks(cb, eb)
        job.launch(run)
        deferred.cancel()
        return deferred
