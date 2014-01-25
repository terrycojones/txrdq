# Copyright 2010-2013 Fluidinfo Inc.
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

from twisted.internet import task
from twisted.python import log

from txrdq.dpq import DeferredPriorityQueue
from txrdq.pool import DeferredPool
from txrdq.job import Job


class QueueStopped(Exception):
    """
    An exception that is raised when a client does a 'put' on a
    L{ResizableDispatchQueue} that has been permanently stopped.
    """


class ResizableDispatchQueue(object):
    """
    @ivar func: The one-argument function that will be called to process
    each job added to the queue.

    @ivar width: The 'width' of the queue. This is the maximum number of
    jobs that the queue will have in progress at any time. Set to 0
    to pause queue processing.

    @ivar size: The maximum number of objects to allow into the queue at a
    time. See the documentation for twisted.internet.defer.DeferredQueue

    @ivar backlog: The maximum number of L{Deferred} gets to allow at one
    time.  See the documentation for twisted.internet.defer.DeferredQueue.

    @raise ValueError: if width is not an integer or is less than zero.
    """

    _NOOP = object()

    def __init__(self, func, width=0, size=None, backlog=None):
        self._func = func
        self.stopped = self.paused = False
        self._queue = DeferredPriorityQueue(size, backlog)
        self._pool = DeferredPool()
        self._coop = task.Cooperator()
        self._currentWidth = 0
        self.pendingStops = 0
        self._underway = set()
        self.width = width

    def put(self, jobarg, priority=0):
        """
        Add a job to the queue.

        @param jobarg: The argument that will be passed to self._func when
        this job is launched.

        @param priority: The priority for the job. Lower means more
        important.  Default is zero.

        @raise QueueStopped: if the queue has been stopped.

        @return: a C{Deferred} that will eventually fire with the result of
        calling self._func on jobarg.
        """
        if self.stopped:
            raise QueueStopped()
        else:
            job = Job(jobarg, priority)
            d = job.watch().addBoth(self._jobDone, job)
            self._queue.put(job, priority)
            return d

    def _jobDone(self, result, job):
        """
        A job has finished one way or another. 'result' is either a
        successful (C{Job}) result or a failure (containing a C{Job} that
        failed or was cancelled). The job argument contains the original
        job in any case.

        Remove a finished job from our queue or (if it's no longer there)
        from the set of underway jobs.

        @param result: the result of the job processing.

        @param job: the job that has just finished.

        @return: the result we are passed.
        """
        try:
            self._queue.delete(job)
        except KeyError:
            self._underway.discard(job)
        return result

    def pending(self):
        """
        Get the pending jobs.

        @return: A C{list} of the L{Job}s that are pending (i.e., which
        have not yet begun to be processed).
        """
        # Turn the generator from our pending queue into a list, to make
        # sure we're returning a snapshot of the queue at this moment.  The
        # list can be altered by our caller without affecting us.
        return list(self._queue.asGenerator())

    def underway(self):
        """
        Get the currently underway jobs. These can have cancel called on
        them and can be reprioritized.

        @return: A (copy of) the set of jobs that are underway.
        """
        # We return a copy in case our caller modifies the return value.
        return set(self._underway)

    def clearQueue(self, cancelPending=True):
        """
        Clear the pending list of jobs.

        @param cancelPending: if True, call 'cancel' on the C{Deferred}s
        for the pending jobs.
        """
        if cancelPending:
            jobs = list(self._queue.asGenerator())
            for job in jobs:
                job.cancel()
        self._queue.clear()

    def size(self):
        """
        Provide information about our size.

        @return: A two-tuple with the number of jobs that are underway and
        the number that are pending.
        """
        return (len(self._underway), len(self._queue))

    def _drain(self):
        """
        For a queue that is either stopped or paused, return a deferred
        that fires when the currently underway jobs in the queue have all
        finished.

        @raises RuntimeError: if the queue is not paused or stopped.

        @return: a C{Deferred} that fires when no jobs are underway.
        """
        # The queue must be paused or stopped in order for us to sanely
        # wait for it to drain to zero.
        if not (self.paused or self.stopped):
            raise RuntimeError('Queue is not paused or stopped.')

        # Flush idle job dispatcher (Deferreds) by giving each a no-op job.
        while self._queue.waiting:
            # We pass an arbitrary priority of zero. We know the queue is
            # empty because self._queue.waiting is not empty, so task
            # priority is irrelevant.
            self._queue.put(self._NOOP, 0)

        return self._pool.notifyWhenEmpty()

    def stop(self, cancelUnderway=False):
        """
        Permanently stop the queue. Any subsequent attempt to put a job
        onto the queue will result in an error. The default behavior is to
        wait for underway jobs to finish, but see the docs below on
        cancelUnderway.

        @param cancelUnderway: if True: 1) call cancel on the deferreds for
        all the currently underway jobs, and 2) add those underway jobs to
        the list of pending jobs returned by the C{Deferred} we return.

        @return: a C{Deferred} that fires with the list of pending jobs.
        """
        self.stopped = True
        self.width = 0

        log.msg('Stop called. Underway in queue %r' % self._underway)

        if cancelUnderway:
            underwayJobs = list(self._underway)
            for job in underwayJobs:
                job.cancel()
        else:
            underwayJobs = []
        log.msg('stop called. underwayJobs is %r' % underwayJobs)
        d = self._drain()
        d.addCallback(lambda _: underwayJobs + self.pending())
        return d

    def reprioritize(self, job, priority):
        """
        Set the priority of a pending job to priority.

        @raise: C{RuntimeError} if the job is not PENDING.

        @raise: C{KeyError} if the job does not exist.

        @param job: The job whose priority should be altered.

        @param priority: The new priority. Lower is more important.
        """
        if job.state != Job.PENDING:
            raise RuntimeError('Cannot delete job that is not pending.')
        else:
            self._queue.reprioritize(job, priority)
            job.priority = priority

    def pause(self):
        self._pausedWidth = self.width
        self.width = 0
        self.paused = True
        return self._drain()

    def resume(self, width=None):
        if self.stopped:
            raise QueueStopped()
        if width is None:
            width = self._pausedWidth
        else:
            width = int(width)
            if width < 0:
                raise ValueError('Queue width cannot be negative.')
        self.paused = False
        self.width = width

    def next(self):
        """
        Return the next job to be processed, unless we are currenly
        narrowing the queue, in which case we raise C{StopIteration}.

        @raise StopIteration: if the queue is in the process of being
        narrowed.

        @return: a C{Deferred} that will fire when a job is added to the
        queue.
        """
        if self.pendingStops:
            self.pendingStops -= 1
            self._currentWidth -= 1
            raise StopIteration
        else:
            return self._queue.get().addCallback(self._launchJob)

    def _launchJob(self, job):
        """
        Launch a queued job.

        @param job: a L{Job} to get underway.  If the job is a no-op, do
        nothing.

        @return: If the job is a no-op, return C{None}. Else return a
        C{Deferred} that will fire when the job has completed.
        """
        if job is not self._NOOP:
            self._underway.add(job)
            job.launch(self._func)
            # We don't care about the result of the job here, we just want
            # to return a deferred that fires when the job is done. Ignore
            # any failure in the deferred returned by C{job.watch} so as
            # not to trigger error messages elsewhere (e.g., from trial)
            # about unhandled errors. (The processing of the job result is
            # handled in C{self._jobDone}, which is called by the deferred
            # returned by C{self.put}.)
            return job.watch().addErrback(lambda err: None)

    def getWidth(self):
        """
        Return the current width of the queue.

        Note that this may be greater or less than the number of jobs
        currently underway. For example, the width could be 10 but we may
        have only 7 jobs underway (due to their being no more pending jobs
        to launch). If the width is then reduced to 4, this method will
        report width as 4 even though there are still 7 jobs in progress.

        @return: the current width of the queue.
        """
        if self.paused:
            return self._pausedWidth
        else:
            return self._currentWidth - self.pendingStops

    def setWidth(self, width):
        """
        Set the width of the queue. If the queue is paused, we simply
        remember what the width should be once queue processing is resumed.
        If we're not paused, then the interesting cases are either making
        the queue wider or narrower. To make ourselves wider, we pass self
        to our coiterator the appropriate number of times. To make
        ourselves narrower, we arrange for self.next to raise StopIteration
        the appropriate number of times.

        @param width: the new width (a non-negative integer) for the queue.

        @raises ValueError: if width is not an integer or is less than zero.
        """
        width = int(width)
        if width < 0:
            raise ValueError('Queue width cannot be negative.')
        if self.paused:
            self._pausedWidth = width
        else:
            increment = width - self.width
            if increment > 0:
                # Make ourselves wider.
                #
                # The actual increment is the requested increment minus the
                # any pending stops (i.e., narrows) we have scheduled.
                actualIncrement = increment - self.pendingStops
                if actualIncrement >= 0:
                    self.pendingStops = 0
                    for i in xrange(actualIncrement):
                        self._pool.add(self._coop.coiterate(self))
                    self._currentWidth += actualIncrement
                else:
                    self.pendingStops -= increment
            elif increment < 0:
                # Make ourselves narrower.
                increment = -increment
                self.pendingStops += increment
                # If there are already waiters on our queue, give up to
                # 'increment' of them a no-op. That ensures that those
                # Deferreds that are already awaiting job args will not
                # process the next values put to the RDQ, they will no-op
                # instead. If there aren't waiters it's no problem, the
                # reduction in queue width will be handled by pending
                # stops.
                #
                # I know, this is too complicated :-(
                #
                # The priority on the put below isn't important. The fact
                # that there are waiters means the DeferredPriorityQueue is
                # empty. So the no-op will go straight out to a waiter no
                # matter what priority it has.
                for _ in range(increment):
                    if self._queue.waiting:
                        self._queue.put(self._NOOP, 0)
                    else:
                        break

    width = property(getWidth, setWidth)
