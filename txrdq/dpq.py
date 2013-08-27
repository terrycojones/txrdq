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

import itertools
from heapq import heappush, heappop
from collections import deque
from twisted.internet import defer


class _Task(object):
    """
    Holds details of a task in a C{DeferredPriorityQueue}.

    @ivar priority: The priority of the task. Lower is more important.

    @ivar count: Used to maintain a stable (sort) order of tasks that have
    the same priority.

    @ivar valid: if C{False}, this task is no longer valid (i.e., it has
    been deleted).

    @ivar obj: The object that was put into the C{DeferredPriorityQueue}.
    """
    def __init__(self, obj, priority, count):
        self.obj = obj
        self.priority = priority
        self.count = count
        self.valid = True

    def __lt__(self, other):
        return (self.priority, self.count) < (other.priority, other.count)


class DeferredPriorityQueue(object):
    """
    A prioritized event-driven queue.

    Objects may be added as usual to this queue.  When an attempt is made
    to retrieve an object when the queue is empty, a C{Deferred} is
    returned which will fire when an object becomes available.

    Note that the same object cannot currently be in the queue twice at the
    same time.  This is because the simple support for delete and
    reprioritize makes doing duplicated objects awkward. Attempts to re-put
    an object already in the queue will raise (see docs for put below).

    Implementation adapted from http://docs.python.org/library/heapq.html
    and from Twisted's twisted.internet.defer.DeferredQueue class.

    @ivar size: The maximum number of objects to allow into the queue at a
    time.  When an attempt to add a new object would exceed this limit,
    C{defer.QueueOverflow} is raised synchronously.  C{None} for no limit.

    @ivar backlog: The maximum number of C{Deferred} gets to allow at one
    time.  When an attempt is made to get an object which would exceed this
    limit, C{defer.QueueUnderflow} is raised synchronously.  C{None} for no
    limit.
    """

    def __init__(self, size=None, backlog=None):
        self.size = size
        self.backlog = backlog
        self.waiting = deque()  # Deferreds awaiting an object.
        self._counter = itertools.count(1)  # Unique sequence count.
        self._objFinder = {}  # Mapping of queued objects to tasks.
        self.clear()

    def clear(self):
        """
        Clear the pending queue.
        """
        # self._pending is a heapq of queued C{Task} instances
        # (some of which may be marked as invalid).
        self._pending = []
        self._nInvalid = 0  # The number of invalid (deleted) queued instances.

    def put(self, obj, priority, count=None):
        """
        Add an object to this queue.

        @param priority: the priority for the object. Lower is more
        important.

        @param obj: the object to add to the queue.

        @param count: A count used to maintain a stable (sort) order of
        tasks that have the same priority.

        @raise defer.QueueOverflow: Too many objects are in the queue.

        @raise RuntimeError: If this object already exists in the queue.
        """
        if self.waiting:
            self.waiting.popleft().callback(obj)
        elif self.size is None or len(self) < self.size:
            try:
                existing = self._objFinder[obj]
            except KeyError:
                pass
            else:
                if existing.valid:
                    raise RuntimeError("Object %r already in queue." % (obj,))
            if count is None:
                count = self._counter.next()
            task = _Task(obj, priority, count)
            self._objFinder[obj] = task
            heappush(self._pending, task)
        else:
            raise defer.QueueOverflow()

    def get(self):
        """
        Attempt to retrieve and remove the highest priority object from the
        queue.

        @return: a L{Deferred} which fires with the highest priority object
        available in the queue.

        @raise QueueUnderflow: Too many (more than C{backlog})
        L{Deferred}s are already waiting for an object from this queue.
        """
        while self._pending:
            task = heappop(self._pending)
            del self._objFinder[task.obj]
            if not task.valid:
                self._nInvalid -= 1
            else:
                return defer.succeed(task.obj)
        if self.backlog is None or len(self.waiting) < self.backlog:
            d = defer.Deferred(canceller=self._cancelGet)
            self.waiting.append(d)
            return d
        else:
            raise defer.QueueUnderflow()

    def delete(self, obj):
        """
        Delete an object from the queue.

        @raise: C{KeyError} if the object is not in the queue or is not
        valid.

        @param obj: The object to delete.
        """
        task = self._objFinder[obj]
        if task.valid:
            self._nInvalid += 1
            task.valid = False
        else:
            raise KeyError(obj)

    def reprioritize(self, obj, priority):
        """
        Set the priority of a pending task to priority.

        @raise: C{KeyError} if the object is not in the queue or is not
        valid.

        @param obj: The object whose priority should be altered.

        @param priority: The new priority. Lower is more important.
        """
        existing = self._objFinder[obj]
        if existing.valid:
            if existing.priority != priority:
                count = existing.count
                # Note: set existing.valid to False before calling
                # self.put, or it will complain that the obj is already in
                # the queue.
                existing.valid = False
                self.put(obj, priority, count)
        else:
            raise KeyError(obj)

    def _cancelGet(self, d):
        """
        Remove a deferred d from our waiting list, as the deferred has been
        canceled.

        Note: We do not need to wrap this in a try/except to catch d not
        being in self.waiting because this canceller will not be called if
        d has fired. put() pops a deferred out of self.waiting and calls
        it, so the canceller will no longer be called.

        @param d: The deferred that has been canceled.
        """
        self.waiting.remove(d)

    def asGenerator(self):
        """
        Return the queue contents as a generator, excluding all invalid
        entries.

        Note: the queued objects are returned in heap order, which is not
        the same as sorted order. If you need the objects sorted by
        priority, you can call sort on the result.
        """
        return (task.obj for task in self._pending if task.valid)

    def __len__(self):
        """
        The number of things in the queue is the length of the pending
        list, leaving out those tasks marked as invalid.
        """
        return len(self._pending) - self._nInvalid
