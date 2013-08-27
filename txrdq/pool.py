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


from twisted.internet import defer


class DeferredPool(object):
    """
    Maintain a pool of not-yet-fired deferreds and provide a mechanism to
    request a deferred that fires when the pool size goes to zero.
    """
    def __init__(self):
        self._pool = set()
        self._waiting = []

    def _fired(self, result, d):
        """
        Callback/errback each pooled deferred runs when it fires. The
        deferred first removes itself from the pool. If the pool is then
        empty, fire all the waiting deferreds (which were returned by
        notifyWhenEmpty).
        """
        self._pool.remove(d)
        if not self._pool:
            waiting, self._waiting = self._waiting, []
            for waiter in waiting:
                waiter.callback(None)
        return result

    def add(self, d):
        """
        Add a deferred to the pool.
        """
        d.addBoth(self._fired, d)
        self._pool.add(d)
        return d

    def notifyWhenEmpty(self, testImmediately=True):
        """
        Return a deferred that fires (with None) when the pool empties.  If
        testImmediately is True and the pool is empty, return an already
        fired deferred (via succeed).
        """
        if testImmediately and not self._pool:
            return defer.succeed(None)
        else:
            d = defer.Deferred()
            self._waiting.append(d)
            return d

    def status(self):
        """
        Return a tuple containing the number of deferreds that are
        outstanding and the number of deferreds that are waiting for the
        pool to empty.
        """
        return len(self._pool), len(self._waiting)
