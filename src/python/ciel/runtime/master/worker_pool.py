# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
from __future__ import with_statement
from Queue import Queue
from ciel.public.references import SWReferenceJSONEncoder
from ciel.runtime.pycurl_rpc import post_string_noreturn, get_string
import ciel
import datetime
import logging
import random
import simplejson
import threading
import uuid
from urlparse import urlparse

class Worker:
    """
    A representation of a worker in the worker pool.  Interesting
    fields:

    id -- our worker ID.  A string, but you probably shouldn't assume that.
    netloc -- our netloc, as returned by urlparse.
    features -- ???
    scheduling_classes -- mapping from scheduling classes, as strings,
                          to this worker's capacity in that class.
                          Must always include the default scheduling
                          class '*'.  Must not be modified while
                          the worker is registered with a WorkerPool.
    last_ping -- the time at which the master last received a ping
                 from this worker.  Updated by direct writes from the
                 WorkerPool, without holding any locks.
    failed -- either False if the worker is currently alive or True if
              the master believes it to have failed.  This gets
              updated by direct writes from the WorkerPool.
    worker_pool -- a reference to the owning WorkerPool.
    
    Most of these are protected by the worker pool lock.
    """
    
    def __init__(self, worker_id, worker_descriptor, worker_pool):
        # Note that this is sometimes called holding the worker pool lock
        self.id = worker_id
        self.netloc = worker_descriptor['netloc']
        self.features = worker_descriptor['features']
        self.scheduling_classes = worker_descriptor['scheduling_classes']
        assert self.scheduling_classes.has_key('*')
        self.last_ping = datetime.datetime.now()
        self.failed = False
        self.worker_pool = worker_pool

    def idle(self):
        """Never called, as far as I can tell."""
        pass

    def get_effective_scheduling_class(self, scheduling_class):
        """
        get_effective_scheduling_class(scheduling_class)

        Compare scheduling_class to the set of scheduling classes
        which this worker can handle and return the closest match.
        In practice, that's either scheduling_class itself
        or the default class '*'
        """
        if scheduling_class in self.scheduling_classes:
            return scheduling_class
        else:
            return '*'

    def get_effective_scheduling_class_capacity(self, scheduling_class):
        """
        get_effective_scheduling_class_capacity(scheduling_class)

        Map scheduling_class into an effective scheduling class for
        this worker, using the same algorithm as
        get_effective_scheduling_class, and then return the capacity
        of that class.
        """
        return self.scheduling_classes[self.get_effective_scheduling_class(scheduling_class)]

    def __repr__(self):
        return 'Worker(%s)' % self.id

    def as_descriptor(self):
        return {'worker_id': self.id,
                'netloc': self.netloc,
                'features': self.features,
                'last_ping': self.last_ping.ctime(),
                'failed':  self.failed}
        
class WorkerPool:
    """
    The main worker pool.  Interesting fields:

    bus -- the cherrypy process bus to which we're attached
    deferred_worker -- a DeferredWorkerPool which we use for deferred work
    job_pool -- our main job pool, or None if we don't have one yet.
    workers -- a mapping from worker IDs to Worker objects.  Workers
               are added to this as soon as they are created and removed
               from worker_failed.
    netlocs -- a mapping from netlocs to Worker objects.  Workers are
               added to this as soon as they are created and removed
               from worker_failed.
    _lock -- an rlock protecting all of our important fields
    is_stopping -- set to True when the process bus shuts down,
                   causing the _reap_dead_workers pseudo-thread to
                   exit.
    scheduling_class_capacities -- mapping from scheduling classes to
                                   pairs of (worker, capacity), where
                                   capacity is the number of things of
                                   the given scheduling class which
                                   the worker can run at once.
    scheduling_class_total_capacities -- mapping from scheduling
                                         classes to integers which are
                                         the total capacity of all
                                         workers in the given
                                         scheduling class.

    There are a couple of slightly odd ones as well:

    idle_worker_queue -- a Queue().  Never actually used.
    idle_set -- a set containing all of the worker IDs which we've ever
                used, some of which will be idle, some of which will be
                busy, and some of which will be failed.  Never actually
                read.
    event_count -- this is an integer which starts at 0 and gets
                   incremented whenever event_condvar is notified
                   Never actually read.
    event_condvar -- a condvar which gets notified under the following
                     conditions:
                     -- a new worker is created (or an old worker returns
                        from the dead)
                     -- we receive a ping via worker_ping()
                     -- the server is shutting down
                     Protected by the _lock.
                     Never actually waited on.
    max_concurrent_waiters -- appears to be the constant 5?
    current_waiters -- appears to be the constant 0?
    
    Scheduling classes are almost opaque from the point of view of
    this class.  We just keep track of how many things of each class
    each worker thinks it can run.  There are two exceptions:

    -- We require that they be usable as keys in a dictionary.
    -- We use the special class \"*\" as a kind of default.  If
       someone asks for a class which we don't know about then
       we'll transparently give them class \"*\" instead.
    
    We keep a work item outstanding against the deferred worker pool
    which calls _reap_dead_workers() every ten seconds, after the pool
    has been running for thirty seconds.

    We publish a couple of events on the bus:

    -- schedule -- whenever anyone calls
                   notify_job_about_current_workers???
    
    """
    def __init__(self, bus, deferred_worker, job_pool):
        """
        WorkerPool(bus, deferred_worker, job_pool)

        Create a new worker pool using the give deferred_worker and
        job_pool.  job_pool can be changed after creation, provided
        that the worker pool hasn't been started.

        The worker pool will subscribe to the bus and start and stop
        itself automatically when the bus starts and stops (or at
        least, it will once self.subscribe() has been called).
        """
        assert hasattr(bus, "subscribe")
        assert hasattr(bus, "unsubscribe")
        assert hasattr(bus, "publish")
        assert hasattr(deferred_worker, "do_deferred_after")
        assert job_pool == None or hasattr(job_pool, "notify_worker_added")
        
        self.bus = bus
        self.deferred_worker = deferred_worker
        self.job_pool = job_pool
        self.idle_worker_queue = Queue()
        self.workers = {}
        self.netlocs = {}
        self.idle_set = set()
        self._lock = threading.RLock()
        self.event_count = 0
        self.event_condvar = threading.Condition(self._lock)
        self.max_concurrent_waiters = 5
        self.current_waiters = 0
        self.is_stopping = False
        self.scheduling_class_capacities = {'*' : []}
        self.scheduling_class_total_capacities = {'*' : 0}

    def subscribe(self):
        """
        Subscribe to the bus, so that the worker pool starts and stops
        automatically as the bus does.
        """
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.server_stopping, 10) 
        
    def unsubscribe(self):
        """
        Unsubscribe from the bus, so that the worker pool no longer
        starts and stops as the bus does.  Note that this does not
        automatically stop the worker pool, so can lead to the pool
        becoming something of an unkillable zombie.
        """
        # XXX sos22 unsubscribe() isn't supposed to have a priority;
        # that presumably means that this is never used?
        self.bus.unsubscribe('start', self.start, 75)
        self.bus.unsubscribe('stop', self.server_stopping) 

    def start(self):
        """
        Start the worker pool going.
        """
        self.deferred_worker.do_deferred_after(30.0, self._reap_dead_workers)
        
    def reset(self):
        """
        Try to clear out all the changeable state in the WorkerPool,
        mostly as a debug aid.
        """
        # XXX sos22 this leaves scheduling_class_capacities and
        # scheduling_class_total_capacities in a state which is
        # inconsistent with workers and netlocs!
        self.idle_worker_queue = Queue()
        self.workers = {}
        self.netlocs = {}
        self.idle_set = set()
        
    def _allocate_worker_id(self):
        """
        Generate an ID for a new worker.  This will be globally unique
        with very high probability, but has no meaning beyond that.
        At a python level, the ID is a string, but should probably be
        treated as completely opaque.
        """
        return str(uuid.uuid1())
        
    def create_worker(self, worker_descriptor):
        with self._lock:
            id = self._allocate_worker_id()
            worker = Worker(id, worker_descriptor, self)
            ciel.log.error('Worker registered: %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING, True)
            assert not self.workers.has_key(id)
            self.workers[id] = worker
            previous_worker_at_netloc = self.netlocs.get(worker.netloc, None)
            if previous_worker_at_netloc:
                ciel.log.error('Worker at netloc %s has reappeared' % worker.netloc, 'WORKER_POOL', logging.WARNING)
                self.worker_failed(previous_worker_at_netloc)
            self.netlocs[worker.netloc] = worker
            self.idle_set.add(id)
            self.event_count += 1
            self.event_condvar.notify_all()
            
            for scheduling_class, capacity in worker.scheduling_classes.items():
                try:
                    capacities = self.scheduling_class_capacities[scheduling_class]
                    current_total = self.scheduling_class_total_capacities[scheduling_class]
                except:
                    capacities = []
                    self.scheduling_class_capacities[scheduling_class] = capacities
                    current_total = 0
                capacities.append((worker, capacity))
                self.scheduling_class_total_capacities[scheduling_class] = current_total + capacity

            self.job_pool.notify_worker_added(worker)
            return id

    def notify_job_about_current_workers(self, job):
        """Nasty function included to avoid the race between job creation and worker creation."""
        with self._lock:
            for worker in self.workers.values():
                job.notify_worker_added(worker)

# XXX: This is currently disabled because we don't have a big central list of references.
#        try:
#            has_blocks = worker_descriptor['has_blocks']
#        except:
#            has_blocks = False
#            
#        if has_blocks:
#            ciel.log.error('%s has blocks, so will fetch' % str(worker), 'WORKER_POOL', logging.INFO)
#            self.bus.publish('fetch_block_list', worker)
            
        self.bus.publish('schedule')
        return id
    
    def shutdown(self):
        """
        Tell all of the workers we know about to kill themselves.
        Only sends the RPC; doesn't update any master-side state.
        """
        for worker in self.workers.values():
            try:
                get_string('http://%s/control/kill/' % worker.netloc)
            except:
                pass
        
    def get_worker_by_id(self, id):
        """Convert a worker ID to a Worker object."""
        # XXX SOS22 not sure what the lock is supposed to be
        # protecting here.
        with self._lock:
            return self.workers[id]
        
    def get_all_workers(self):
        """
        Get a snapshot all available worker objects.  Note that
        there's nothing to say that the snapshot will still be
        accurate by the time you actually come to use it.
        """
        # XXX SOS22 not sure what the lock is supposed to be
        # protecting here.
        with self._lock:
            return self.workers.values()
    
    def execute_task_on_worker(self, worker, task):
        """
        Actually run the given task on the given worker.  This only
        does the RPC, and doesn't attempt to update the master state
        at all.

        Can aquire the lock if we hit an error in the RPC operation.
        """
        if worker.failed or self.workers.get(worker.id) != worker or self.netlocs.get(worker.netloc) != worker:
            ciel.log.error("Executing task %s on worker %s which doesn't seem to be properly registered?" % (str(task), str(worker)),
                           "WORKER_POOL",
                           logging.WARNING)
        try:
            ciel.stopwatch.stop("master_task")
            
            message = simplejson.dumps(task.as_descriptor(), cls=SWReferenceJSONEncoder)
            post_string_noreturn("http://%s/control/task/" % (worker.netloc), message, result_callback=self._worker_post_result_callback)
        except:
            self.worker_failed(worker)

    def abort_task_on_worker(self, task, worker):
        """
        Abort the given task, which must be running on the given
        worker.  This only does the RPC, and doesn't attempt to update
        the master state at all.  Note that the arguments are
        the opposite way around to execute_task_on_worker()!

        Can aquire the lock if we hit an error in the RPC operation.
        """
        try:
            ciel.log("Aborting task %s on worker %s" % (task.task_id, worker), "WORKER_POOL", logging.WARNING)
            post_string_noreturn('http://%s/control/abort/%s/%s' % (worker.netloc, task.job.id, task.task_id), "", result_callback=self._worker_post_result_callback)
        except:
            self.worker_failed(worker)
    
    def worker_failed(self, worker):
        """
        worker_failed(worker)

        Notify the worker pool that a worker has failed.  This can be
        called by the worker pool itself or by external users.
        """
        ciel.log.error('Worker failed: %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING, True)
        with self._lock:
            worker.failed = True
            del self.netlocs[worker.netloc]
            del self.workers[worker.id]

            for scheduling_class, capacity in worker.scheduling_classes.items():
                self.scheduling_class_capacities[scheduling_class].remove((worker, capacity))
                self.scheduling_class_total_capacities[scheduling_class] -= capacity
                if self.scheduling_class_total_capacities[scheduling_class] == 0:
                    del self.scheduling_class_capacities[scheduling_class]
                    del self.scheduling_class_total_capacities[scheduling_class]

        if self.job_pool is not None:
            self.job_pool.notify_worker_failed(worker)

    def worker_ping(self, worker):
        """Record that a worker just received a ping."""
        with self._lock:
            self.event_count += 1
            self.event_condvar.notify_all()
        worker.last_ping = datetime.datetime.now()

    def server_stopping(self):
        """The main process bus is shutting down.  Tell our listeners."""
        with self._lock:
            self.is_stopping = True
            self.event_condvar.notify_all()

    def _investigate_worker_failure(self, worker):
        """
        Called by _reap_dead_workers() if the worker has gone too long
        without giving us a ping.
        """
        ciel.log.error('Investigating possible failure of worker %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING)
        try:
            content = get_string('http://%s/control/master/' % worker.netloc)
            worker_fetch = simplejson.loads(content)
            assert worker_fetch['id'] == worker.id
        except:
            self.worker_failed(worker)

    def get_random_worker(self):
        """Select a worker at random according to a uniform distribution."""
        # XXX SOS22 not quite sure what the lock is supposed to be
        # protecting here?  values() copies the list, so we're already
        # working on an atomic snapshot of the worker list.
        with self._lock:
            return random.choice(self.workers.values())
        
    def get_random_worker_with_capacity_weight(self, scheduling_class):
        """
        Select a worker at random weighted according to the worker's
        capacity within a given scheduling class, without reference to
        its current load.  Returns None if the total capacity of the
        class is 0.  If the scheduling class is completely unknown
        then we use '*' instead.
        """
        with self._lock:
            try:
                candidates = self.scheduling_class_capacities[scheduling_class]
                total_capacity = self.scheduling_class_total_capacities[scheduling_class]
            except KeyError:
                scheduling_class = '*'
                candidates = self.scheduling_class_capacities['*']
                total_capacity = self.scheduling_class_total_capacities['*']
        
            if total_capacity == 0:
                return None

            selected_slot = random.randrange(total_capacity)
            curr_slot = 0
            i = 0
            
            for worker, capacity in candidates:
                curr_slot += capacity
                if curr_slot > selected_slot:
                    return worker

            # XXX sos22 this is actually really quite bad; I think it
            # wants to be an abort().
            ciel.log('Ran out of workers in capacity-weighted selection class=%s selected=%d total=%d' % (scheduling_class, selected_slot, total_capacity), 'WORKER_POOL', logging.ERROR)
            
    def get_worker_at_netloc(self, netloc):
        """Return the Worker object for the given netloc, or None."""
        return self.netlocs.get(netloc, None)

    def _reap_dead_workers(self):
        """
        In effect, a worker thread which runs every ten seconds and
        checks for workers which are late pinging us.  If there are
        any, invoke _investigate_worker_failure() in a deferred work
        item.
        """
        if not self.is_stopping:
            for worker in self.workers.values():
                if worker.failed:
                    continue
                if (worker.last_ping + datetime.timedelta(seconds=10)) < datetime.datetime.now():
                    failed_worker = worker
                    self.deferred_worker.do_deferred(lambda: self._investigate_worker_failure(failed_worker))
                    
            self.deferred_worker.do_deferred_after(10.0, self._reap_dead_workers)

    def _worker_post_result_callback(self, success, url):
        # An asynchronous post_string_noreturn has completed against 'url'. Called from the cURL thread.
        if not success:
            parsed = urlparse(url)
            worker = self.get_worker_at_netloc(parsed.netloc)
            if worker is not None:
                ciel.log("Aysnchronous post against %s failed: investigating" % url, "WORKER_POOL", logging.ERROR)
                # Safe to call from here: this bottoms out in a deferred-work call quickly.
                self.worker_failed(worker)
            else:
                ciel.log("Asynchronous post against %s failed, but we have no matching worker. Ignored." % url, "WORKER_POOL", logging.WARNING)

