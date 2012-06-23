# Copyright (c) 2010--2011 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from ciel.public.references import SW2_StreamReference, SW2_FixedReference
import datetime
import time

TASK_CREATED = -1
TASK_BLOCKING = 0
TASK_SELECTING = 1
TASK_RUNNABLE = 2
TASK_QUEUED_STREAMING = 3
TASK_QUEUED = 4
TASK_ASSIGNED = 6
TASK_COMMITTED = 7
TASK_FAILED = 8
TASK_ABORTED = 9

TASK_STATES = {'CREATED': TASK_CREATED,
               'BLOCKING': TASK_BLOCKING,
               'SELECTING': TASK_SELECTING,
               'RUNNABLE': TASK_RUNNABLE,
               'QUEUED_STREAMING': TASK_QUEUED_STREAMING,
               'QUEUED': TASK_QUEUED,
               'ASSIGNED': TASK_ASSIGNED,
               'COMMITTED': TASK_COMMITTED,
               'FAILED': TASK_FAILED,
               'ABORTED': TASK_ABORTED}

TASK_STATE_NAMES = {}
for (name, number) in TASK_STATES.items():
    TASK_STATE_NAMES[number] = name

class TaskPoolTask:
    """
    Interesting fields:

    task_id -- the ID of the task
    parent -- either None or the task which spawned us
    children -- a list of tasks which we've spawned.  This is
                maintained by DynamicTaskGraph::spawn() and
                RecoveryManager::load_other_tasks_for_job, rather than
                directly by this class.
    handler -- a string which says which task handler to use.  This is
               used by Job::assign_scheduling_class_to_task() to figure
               out a default scheduling class and also
               TaskExecutionRecord::run() to index into the big table of
               executors.
    inputs -- a dictionary from local reference IDs to references?  It
              looks like this only gets filled out as the inputs
              become available.
    dependencies -- a slightly different dictionary from reference IDs to
                    references?
    expected_outputs -- a list of strings which says what references we expect this
                        task to ultimately produce.
    task_private -- I have no idea what this is, but it looks important
    unfinished_input_streams -- a set of reference IDs which we're
                                consuming in streaming mode.  Only really used
                                so as we can go from QUEUED_STREAMING to
                                just QUEUED when the last one becomes ready.
                                Also updated by DynamicTaskGraph::reduce_graph_for_tasks().
    constrained_location -- Either None or a netloc.  If it's a netloc then
                            one of our inputs is a FixedRef which forces us
                            to run on that netloc.
    constrained_location_checked -- True iff constrained_location has been
                                    calculated.  XXX SOS22: Not quite sure
                                    why this is being done lazily; it's
                                    not like it's a very expensive thing to
                                    compute.
    _blocking_dict -- a dictionary from global IDs to sets of local
                      IDs.  The global IDs are the references which
                      we're blocked on, and the local IDs are the
                      matching IDs in the inputs dictionary.  When
                      this goes empty the task becomes runnable.
    history -- a list of two-tuples.  The first argument is a time,
               and the second is either a description of the thing
               which happened as a string or a two-tuple of the
               description and some additional information.  Pulled
               out by the task crawler as a debug aid; not otherwise
               used.
    job -- either the job this task is part of or an instance of DummyJob.
           This class only uses it for logging, but lots of other places
           make more sensible use of it.
    taskset --
    worker_private -- a string->string dictionary of information for the
                      worker.  The only key is ``hint'', which is either
                      ``small_task'' or not present at all.
    type -- Either None or the task type as a string.  Only set from the initialiser,
            only accessed by get_type().
    worker -- None or a Worker.  Set when the task is assigned to a worker, but not
              cleared when the task completes (unless the worker fails and we need
              to migrate the task).
    state -- None or a state identifier (i.e. an index into
             TASK_STATE_NAMES).  This should only be changed by
             set_state.
    scheduling_class -- either \"cpu\" or \"disk\".  Used when considering scheduling
                        tasks together so that we don't put lots of disk-heavy tasks
                        on the same machine.
    current_attempt -- the number of times that we've tried to run this task so
                       far.  Maintained by the job pool rather than by the task
                       itself.
    profiling -- a dictionary from strings to timestamps, measured as seconds
                 since the POSIX epoch.  The strings are descriptions of interesting
                 events, and the timestamps are when they happened.  Events include
                 at least CREATED, STARTED, and FINISHED.

    There are a couple of less interesting fields, as well:
    
    event_index -- 0
    saved_continuation_uri -- None

    """
    def __init__(self, task_id, parent_task, handler, inputs, dependencies, expected_outputs, task_private=None, state=TASK_CREATED, job=None, taskset=None, worker_private=None, workers=[], scheduling_class=None, type=None):
        self.task_id = task_id
        
        # Task creation graph.
        self.parent = parent_task
        self.children = []
        
        self.handler = handler
        
        self.inputs = inputs
        self.dependencies = dependencies
            
        self.expected_outputs = expected_outputs

        self.task_private = task_private

        self.unfinished_input_streams = set()

        self.constrained_location_checked = False
        self.constrained_location = None

        self._blocking_dict = {}
            
        self.history = []
        
        self.job = job

        self.taskset = taskset
        
        self.worker_private = worker_private
        
        self.type = type
        
        self.worker = None
        
        self.state = None
        self.set_state(state)
        
        #self.worker = None
        self.scheduling_class = scheduling_class
        
        self.saved_continuation_uri = None

        self.event_index = 0
        self.current_attempt = 0
        
        self.profiling = {}

    def __str__(self):
        return 'TaskPoolTask(%s)' % self.task_id

    def set_state(self, state, additional=None):
        if self.job is not None and self.state is not None:
            self.job.record_state_change(self, self.state, state, additional)
        self.record_event(TASK_STATE_NAMES[state], additional=additional)
        #print self, TASK_STATE_NAMES[self.state] if self.state is not None else None, '-->', TASK_STATE_NAMES[state] if state is not None else None
        self.state = state
        
    def record_event(self, description, time=None, additional=None):
        """
        Add an event to the debug history.  description should be a
        string describing the event.  additional can be anything which
        you think is interesting.  This gets interpreted in various
        exciting ways by the task crawler.
        """
        if time is None:
            time = datetime.datetime.now()
        if additional is not None:
            self.history.append((time, (description, additional)))
        else:
            self.history.append((time, description))
        
    def is_blocked(self):
        return self.state == TASK_BLOCKING
    
    def is_queued_streaming(self):
        return self.state == TASK_QUEUED_STREAMING
        
    def blocked_on(self):
        if self.state == TASK_BLOCKING:
            return self._blocking_dict.keys()
        else:
            return []

    def set_profiling(self, profiling):
        if profiling is not None:
            self.profiling.update(profiling)
            try:    
                self.record_event('WORKER_CREATED', datetime.datetime.fromtimestamp(profiling['CREATED']))
                self.record_event('WORKER_STARTED', datetime.datetime.fromtimestamp(profiling['STARTED']))
                self.record_event('WORKER_FINISHED', datetime.datetime.fromtimestamp(profiling['FINISHED']))
            except KeyError:
                pass
    
    def get_type(self):
        if self.type is None:
            # Implicit task type assigned from the executor name, the number of inputs and the number of outputs.
            # FIXME: Obviously, we could do better.
            return '%s:%d:%d' % (self.handler, len(self.inputs), len(self.expected_outputs))
        else:
            return self.type
    
    def get_profiling(self):
        return self.profiling

    def set_worker(self, worker):
        self.set_state(TASK_ASSIGNED, additional=worker.netloc)
        self.worker = worker

    def unset_worker(self, worker):
        # XXX SOS22 this might leave us in state ASSIGNED, which
        # doesn't sound like a good idea.
        assert self.worker is worker
        self.worker = None

    def get_worker(self):
        """Returns the worker to which this task is assigned, or None
        if it doesn't have one."""
        return self.worker

    def block_on(self, global_id, local_id):
        # Note that we might already be in state BLOCKING here, in which
        # case we just add the new ID to the blocking set.
        self.set_state(TASK_BLOCKING)
        try:
            self._blocking_dict[global_id].add(local_id)
        except KeyError:
            self._blocking_dict[global_id] = set([local_id])
            
    def notify_ref_table_updated(self, ref_table_entry):
        # Called by the task graph when a new input reference becomes
        # consumable or when <something> happens to a streamable
        # reference.
        global_id = ref_table_entry.ref.id
        ref = ref_table_entry.ref
        if global_id in self.unfinished_input_streams:
            self.unfinished_input_streams.remove(global_id)
            ref_table_entry.remove_consumer(self)
            if len(self.unfinished_input_streams) == 0:
                if self.state == TASK_QUEUED_STREAMING:
                    self.set_state(TASK_QUEUED)
        else:
            if self.state == TASK_BLOCKING:
                local_ids = self._blocking_dict.pop(global_id)
                for local_id in local_ids:
                    self.inputs[local_id] = ref
                if isinstance(ref, SW2_StreamReference):
                    # Stay subscribed; this ref is still interesting
                    self.unfinished_input_streams.add(global_id)
                else:
                    # Don't need to hear about this again
                    ref_table_entry.remove_consumer(self)
                if len(self._blocking_dict) == 0:
                    self.set_state(TASK_RUNNABLE)
        
    def convert_dependencies_to_futures(self):
        """Convert all of our input references to future references.
        This is used if we've tried to run the task and found that at
        least one of the inputs was unexpectedly unavailable, so we
        just back off and try to fetch all of them again.  It would be
        possible to be more cunning here and only convert the failed
        inputs, but this is much easier, and we're a long way from the
        fast path here."""
        # XXX SOS22 this discards FixedReferences, which doesn't sound
        # like a great idea (although we do want to discard the
        # various netloc hints).
        # Also, I'm not sure how this would interact with SWDataValue
        # references; discarding the value probably isn't a great
        # idea.
        new_deps = {}
        for local_id, ref in self.dependencies.items(): 
            new_deps[local_id] = ref.as_future()
        self.dependencies = new_deps

    def has_constrained_location(self):
        for dep in self.dependencies.values():
            if isinstance(dep, SW2_FixedReference):
                self.constrained_location = dep.fixed_netloc
        self.constrained_location_checked = True
                
    def get_constrained_location(self):
        if not self.constrained_location_checked:
            self.has_constrained_location()
        return self.constrained_location

    def as_descriptor(self, long=False):        
        descriptor = {'task_id': self.task_id,
                      'dependencies': self.dependencies.values(),
                      'handler': self.handler,
                      'expected_outputs': self.expected_outputs,
                      'inputs': self.inputs.values(),
                      'event_index': self.event_index,
                      'job' : self.job.id}
        
        descriptor['parent'] = self.parent.task_id if self.parent is not None else None
        
        if long:
            descriptor['history'] = map(lambda (t, name): (time.mktime(t.timetuple()) + t.microsecond / 1e6, name), self.history)
            descriptor['state'] = TASK_STATE_NAMES[self.state]
            descriptor['children'] = [x.task_id for x in self.children]
            descriptor['profiling'] = self.profiling
            descriptor['worker'] = self.worker.netloc if self.worker is not None else None
        
        if self.task_private is not None:
            descriptor['task_private'] = self.task_private
        if self.scheduling_class is not None:
            descriptor['scheduling_class'] = self.scheduling_class
        if self.type is not None:
            descriptor['scheduling_type'] = self.type
        
        return descriptor

class DummyJob:
    """Used to ensure that tasks on the worker can refer to their job (for inheriting job ID, e.g.)."""
    
    def __init__(self, id):
        self.id = id
        
    def record_state_change(self, task, from_state, to_state, additional=None):
        pass

def build_taskpool_task_from_descriptor(task_descriptor, parent_task=None, taskset=None):

    task_id = task_descriptor['task_id']

    handler = task_descriptor['handler']
    
    if parent_task is not None:
        job = parent_task.job
    else:
        try:
            job = DummyJob(task_descriptor['job'])
        except KeyError:
            job = DummyJob(None)
    
    try:
        inputs = dict([(ref.id, ref) for ref in task_descriptor['inputs']])
    except KeyError:
        inputs = {}
        
    dependencies = dict([(ref.id, ref) for ref in task_descriptor['dependencies']])
    expected_outputs = task_descriptor['expected_outputs']

    try:
        task_private = task_descriptor['task_private']
    except KeyError:
        task_private = None

    try:
        worker_private = task_descriptor['worker_private']
    except KeyError:
        worker_private = {}

    try:
        workers = task_descriptor['workers']
    except KeyError:
        workers = []

    try:
        scheduling_class = task_descriptor['scheduling_class']
    except KeyError:
        if parent_task is not None:
            # With no other information, scheduling class is inherited from the parent.
            scheduling_class = parent_task.scheduling_class
        else:
            scheduling_class = None
    
    try:
        type = task_descriptor['scheduling_type']
    except KeyError:
        type = None
    
    state = TASK_CREATED
    
    return TaskPoolTask(task_id, parent_task, handler, inputs, dependencies, expected_outputs, task_private, state, job, taskset, worker_private, workers, scheduling_class, type)
