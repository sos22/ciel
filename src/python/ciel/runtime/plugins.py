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
import ciel
from Queue import Queue
import logging
import threading

class ThreadTerminator:
    pass
THREAD_TERMINATOR = ThreadTerminator()

class AsynchronousExecutePlugin:
    """Thread pool helper class.  Important fields:

    bus -- cherrypy process bus which we use for coordination
    threads -- all of threads we've created
    queue -- a queue of tasks which we are going to eventually execute
    num_threads -- how many worker threads to use
    subscribe_event -- an event on the bus which is used to receive
                       new work items
    publish_success_event -- an optional event on the bus which we
                             notify when a work item is complete (or
                             None).  Published with two arguments: the
                             input which completed, and the result
                             returned by handle_input.
    publish_fail_event -- an optional event on the bus which we notify
                          when a work item fails (i.e. handle_input
                          raises an exception).  Published with two
                          arguments: the input which failed, and the
                          exception object.  If this is None then
                          exeptions get logged as errors and otherwise
                          discarded.
    is_running -- a boolean which is True when the thread pool is running
                  normally or False if it either hasn't been started yet
                  or is trying to shut down.
    """
    def __init__(self, bus, num_threads, subscribe_event=None, publish_success_event=None, publish_fail_event=None):
        """
        AsynchronousExecutePlugin(bus, num_threads, subscribe_event=None, publish_success_event=None, publish_fail_event=None)

        Create a new thread pool with num_threads threads attached to
        the specified cherrypy process bus.  The thread pool is
        started and stopped automatically as the bus starts and stops.

        If subscribe_event is non-None, the thread pool subscribes to
        that event on the bus and checks for more input whenever it
        fires.  The event should be triggered with a single argument
        which is the work item to be run.

        Work items are passed to handle_input() for processing.  The
        default implementation does nothing; most users will want to
        create a derived class and override that method.
        
        The pool does not automatically subscribe to any events when
        it's created; call subscribe() to do that.

        The pool is created in a quiescent state, and will remain so
        until the start() method is called.  This is done
        automatically when the bus starts.  It is a serious error to
        start a pool twice without stopping it in between.
        """
        assert hasattr(bus,"subscribe")
        assert hasattr(bus,"unsubscribe")
        assert (publish_success_event == None and publish_fail_event == None) or hasattr(bus, "publish")

        self.bus = bus
        self.threads = []
        
        self.queue = Queue()
        
        self.num_threads = num_threads
        self.subscribe_event = subscribe_event
        self.publish_success_event = publish_success_event
        self.publish_fail_event = publish_fail_event
        self.is_running = False

    def subscribe(self):
        """
        Subscribe to the bus associated with the thread pool, so that
        the pool starts accepting requests.
        """
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        if self.subscribe_event is not None:
            self.bus.subscribe(self.subscribe_event, self.receive_input)
            
    def unsubscribe(self):
        """
        Unsubscribe from the bus, so that no more requests will be
        received.  Requests which have already been received will
        continue to be processed.  Note that this means that the
        thread pool will not automatically shut down when the bus
        does.
        """
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        if self.subscribe_event is not None:
            self.bus.unsubscribe(self.subscribe_event, self.receive_input)
            
    def start(self):
        """
        Start the thread pool going.
        """
        assert not self.is_running
        self.is_running = True
        for _ in range(self.num_threads):
            t = threading.Thread(target=self.thread_main, args=())
            self.threads.append(t)
            t.start()
                
    def stop(self):
        """
        Stop all of the worker threads.  This waits for the threads to
        terminate, but does not attempt to drain the queue.
        """
        
        assert self.is_running
        self.is_running = False
        for _ in range(self.num_threads):
            self.queue.put(THREAD_TERMINATOR)
        for thread in self.threads:
            thread.join()
        self.threads = []
        if not self.queue.empty():
            ciel.log.error("Stopping an AsynchronousExecutePlugin (%s) with %d outstanding work items" % (str(this), queue.qsize()),
                           'PLUGIN',
                           logging.WARN,
                           True)
            
    def receive_input(self, input=None):
        """
        receive_input(input=None)
        Enqueue a new work item to be processed asynchronously.  input
        is the work item to run; it will be passed to self.handle_input()
        later.
        """
        self.sniff(input)
        self.queue.put(input)
        
    def thread_main(self):
        """
        Main thread function.  Runs in each worker thread.
        """
        while True:
            if not self.is_running:
                break
            input = self.queue.get()
            if input is THREAD_TERMINATOR:
                break

            try:
                result = self.handle_input(input)
                if self.publish_success_event is not None:
                    self.bus.publish(self.publish_success_event, input, result)
            except Exception, ex:
                if self.publish_fail_event is not None:
                    self.bus.publish(self.publish_fail_event, input, ex)
                else:
                    ciel.log.error('Error handling input in %s' % (self.__class__, ), 'PLUGIN', logging.ERROR, True)

    def handle_input(self, input):
        """
        handle_input(self, input)
        
        Override this method to specify the behaviour on processing a
        single input.  This method will be invoked in a worker thread
        for each work item queued to the thread pool.
        """
        pass

    def sniff(self, input):
        """
        sniff(self, input)

        Perform any appropriate checks on the input item before it
        gets added to the queue.  This is never actually required, but
        sometimes allows slightly better error messages.  Default does
        nothing; override in a derived class if you want to do
        anything useful here.
        """
        pass
    
