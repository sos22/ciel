'''
Created on 17 Aug 2010

@author: dgm36
'''
from ciel.runtime.plugins import AsynchronousExecutePlugin
from threading import Timer

class DeferredWorkPlugin(AsynchronousExecutePlugin):
    """
    A plugin for running work items after a delay.  Interesting fields:

    timers -- a dictionary mapping timer ids to Timers.
    current_timer_id -- next unused timer id.
    """
    def __init__(self, bus, event_name="deferred_work"):
        """
        DeferredWorkPlugin(bus, event_name=\"deferred_work\")

        Create a new deferred work worker thread and attach it to the
        bus @bus.  It will listen for new work items on @event_name
        and, when they are received, run them immediately.  These work
        items should be simple callables, and they will be invoked
        with no arguments.  Work items can also be queued for
        immediate execution using do_deferred().

        Alternatively, work items can be enqueued using
        do_deferred_after(), in which case they will only be invoked
        after a timeout.

        Note that there is only one worker thread, so slow items will
        prevent any other items from running.
        
        XXX -- as far as I can tell, nobody ever actually signals that
        event; everything goes through do_deferred_after() and
        do_deferred() instead.
        """
        AsynchronousExecutePlugin.__init__(self, bus, 1, event_name)
        self.timers = {}
        self.current_timer_id = 0
    
    def stop(self):
        """
        Shut the plugin down.  Items which have been queued for
        deferred execution will be abandoned, as will any which have
        been queued to the worker thread but not yet started.  Waits
        until any items which have been started complete.  There's no
        built-in way to tell which items are completed and which are
        abandoned, but it's not particularly difficult to do something
        from the work items.
        """
        for timer in self.timers.values():
            timer.cancel()
        AsynchronousExecutePlugin.stop(self)
    
    def handle_input(self, input):
        """
        Process a work item in this thread.
        """
        input()

    def sniff(self, input):
        "Check whether an input looks vaguely sane for use as a deferred work item."
        assert callable(input)
        
    def do_deferred(self, item):
        """
        do_deferred(item)

        Queue the work item (which should be callable with no
        arguments) for immediate execution in the worker thread.
        """
        self.receive_input(item)
        
    def do_deferred_after(self, secs, workitem):
        """
        do_deferred_after(secs, workitem)

        Arrange for the given work item to run in the worker thread
        after a delay of at least secs seconds.  workitem will be
        invoked as a callable with no arguments.
        """
        assert callable(workitem)

        # XXX this should really have a lock around it!
        timer_id = self.current_timer_id
        self.current_timer_id += 1
        
        def _handle_deferred_after():
            del self.timers[timer_id]
            self.do_deferred(workitem)
        t = Timer(secs, _handle_deferred_after)
        assert not self.timers.has_key(timer_id)
        self.timers[timer_id] = t
        t.start()
        
