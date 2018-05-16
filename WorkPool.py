#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function
import random
import Queue
import threading
import time

"""
    TODO: Documentation
"""

class WorkPool ( threading.Thread ):
    def __init__ ( self, thread_num=20 ):
        threading.Thread.__init__( self )
        self.thread_num = thread_num
        self.quit = False
        self.pending_job_queue = Queue.Queue()
        self.done_job_queue = Queue.Queue()

    def run ( self ):
        self.workspace = [ None for i in range(self.thread_num) ]

        while self.quit == False:

            join_anything = False
            fire_anything = False
            available_slot = None

            # Go through each thread slot
            for idx in range(self.thread_num):
                t = self.workspace[idx]

                # Skip empty slots.
                if None == t:
                    # Cache the first empty slot as available_slot.
                    if None == available_slot:
                        available_slot = idx
                    continue
                
                # For each non-empty slot, poll to see if it can be joined.
                t.join(0.0)
                
                # If a job has been joined, move it to done_job_queue, 
                # empty the slot and set join_anything to be True.
                if not t.isAlive():
                    self.done_job_queue.put(t.callable_obj)
                    self.workspace[idx] = None
                    join_anything = True

            # After a walk through all slots in workspace, 
            # try to fire a pending job if available_slot is not None.
            if None != available_slot:
                try:
                    j = self.pending_job_queue.get(False)
                    t = threading.Thread(None, j, 'Job-' + str(j.myid))
                    t.callable_obj = j
                    self.workspace[available_slot] = t
                    t.start()
                    fire_anything = True
                except Queue.Empty:
                    pass

            # If we can not join anything nor fire anything at this round,
            # sleep for a while then.
            if (not join_anything) and (not fire_anything):
                time.sleep(0.1)

    def join ( self, timeout = None ):
        self.quit = True
        threading.Thread.join( self, timeout )

    # Append a job to work pool.
    # 'callable_obj' can be a function, lambda, or a class object which 
    # has implemented the '__call__' method
    def append_job ( self, callable_obj ):
        self.pending_job_queue.put(callable_obj)

    # Retrieve the result of a job.
    # Might return None if there is no done job yet.
    def retrieve_job ( self ) :
        try:
            return self.done_job_queue.get(False)
        except Queue.Empty:
            return None

# ---------------------------------------------------------------

if __name__ == '__main__':

    class MyJob:
        def __init__ (self, id, countdown):
            self.cntdwn = countdown
            self.myid = id

        def __call__ (self):
            with glck:
                print("[Job] ID: %s Starts." % str(self.myid))
            while self.cntdwn >= 0:
                with glck:
                    print("[Job] ID: %s, CountDown: %d" % (str(self.myid), self.cntdwn))
                self.cntdwn -= 1
                time.sleep(1.0)
            with glck:
                print("[Job] ID: %s Ends." % str(self.myid))

    wp = WorkPool()
    wp.start()

    glck = threading.Lock()

    random.seed()

    for i in range(50):
        j = MyJob(i, random.randint(1,10))
        wp.append_job(j)

    dj = set()
    while len(dj) < 50:
        r = wp.retrieve_job()
        if None == r:
            time.sleep(0.5)
            continue
        dj.add(r.myid)
    
    print("[Main] All job done. Joining work pool...")
    wp.join()
    print("[Main] Work pool has joined.")
