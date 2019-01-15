from threading import Thread, Lock, Event
import time
import random
from concurrent.futures import ThreadPoolExecutor


NOF_TICKETS_PER_SECOND = 8
NOF_THREADS = 4

tickets = 0
ticketing_lock = Lock()
ticketing_event = Event()

def timer_do_work():
    time_zero = time.perf_counter()
    while True:
        global tickets, ticketing_lock
        try:
            ticketing_lock.acquire()
            tickets = NOF_TICKETS_PER_SECOND

            ticketing_event.set()
        finally:
            ticketing_lock.release()
            
        
        elapsed_time = time.perf_counter() - time_zero

        print("-------------current time: {:6.3f}---------------".format(elapsed_time))

        time.sleep(1)

def worker_do_work(work_item):
    global tickets, ticketing_lock

    while True:
        
        # wait_flag = False # no need own flag, because event has internal flag
        try:
            ticketing_lock.acquire()
            if tickets <= 0:
                # use a local flag to move the event.wait outside locking block. (don't block other threads from using this lock and do checking)
                ticketing_event.clear()
                # wait_flag = True # no need own flag, because event has internal flag

            else:
                # has available tickets, can proceed
                tickets -= 1
                break
        finally:
            ticketing_lock.release()
            
        #if wait_flag: # no need own flag, because event has internal flag
        ticketing_event.wait()
            
    # do actual work
    print("work item {} has started".format(work_item))
    time.sleep(random.randint(0, 10))
    # commet it out if it makes you harder to count started work item between timer printouts
    # print("done with work item {}".format(work_item))

if __name__ == "__main__":
    timer_thread = Thread(target=timer_do_work, args=(), daemon=True)
    # timer_thread = Thread(target=timer_do_work, args=())

    timer_thread.start()

    with ThreadPoolExecutor(max_workers=NOF_THREADS) as executor:
        executor.map(worker_do_work, range(128))
        
    
