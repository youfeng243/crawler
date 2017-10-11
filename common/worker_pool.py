# coding:utf-8
import signal
import sys
import time
import traceback
from multiprocessing import Process, Queue
from threading import Thread, Lock

from common.log import log
from i_util.input_thread_new import InputThreadNew


import os



def set_proc_name(newname):
    from ctypes import cdll, byref, create_string_buffer
    import platform
    sys_name = platform.system().lower()
    if sys_name == "linux":
        libc = cdll.LoadLibrary('libc.so.6')
        buff = create_string_buffer(len(newname)+1)
        buff.value = newname
        libc.prctl(15, byref(buff), 0, 0, 0)

class PoolState:

    STOPPED = 0
    STARTING = 1
    RUNNING = 2
    STOPPING = 3
    JOINNING = 4

    _name_map = {
        STOPPED : 'STOPPED',
        STARTING : 'STARTING',
        RUNNING : 'RUNNING',
        STOPPING : 'STOPPING',
        JOINNING : 'JOINNING'
    }

    @staticmethod
    def name(state):
        return PoolState._name_map[state]

class ControlPacket(object):
    START = 1
    STOP = 2
    RELOAD = 3
    def __init__(self, type_):
        self.type_ = type_

class ResponsePacket(object):
    def __init__(self, ipack, succ=True, msg=None, data=None):
        self.type_ = ipack.type_
        self.succ = succ
        self.msg = msg
        self.data = data

    def is_succ(self):
        return True == self.succ

class StopPacket(ControlPacket):
    def __init__(self):
        ControlPacket.__init__(self, ControlPacket.STOP)

class StartPacket(ControlPacket):
    def __init__(self):
        ControlPacket.__init__(self, ControlPacket.START)

class MockInputThread(Thread):
    def __init__(self, conf=None, proc_name=None):
        super(MockInputThread, self).__init__()
        self.conf = conf
        self.running = False
        self.proc_name = proc_name

    def start(self):
        self.running = True
        log.debug("input thread in proc " + self.proc_name + " starting...")
        return super(MockInputThread, self).start()

    def stop(self):
        log.debug("input thread in proc " + self.proc_name + " stopping...")
        self.running = False

    def run(self):
        while self.running:
            log.debug("input thread in proc " + self.proc_name + " running...")
            time.sleep(1)
        log.debug("input thread in proc " + self.proc_name + " gracefully stopped")


class BasicWorkerControler(object):

    def __init__(self, proc=None, conf=None):
        if proc is None:
            raise Exception("Worker proc not given")
        self.proc = proc
        self.conf = conf
        # self.input_thread = MockInputThread(conf, proc_name=proc.process.name)
        self.processor_map = {
            StartPacket.__name__ : self.start_handler,
            StopPacket.__name__ : self.stop_handler
        }

    def get_handler(self, packet):
        return self.processor_map.get(type(packet).__name__, self.default_handler)

    def process_packet(self, packet, from_main_queue, to_main_queue):
        handler = self.get_handler(packet)
        handler(packet, from_main_queue, to_main_queue)

    def default_handler(self, packet, from_main_queue, to_main_queue):
        log.debug("Invoking default handler for process " + str(self.proc.process.name))
        to_main_queue.put(ResponsePacket(packet, True))

    def start_handler(self, packet, from_main_queue, to_main_queue):
        log.debug("Invoking start handler for process " + str(self.proc.process.name))
        # self.input_thread.start()
        self.proc.running = True
        to_main_queue.put(ResponsePacket(packet, True))

    def stop_handler(self, packet, from_main_queue, to_main_queue):
        log.debug("Invoking stop handler for process " + str(self.proc.process.name))
        # self.input_thread.stop()
        # self.input_thread.join()
        # print "input thread in proc " + str(self.proc.process.name) + " joined"
        self.proc.running = False
        to_main_queue.put(ResponsePacket(packet, True))

# class ReloadPacket(ControlPacket):
#     def __init__(self):
#         super(ReloadPacket, self).__init__(ControlPacket.RELOAD)

class WorkerControllerWithInputThread(BasicWorkerControler):
    def __init__(self, proc=None, conf=None):
        super(WorkerControllerWithInputThread, self).__init__(proc, conf)
        self.processor = None
        self.input_thread = None
        self.processor_map = {
            StartPacket.__name__: self.start_handler,
            StopPacket.__name__: self.stop_handler
        }

    def start_handler(self, packet, from_main_queue, to_main_queue):
        try:
            self.on_start()
            self.input_thread = InputThreadNew(conf=self.conf, proc_name=self.proc.process.name, processor=self.processor)
            self.input_thread.start()
            self.proc.running = True
            to_main_queue.put(ResponsePacket(packet, True))
        except Exception as e:
            log.error(str(traceback.format_exc()))
            to_main_queue.put(ResponsePacket(packet, False))
            raise e
        log.debug("%s : invoking start handler for process " % str(self.proc.process.name))


    def stop_handler(self, packet, from_main_queue, to_main_queue):
        log.debug("%s : invoking stop handler for process " % str(self.proc.process.name))
        self.proc.running = False
        if self.input_thread:
            self.input_thread.stop()
            self.input_thread.join()
            log.debug("%s : input thread in proc joined" % str(self.proc.process.name))
        to_main_queue.put(ResponsePacket(packet, True))

    def on_start(self):
        pass



# Woker control interface
class BasicWorker(object):

    def __init__(self, conf=None, name=None, controller_class=BasicWorkerControler):
        if conf is None:
            raise Exception("conf is not given")
        self.conf = conf
        self.from_main_queue = Queue()
        self.to_main_queue = Queue()
        self.process = Process(target=self.worker_proc_main, args=(self.from_main_queue, self.to_main_queue,), name=name)
        #self.process.daemon = True
        self.running = False
        self.controller = controller_class(proc=self, conf=conf)

    # Interface part
    def start(self):
        self.process.start()
        self.from_main_queue.put(StartPacket(), block=True)
        resp = self.to_main_queue.get(block=True)
        assert type(resp) == ResponsePacket
        if resp.is_succ():
            log.debug("main proc : %s started successfully" % str(self.process.name))
            self.running = True
            return True
        else:
            log.error("main proc : Failed to start subprocess " + str(self.process.name))
            self.running = False
            #保证子进程启动不成功的is_alive状态为false
            self.process.join()
            return False

    def stop_and_join(self):
        self.from_main_queue.put(StopPacket(), block=True)
        self.join()

    def do_stop(self):
        self.from_main_queue.put(StopPacket(), block=True)

    def do_join(self):
        resp = self.to_main_queue.get(block=True)
        assert type(resp) == ResponsePacket
        if resp.is_succ():
            self.process.join()
            log.debug("main proc : %s joined" % str(self.process.name))
        else:
            log.error("Failed to stop subprocess " + str(self.process.name))


    def join(self):
        resp = self.to_main_queue.get(block=True)
        assert type(resp) == ResponsePacket
        if resp.is_succ():
            self.process.join()
            log.debug("main proc : %s joined" % str(self.process.name))
        else:
            log.error("Failed to stop subprocess " + str(self.process.name))

    # Worker process part
    def kill_isr(self, a, b):
        log.warning("%s : received killed command ,now ignore it!" %(self.process.name,))
        #log.debug("%s : invoking kill isr" % str(self.process.name))
        #self.from_main_queue.put(StopPacket(), block=True)
        # self.controller.process_packet(StopPacket(), self.from_main_queue, self.to_main_queue)
        #self.running = False

    def worker_proc_main(self, from_main_queue, to_main_queue):
        try:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            set_proc_name(self.process.name)
            try:
                log.init_log(self.conf, level=self.conf['logger']['level'], console_out=self.conf['logger']['console'], name=self.process.name)
            except Exception as e:
                packet = from_main_queue.get(block = True)
                to_main_queue.put(ResponsePacket(packet, succ=False), block=True)
                raise e
            # with daemon.DaemonContext:
            log.debug(str(self.process.name) + " : started")
            self.running = True
            while self.running:
                pack = from_main_queue.get(block=True)
                self.controller.process_packet(pack, from_main_queue, to_main_queue)
            log.debug(str(self.process.name) + " : gracefully stopped")
        finally:
            log.debug(str(self.process.name) + " : exited")

class BasicWorkerPool(Thread):

    DEFAULT_INIT_COUNT = 4

    def __init__(self, init_count=DEFAULT_INIT_COUNT, conf=None, tag=None, worker_class=BasicWorker, auto_restart=True):
        super(BasicWorkerPool, self).__init__()
        self.daemon = True
        self.work_class = worker_class
        self.conf = conf
        self.tag = tag
        self.init_count = init_count
        self.subs = [worker_class(conf=conf, name=(
            "%s-worker-%d" % (str(tag), i)) if tag is not None else None) for i in xrange(init_count)]
        self.running = False
        self.auto_restart = auto_restart
        self.state = PoolState.STOPPED
        self.state_lock = Lock()
        self.stop_pending = False


    def state_cas(self, check_states, target_state):
        with self.state_lock:
            if self.state in check_states:
                log.info("Worker pool state transit from %s to %s" % (PoolState.name(self.state), PoolState.name(target_state)))
                self.state = target_state
                return True
            else:
                return False

    def check_state(self, target_states):
        with self.state_lock:
            return self.state in target_states

    def set_state(self, target_state):
        with self.state_lock:
            log.info("Worker pool state transit from %s to %s" % (PoolState.name(self.state), PoolState.name(target_state)))
            self.state = target_state



    def start_all(self):
        if self.state_cas({PoolState.STOPPED}, PoolState.STARTING):
            all_success = True
            for proc in self.subs:
                if not proc.start():
                    all_success = False
                    break
            if not all_success:
                self.stop_all()
                raise Exception("cant not start all subprocess correctly!")
            else:
                self.set_state(PoolState.RUNNING)
                if self.auto_restart:
                    self.start()
                return True

    def do_stop_all(self):
        for proc in self.subs:
            proc.do_stop()

    def do_join_all(self):
        for proc in self.subs:
            # if proc.process.is_alive():
                proc.do_join()
            # else:
            #     log.debug("main proc : %s is not running" % proc.process.name)

    def stop_all(self):
        if self.state_cas({PoolState.RUNNING}, PoolState.STOPPING):
            self.running = False
            self.do_stop_all()
            self.set_state(PoolState.JOINNING)
            self.do_join_all()
            if self.auto_restart and self.is_alive():
                self.join()
            self.set_state(PoolState.STOPPED)
            return True
        else:
            self.stop_pending = True
            return False

    def join_all(self):
        if self.state_cas({PoolState.RUNNING}, PoolState.JOINNING):
            self.running = False
            self.do_join_all()
            if self.auto_restart:
                self.join()
            self.set_state(PoolState.STOPPED)
            return True
        else:
            self.stop_pending = True
            return False

    def get_state(self):
        return self.state

    def run(self):
        self.running = True
        while self.running and self.check_state({PoolState.RUNNING}):
            log.debug("check worker status")
            for idx, sub in enumerate(self.subs):
                # log.debug("Worker %d is alive %s" % (idx, sub.process.is_ssssalive()))
                if not sub.process.is_alive():
                    self.subs[idx] = self.work_class(conf = self.conf,
                                                     name=("%s-worker-%d" %(str(self.tag) \
                                                                                if self.tag is not None else None,idx))
                                                     )
                    log.info("process %d is not active" %(idx))
                    self.subs[idx].start()
                    log.info("restart process %d success" %(idx))
            time.sleep(1)



# holder = BasicWorkerPool(init_count=10, conf={'aaa': 1}, tag="ee", worker_class=None)



def exit_isr(a, b):
    global holder
    log.debug("Begin killing sub processes...")
    holder.stop_all()
    log.debug("All sub processes gracefully stopped")
    sys.exit(1)

if __name__ == "__main__":

    signal.signal(signal.SIGINT, exit_isr)
    signal.signal(signal.SIGTERM, exit_isr)


    holder = BasicWorkerPool(init_count=10, conf={'aaa':1}, tag="ee", worker_class=BasicWorker)
    holder.start_all()
    time.sleep(2)
    for worker in holder.subs:
        worker.reload()
    time.sleep(2)
    holder.stop_all()
    print "All sub processes gracefully stopped"


