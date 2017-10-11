# coding=utf-8
import threading
import time
import traceback
import sys
from i_util.pybeanstalk import PyBeanstalk
from beanstalkc import BeanstalkcException
from beanstalkc import UnexpectedResponse
from beanstalkc import CommandFailed
from beanstalkc import DeadlineSoon
from beanstalkc import SocketError
from beanstalkc import Connection
from i_util.logs import LogHandler
from i_util.ProfilingUtil import ZProfiler
sys.path.append('..')


class InputThread(threading.Thread):
    def __init__(self, beanstalk_conf, log=None, process_pool=None):
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = True
        self.beanstalk = PyBeanstalk(beanstalk_conf['host'], beanstalk_conf['port'])
        self.out_beanstalk = PyBeanstalk(beanstalk_conf['host'], beanstalk_conf['port'])
        self.input_tube = beanstalk_conf['input_tube']
        self.output_tube = beanstalk_conf['output_tube']
        self.log = log
        if not self.log:
            self.log = LogHandler("i_input_thread")

        self.process_pool = process_pool
        self.wlock = threading.Lock()

    def stop(self):
        self.log.warning("stop input_thread")
        self.running = False
        proccesor = None
        try:
            while True:
                if self.process_pool.get_task_num() == 0:
                    if self.process_pool.thread_local_constructors.has_key('processor'):
                        processor = self.process_pool.thread_local_constructors['processor'][1][1]
                        self.log.warning("prepare call scheduler_processor to stop scheduler")
                        processor.save_status()
                        break
                else:
                    self.log.info("wait tasks be consumed over, wait 5s")
                    time.sleep(5)

            self.beanstalk.__del__()  # 关闭连接不再接受数据
        except Exception, e:
            self.log.error("stop input_thread fail:%s" % e.message)

    def run(self):
        job_num = 0
        while self.running and self.input_tube:
            try:
                job = self.beanstalk.reserve(self.input_tube, 30)
                if not job is None:
                    job_num += 1
                    body = job.body
                    job.delete()
                    if self.process_pool:
                        self.process_pool.queue_task(self._on_task_start, (body,), self._on_task_finished)
                        if self.process_pool.get_task_num() >= 50:
                            self.log.info("place_processor\ttasks:%d" % (self.process_pool.get_task_num()))
                            time.sleep(2)
                else:
                    self.log.info("not msg from:%s" % self.input_tube)
            except SocketError as e:
                time.sleep(30)
                self.log.error('beanstalk\tconnect\tfail\tstart\treconnect')
                try:
                    self.beanstalk.reconnect()
                    self.out_beanstalk.reconnect()
                    self.log.error('beanstalk\treconnect\tsuccess')
                except Exception as e:
                    self.log.error('beanstalk\treconnect\tfail')
            except:
                self.log.error("not msg from:%s\tresult:%s" % (self.input_tube, str(traceback.format_exc())))

    def _on_task_start(self, task, **thread_locals):
        result = None
        if thread_locals.has_key('profiler'):
            thread_locals['profiler'].begin()
        if thread_locals.has_key('processor'):
            result = thread_locals['processor'].do_task(task)
        return result

    def _on_task_finished(self, (task), **thread_locals):
        self.wlock.acquire()
        proccesor = None
        if thread_locals.has_key('processor'):
            proccesor = thread_locals['processor']
        if thread_locals.has_key('profiler'):
            thread_locals['profiler'].end()
        if task and isinstance(task, basestring):
            self._output_msg(task, proccesor)
        elif isinstance(task, list):
            for message in task:
                self._output_msg(message, proccesor)
        self.wlock.release()

    def _output_msg(self, task, proccesor):
        if task and isinstance(task, basestring):
            try:
                if isinstance(self.output_tube, list):
                    for output_tube in self.output_tube:
                        self.out_beanstalk.put(output_tube, str(task))
                else:
                    self.out_beanstalk.put(self.output_tube, str(task))
            except Exception, e:
                self.log.error("put msg from:%s\tresult:%s" % (self.output_tube, str(e)))
        elif task and proccesor:
            self.log.info("put_msg_to:%s\tresult:%s" % (self.output_tube, type(task)))
            proccesor.do_output(task)

is_stop = False


def signal_handler(signal=None, frame=None):
    global is_stop
    is_stop = True


def main(beanstalk_conf, log=None, process_pool=None):
    import signal
    global is_stop
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    signal.signal(signal.SIGUSR1, signal_handler)
    tr = InputThread(beanstalk_conf)
    tr.start()
    while not is_stop:
        time.sleep(10)
    tr.stop()

if __name__ == '__main__':
    time.sleep(2)
    beanstalk_conf = {}
    # beanstalk_conf['host'] = "101.201.102.37";
    beanstalk_conf['host'] = "127.0.0.1"
    beanstalk_conf['port'] = 11300
    beanstalk_conf['input_tube'] = 'scheduler_info'
    beanstalk_conf['output_tube'] = ['test1', 'test2']
    main(beanstalk_conf, log=None, process_pool=None)