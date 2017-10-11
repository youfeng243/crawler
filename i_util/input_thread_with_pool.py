# coding=utf-8
import sys
sys.path.append('..')
import threading
import time
import traceback

from beanstalkc import SocketError

from i_util.logs import LogHandler
from i_util.pybeanstalk import PyBeanstalk

from i_util.thread_pool import ThreadPool

class InputThread(threading.Thread):
    def __init__(self, conf, processor, proc_name= None):
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = True
        self.beanstalk = PyBeanstalk(conf['beanstalk_conf']['host'], conf['beanstalk_conf']['port'])
        self.out_beanstalk = PyBeanstalk(conf['beanstalk_conf']['host'], conf['beanstalk_conf']['port'])
        self.input_tube = conf['beanstalk_conf']['input_tube']
        self.output_tube = conf['beanstalk_conf']['output_tube']

        self.log = conf['log']
        if not self.log:
            self.log = LogHandler("i_input_thread")
        self.processor = processor
        if self.processor is None:
            self.log.error("Processor not given !")
            raise Exception("Processor not given !")

        self.processor_pool = ThreadPool(conf['server'].get("process_thread_num", 1),\
                                         {},\
                                         int(conf['server'].get("process_thread_num", 1))
                                         )
        self.wlock = threading.Lock()

    def stop(self):
        self.log.warning("stop input_thread")
        self.running = False
        self.processor_pool.join_all()

    def run(self):
        job_num = 0
        self.running = True
        while self.running and self.input_tube:
            try:
                job = self.beanstalk.reserve(self.input_tube, 3)
                if not job is None:
                    job_num += 1
                    body = job.body
                    job.delete()
                    self.processor_pool.queue_task(self._on_task_start, (body,), self._on_task_finished)

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
        try:
            result = self.processor.do_task(task)
        except Exception as e:
            self.log.error(e.message)
        return result

    def _on_task_finished(self, (task), **thread_locals):
        self.wlock.acquire()
        if task and isinstance(task, basestring):
            self._output_msg(task)
        elif isinstance(task, list):
            for message in task:
                self._output_msg(message)
        self.wlock.release()

    def _output_msg(self, task):
        if task and isinstance(task, basestring):
            try:
                if isinstance(self.output_tube, list):
                    for output_tube in self.output_tube:
                        self.out_beanstalk.put(output_tube, str(task))
                else:
                    self.out_beanstalk.put(self.output_tube, str(task))
            except Exception, e:
                self.log.error("put msg from:%s\tresult:%s" % (self.output_tube, str(e)))
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
