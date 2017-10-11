# coding=utf-8
import sys

from gevent import monkey

monkey.patch_socket()
monkey.patch_time()
import socket
reload(socket)
#monkey.patch_subprocess()
import time
import threading
import traceback
from beanstalkc import SocketError
from common.log import log
from i_util.pybeanstalk import PyBeanstalk

from gevent.pool import Pool as GeventPool

sys.path.append('..')

class GeventInputThread(threading.Thread):
    def __init__(self, conf, processor=None, proc_name=None):
        threading.Thread.__init__(self)
        self.running = True

        self.proc_name = proc_name # Only for logging
        self.letpool = GeventPool(max(1,conf['server']['process_thread_num']))
        self.beanstalk = PyBeanstalk(conf['beanstalk_conf']['host'], conf['beanstalk_conf']['port'])
        self.out_beanstalk = PyBeanstalk(conf['beanstalk_conf']['host'], conf['beanstalk_conf']['port'])
        self.input_tube = conf['beanstalk_conf']['input_tube']
        self.output_tube = conf['beanstalk_conf']['output_tube']
        self.log = log
        if not self.input_tube:
            log.err("Input tube not given !")
            raise Exception("Input tube not given !")
        if processor is None:
            log.error("Processor not given !")
            raise Exception("Processor not given !")
        else:
            self.processor = processor

    def spawn_task(self, job):
        job.delete()
        try:
            msg = self.processor.do_task(job.body)
            if msg is not None:
                self.output_msg(msg)
        except Exception as e:
            self.log.error(e.message)

    def stop(self):
        self.log.warning("stop input thread")
        self.running = False
        self.letpool.join()

    def run(self):
        log.info("starting input thread")

        while self.running:
            try:
                if self.letpool.wait_available():
                    if not self.running:
                        break
                    job = self.beanstalk.reserve(self.input_tube, 10)
                    self.letpool.spawn(self.spawn_task, job)
                    if not job is None:
                        self.letpool.spawn(self.spawn_task, job)
                    else:
                        self.log.info("no msg from:%s" % (self.input_tube))
            except SocketError as e:
                time.sleep(30)
                self.log.error('beanstalk\tconnect\tfail\tstart\treconnect')
                try:
                    self.beanstalk.reconnect()
                    self.out_beanstalk.reconnect()
                    self.log.error('beanstalk\treconnect\tsuccess')
                except Exception as e:
                    self.log.error('beanstalk\treconnect\tfail')
            except Exception, e:
                self.log.error("Uncaught exception: %s traceback: %s" % (e.message, str(traceback.format_exc())))
        log.debug("input thread gracefully stopped")

    def output_msg(self, processor_resp):
        if processor_resp and isinstance(processor_resp, basestring):
            self.do_output_msg(processor_resp)
        elif isinstance(processor_resp, list):
            for message in processor_resp:
                self.do_output_msg(message)

    def do_output_msg(self, processor_resp):
        assert isinstance(processor_resp, basestring)
        try:
            if isinstance(self.output_tube, list):
                for output_tube in self.output_tube:
                    self.out_beanstalk.put(output_tube, str(processor_resp))
            else:
                self.out_beanstalk.put(self.output_tube, str(processor_resp))
        except Exception, e:
            self.log.error("put msg from:%s\tresult:%s" % (self.output_tube, str(e)))

if __name__ == "__main__":
    conf = {u'server':
                {u'server_thread_num': 150,
                 u'process_thread_num': 100,
                 u'host': u'182.61.13.185',
                 u'server_process_num': 1,
                 u'port': 12200,
                 u'name': u'download'},
            u'beanstalk_conf':
                {u'host': u'182.61.13.185',
                 u'input_tube': u'download_req',
                 u'port': 11300,
                 u'output_tube': u'download_rsp'},
            }
    log.init_log(conf, level="debug", console_out=True)
    conf['log'] = log
    def spawn(job):
        print job
    gi = GeventInputThread(conf, processor=spawn, proc_name="hello")
    gi.start()
    pass