# coding=utf-8
import sys
import threading
import time
import traceback

from beanstalkc import SocketError

from common.log import log
from i_util.pybeanstalk import PyBeanstalk
from multiprocessing import current_process
# from i_extractor.extractor_proccessor import ExtractorProccessor

sys.path.append('..')

class InputThreadNew(threading.Thread):
    def __init__(self, conf, processor=None, proc_name=None):
        threading.Thread.__init__(self)
        self.running = True
        self.proc_name = proc_name # Only for logging
        self.input_tube = conf['beanstalk_conf']['input_tube']
        self.beanstalk = PyBeanstalk(conf['beanstalk_conf']['host'], conf['beanstalk_conf']['port'])
        self.out_beanstalk = PyBeanstalk(conf['beanstalk_conf']['host'], conf['beanstalk_conf']['port'])
        self.output_tube = conf['beanstalk_conf']['output_tube']
        self.topic_output_tubes = {}
        self.topic_output_tubes.setdefault('default', [])
        """
            output_tube = ["default_out", "only_special_out:1,2,3:exclusive", "special_out:4", ":5:exclusive"]
            topic_id:1,2,3只会用到only_special_out
            topic_id:4 会进入special_out和default_out
            topic_id:5 不会进入队列
            topic_id:else 用用default_out队列
        """
        if type(self.output_tube) == list:
            for tube_def in self.output_tube:
                tube_def = tube_def.strip()
                if len(tube_def.split(":")) < 2:
                    self.topic_output_tubes['default'].append(tube_def)
                else:
                    elements = [a.strip() for a in tube_def.split(':')]
                    tube_name = elements[0]
                    topic_ids = [int(a.strip()) for a in elements[1].split(',')]
                    exclusive = False
                    if len(elements) == 3 and elements[2] == 'exclusive':
                        exclusive = True
                    for topic_id in topic_ids:
                        self.topic_output_tubes.setdefault(topic_id, [])
                        self.topic_output_tubes[topic_id].append((tube_name, exclusive))
        else:
            self.topic_output_tubes['default'].append(self.output_tube)

        self.log = log
        if processor is None:
            log.error("Processor not given !")
            raise Exception("Processor not given !")
        else:
            self.processor = processor

    def stop(self):
        self.log.warning("stop input thread")
        self.running = False

    def run(self):
        log.debug("starting input thread")
        job_num = 0
        while self.running and self.input_tube:
            try:
                job = self.beanstalk.reserve(self.input_tube, 3)
                if job:
                    job_num += 1
                    body = job.body
                    resp = None
                    job.delete()
                    if self.processor is not None:
                        topic_id = None
                        try:
                            if type(self.processor).__name__ in ('ExtractorProccessor', 'SingleSrcMergerProccessor'):
                                resp, topic_id = self.processor.do_task(body)
                            else:
                                resp = self.processor.do_task(body)
                        except Exception, e:
                            log.error("Process failed. " + traceback.format_exc())
                        if resp is not None:
                            self.output_msg(resp, topic_id)
                else:
                    self.log.debug(current_process().name + " : no msg from : %s" % (self.input_tube))
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

    def output_msg(self, processor_resp, topic_id=None):
        if processor_resp and isinstance(processor_resp, basestring):
            self.do_output_msg(processor_resp, topic_id)
        elif isinstance(processor_resp, list):
            for message in processor_resp:
                self.do_output_msg(message, topic_id)

    def do_output_msg(self, processor_resp, topic_id=None):
        assert isinstance(processor_resp, basestring)
        try:
            no_default_tubes = False

            if topic_id is not None and topic_id in self.topic_output_tubes:
                defined_output_tubes = self.topic_output_tubes[topic_id]
                for defined_output_tube in defined_output_tubes:
                    tube_name = defined_output_tube[0]
                    no_default_tubes |= defined_output_tube[1]
                    if tube_name:
                        self.out_beanstalk.put(tube_name, str(processor_resp))

            if not no_default_tubes:
                default_tubes = self.topic_output_tubes['default']
                for tube_name in default_tubes:
                    result = None
                    while not result:
                        result = self.out_beanstalk.put(tube_name, str(processor_resp))

            # if isinstance(self.output_tube, list):
            #     for output_tube in self.output_tube:
            #         self.out_beanstalk.put(output_tube, str(processor_resp))
            # else:
            #     self.out_beanstalk.put(self.output_tube, str(processor_resp))
        except Exception, e:
            self.log.error("put msg from:%s\tresult:%s" % (self.output_tube, str(e)))