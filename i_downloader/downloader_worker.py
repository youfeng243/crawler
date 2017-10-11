#!/usr/bin/env python
# coding:utf-8
import traceback

from common.log import log
from common.worker_pool import  BasicWorker, \
                                WorkerControllerWithInputThread,\
                                BasicWorkerPool, ResponsePacket
from download_proccessor import DownloaderProccessor
from i_util.input_thread_with_pool import InputThread

class DownloaderWorkerController(WorkerControllerWithInputThread):
    def __init__(self, proc=None, conf=None):
        super(DownloaderWorkerController, self).__init__(proc, conf)
        self.conf = conf

    def stop_handler(self, packet, from_main_queue, to_main_queue):
        WorkerControllerWithInputThread.stop_handler(self, packet, from_main_queue, to_main_queue)
        self.processor.stop()

    def start_handler(self, packet, from_main_queue, to_main_queue):
        try:
            self.on_start()
            self.input_thread = InputThread(conf=self.conf, proc_name=self.proc.process.name,
                                               processor=self.processor)
            self.input_thread.start()
            self.proc.running = True
            to_main_queue.put(ResponsePacket(packet, True))
        except Exception as e:
            log.error(str(traceback.format_exc()))
            to_main_queue.put(ResponsePacket(packet, False))
            raise e
        log.debug("%s : invoking start handler for process " % str(self.proc.process.name))

    def on_start(self):
        self.processor = DownloaderProccessor(self.conf)

class DownloaderWorker(BasicWorker):
    def __init__(self, conf=None, name=None):
        BasicWorker.__init__(self, conf, name, controller_class=DownloaderWorkerController)

class DownloaderWorkerPool(BasicWorkerPool):
    def __init__(self, init_count=BasicWorkerPool.DEFAULT_INIT_COUNT, conf=None):
        super(DownloaderWorkerPool, self).__init__(
                                            init_count=init_count,
                                            conf=conf, tag=conf['server']['name'],
                                            worker_class=DownloaderWorker)
