#!/usr/bin/env python
# coding:utf-8
from common.log import log
from common.worker_pool import ControlPacket,\
                                ResponsePacket,\
                                BasicWorker, \
                                WorkerControllerWithInputThread,\
                                BasicWorkerPool
from extractor_proccessor import ExtractorProccessor


class ReloadPacket(ControlPacket):
    def __init__(self, parser_id=-1):
        super(ReloadPacket, self).__init__(ControlPacket.RELOAD)
        self.parser_id=parser_id

class ReloadRespPacket(ResponsePacket):
    def __init__(self, ipack, succ=True, msg=None, data=None):
        super(ReloadRespPacket, self).__init__(ipack, succ, msg)
        self.data = data

    def get_data(self):
        return self.data

class ExtractorWorkerController(WorkerControllerWithInputThread):
    def __init__(self, proc=None, conf=None):
        self.conf = conf
        self.processor = None
        super(ExtractorWorkerController, self).__init__(proc, conf)
        self.processor_map[ReloadPacket.__name__] = self.reload_handler

    def reload_handler(self, packet, from_main_queue, to_main_queue):
        log.debug("Invoking reload handler for process " + str(self.proc.process.name))
        assert type(packet) == ReloadPacket
        ret_data = self.processor.extractor.reload_parser_config(packet.parser_id)
        to_main_queue.put(ReloadRespPacket(packet, True, data=ret_data))

    def stop_handler(self, packet, from_main_queue, to_main_queue):
        WorkerControllerWithInputThread.stop_handler(self, packet, from_main_queue, to_main_queue)
        self.processor.stop()

    def on_start(self):
        self.processor = ExtractorProccessor(self.conf)

class ExtractorWorker(BasicWorker):
    def __init__(self, conf=None, name=None):
        BasicWorker.__init__(self, conf, name, controller_class=ExtractorWorkerController)

    def reload(self, parser_id=-1):
        self.from_main_queue.put(ReloadPacket(parser_id=parser_id), block=True)
        resp = self.to_main_queue.get(block=True)
        assert type(resp) == ReloadRespPacket
        if resp.is_succ():
            log.debug(str(self.process.name) + " : reload success")
        else:
            log.debug(str(self.process.name) + " : reload failed, " + str(resp.msg))
        return resp.is_succ(), resp.get_data()

class ExtractorWorkerPool(BasicWorkerPool):
    def __init__(self, init_count=BasicWorkerPool.DEFAULT_INIT_COUNT, conf=None):
        super(ExtractorWorkerPool, self).__init__(
                                            init_count=init_count,
                                            conf=conf, tag=conf['server']['name'],
                                            worker_class=ExtractorWorker)

    def reload_all(self, parser_id=-1):
        last_succ_ret = None
        for worker in self.subs:
            assert type(worker) == ExtractorWorker
            reload_succ, data = worker.reload(parser_id)
            if not reload_succ:
                log.warning(worker.process.name + " : reload failed")
            else:
                last_succ_ret = data
        return last_succ_ret
