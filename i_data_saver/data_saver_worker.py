#!/usr/bin/env python
# coding:utf-8
from common.log import log
from common.worker_pool import ControlPacket,\
                                ResponsePacket,\
                                BasicWorker, \
                                WorkerControllerWithInputThread,\
                                BasicWorkerPool
from i_data_saver.data_saver_proccessor import DataSaverProcessor


class ReloadPacket(ControlPacket):
    def __init__(self, topic_id=-1):
        super(ReloadPacket, self).__init__(ControlPacket.RELOAD)
        self.topic_id=topic_id

class ReloadRespPacket(ResponsePacket):
    def __init__(self, ipack, succ=True, msg=None, data=None):
        super(ReloadRespPacket, self).__init__(ipack, succ, msg)
        self.data = data

    def get_data(self):
        return self.data


class DatasaverWorkerController(WorkerControllerWithInputThread):
    def __init__(self, proc=None, conf=None):
        self.conf = conf
        self.processor = None
        super(DatasaverWorkerController, self).__init__(proc, conf)
        self.processor_map[ReloadPacket.__name__] = self.reload_handler

    def reload_handler(self, packet, from_main_queue, to_main_queue):
        log.debug("Invoking reload handler for process " + str(self.proc.process.name))
        assert type(packet) == ReloadPacket
        ret_data = self.processor.datasaver.reload(packet.topic_id)
        to_main_queue.put(ReloadRespPacket(packet, True, data=ret_data))

    def stop_handler(self, packet, from_main_queue, to_main_queue):
        WorkerControllerWithInputThread.stop_handler(self, packet, from_main_queue, to_main_queue)
        self.processor.stop()

    def on_start(self):
        self.processor = DataSaverProcessor(self.conf)

class DatasaverWorker(BasicWorker):
    def __init__(self, conf=None, name=None):
        BasicWorker.__init__(self, conf, name, controller_class=DatasaverWorkerController)

    def reload(self, topic_id=-1):
        self.from_main_queue.put(ReloadPacket(topic_id=topic_id), block=True)
        resp = self.to_main_queue.get(block=True)
        assert type(resp) == ReloadRespPacket
        if resp.is_succ():
            log.debug(str(self.process.name) + " : reload success")
        else:
            log.debug(str(self.process.name) + " : reload failed, " + str(resp.msg))
        return resp.is_succ(), resp.get_data()

class DatasaverWorkerPool(BasicWorkerPool):
    def __init__(self, init_count=BasicWorkerPool.DEFAULT_INIT_COUNT, conf=None):
        super(DatasaverWorkerPool, self).__init__(
                                            init_count=init_count,
                                            conf=conf, tag=conf['server']['name'],
                                            worker_class=DatasaverWorker)

    def reload_all(self, topic_id=-1):
        last_succ_ret = None
        for worker in self.subs:
            assert type(worker) == DatasaverWorker
            reload_succ, data = worker.reload(topic_id)
            if not reload_succ:
                log.warning(worker.process.name + " : reload failed")
            else:
                last_succ_ret = data
        return last_succ_ret
