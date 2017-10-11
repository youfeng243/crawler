# -*- coding:utf-8 -*-
import threading
import traceback
from Queue import Queue

from beanstalkc import SocketError
from thrift.protocol.TBinaryProtocol import TBinaryProtocol as TBinaryServerProtocol
from thrift.transport.TTransport import TMemoryBuffer

from i_util.pybeanstalk import PyBeanstalk
from i_util.tools import multi_thread_singleton


@multi_thread_singleton
class PutBeanstaldServer(threading.Thread):
    def __init__(self, beanstalk_conf, log):
        self._queue = Queue()
        self._log = log
        self.beanstalk = PyBeanstalk(beanstalk_conf['host'], beanstalk_conf['port'])
        self.beanstalk_conf = beanstalk_conf
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = True

    def to_string(self, page_info):
        str_page_info = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryServerProtocol(tMemory_b)
            page_info.write(tBinaryProtocol_b)
            str_page_info = tMemory_b.getvalue()
        except EOFError as e:
            self._log.warning("cann't write data to string")
        return str_page_info

    def put_beanstalkd(self, tube_name, obj):
        str_page_info = self.to_string(obj)
        try:
            self.beanstalk.put(tube_name, str_page_info)
            self._log.info('put beanstalk \ttube:%s success' % (tube_name,))
        except SocketError as e:
            self._log.error('beanstalk connect failed, {}'.format(e.message))
            self.beanstalk = PyBeanstalk(self.beanstalk_conf['host'], self.beanstalk_conf['port'])
        except Exception as e:
            self._log.info(
                'beanstalk put tube{} error {}'.format(tube_name,str(traceback.format_exc())))

    def run(self):
        while True:
            record = self._queue.get()
            self._build_record_and_put(record)
    def get_tube_by_name(self, tube_name):
        return self.beanstalk_conf.get(tube_name, None)

    def _build_record_and_put(self, data):
        tube_name = data.get('tube_name', None)
        if not tube_name: return
        obj = data.get('obj', None)
        if not obj:return
        self.put_beanstalkd(tube_name,obj)

    def save_record(self, data):
        self._queue.put(data)

if __name__ == "__main__":
    pass