# coding=utf-8
import json
import sys
import traceback

from common.log import log
from i_data_saver.data_saver import DataSaver

sys.path.append('..')
from i_util.normal_proccessor import NormalProccessor

# beanstalk的入口
class DataSaverProcessor(NormalProccessor):
    def __init__(self, conf):
        self.log = log
        self.datasaver = DataSaver(conf)

    def do_task_json(self, body):
        try:
            j = json.loads(body)
            self.datasaver.check_data(j, save=True)
        except EOFError, e:
            self.log.warning("cann't process error")
            self.log.error(traceback.format_exc())
            return None
        return None
    def do_task(self, body):
        return self.do_task_json(body)

    def do_output(self,body):
        return True

    def stop(self):
        self.datasaver.data_stastics.stop()