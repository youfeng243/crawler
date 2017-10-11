#coding=utf8

import time
import pytoml
import logging
from i_entity_extractor.etl.wenshu.clean_wenshu import Wenshu
from i_entity_extractor.etl.fygg.clean_fygg import Fygg
from i_entity_extractor.etl.ktgg.clean_ktgg import Ktgg
from i_entity_extractor.etl.judge_process.clean_judge_process import JudgeProcess
from common.log import log

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                 datefmt='%a, %d %b %Y %H:%M:%S',
                 filename='./etl.log',
                 filemode='a')

class StartEtl:
    def __init__(self,filename):
        self.conf = self.load_conf(filename)
        self.log  = self.conf.get("log")
        self.etl_wenshu_obj        = Wenshu(self.conf)
        self.etl_fygg_obj          = Fygg(self.conf)
        self.etl_Ktgg_obj          = Ktgg(self.conf)
        self.etl_judge_process_obj = JudgeProcess(self.conf)

        self.etl_route = {
            "32": self.etl_wenshu_obj,
            "33": self.etl_fygg_obj,
            "34": self.etl_Ktgg_obj,
            "37": self.etl_judge_process_obj,
        }

    def load_conf(self,filename):
        with open(filename, 'rb') as config:
            conf = pytoml.load(config)

        log.init_log(conf, console_out=conf['logger']['console'])
        conf['log'] = log

        conf['log_etl'] = logging
        return conf

    def do_etl(self,topic_id):
        topic_id = str(topic_id)
        obj      = self.etl_route.get(topic_id)
        obj.etl()

    def do_reset(self,topic_id):
        '''清洗回滚'''
        topic_id = str(topic_id)
        obj      = self.etl_route.get(topic_id)
        if obj:

            collection_names = obj.db_obj.db.db.collection_names()
            cur_table        = self.conf.get("topic2table",{}).get(topic_id,"")
            last_table       = cur_table + "_last"
            last_table       = obj.db_obj.get_last_table(last_table)

            if last_table not in collection_names or cur_table not in collection_names:
                self.log.error("last_table or cur_table not in collection_names,last_table:%s\tcur_table:%s"%(last_table,cur_table))
                return False

            ret = obj.db_obj.reset_table(last_table,cur_table)
            if not ret:
                self.log.error("reset_table_error,last_table:%s\tcur_table:%s"%(last_table,cur_table))
                return False

            self.log.info("reset_table_success,last_table:%s\tcur_table:%s" % (last_table, cur_table))
            return True


        return False





if __name__ == "__main__":
    begin_time = time.time()
    print "start"
    obj = StartEtl("etl.toml")
    obj.do_etl(33)
    #obj.do_reset(37)

    print "finish_etl,time_cost:%s"%(time.time() - begin_time)