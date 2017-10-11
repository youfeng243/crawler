# coding=utf8
import time
import traceback
import json
from i_entity_extractor.etl.base_etl import BaseEtl
from i_entity_extractor.extractors.fygg.fygg_extractor import FyggExtractor
from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
from multiprocessing import Pool


class Fygg(BaseEtl):
    def __init__(self, conf):
        BaseEtl.__init__(self,conf)
        route         = EntityExtractorRoute(conf)
        self.topic_id = 33
        topic_info    = route.all_topics.get(self.topic_id, None)
        self.fygg_obj = FyggExtractor(topic_info, conf.get('log'))

    def process_data(self, item):
        if not item.has_key("bulletin_content"):
            item["bulletin_content"] = item.get("bulletin_type","") + "\t" + item.get("norm_bulletin_content","")

        etl_data = self.fygg_obj.format_extract_data(item, self.topic_id)
        etl_data = self.fygg_obj.after_process(etl_data)
        #etl_data.pop("_id")
        #print json.dumps(etl_data,ensure_ascii=False,encoding='utf8')

        return etl_data

    def etl(self):
        '''清洗总入口'''
        begin_time   = time.time()
        # 1 获取源数据
        src_table  = self.conf.get('topic2table').get(str(self.topic_id), '')
        last_table = src_table + "_last"

        ret = self.db_obj.cur_table_bak(src_table,last_table)
        if not ret:
            return False

        bak_table = self.db_obj.get_last_table(last_table)
        cursor    = self.db_obj.db.select(bak_table, {})

        # 2 清洗数据
        insert_data_list = []
        num = 0
        try:
            for item in cursor:
                num += 1
                if item.has_key("bulletin_content"):
                    etl_data = self.process_data(item)
                else:
                    etl_data = item
                insert_data_list.append(etl_data)
                if len(insert_data_list) >= self.max_insert_data:
                    self.db_obj.insert_info_batch(src_table, insert_data_list)
                    del insert_data_list[:]

                if num % 500 == 0:
                    self.log.info("process_num:%s\ttime_cost:%s" % (num, time.time() - begin_time))
        except Exception as e:
            self.log.error(traceback.format_exc())
            return False

        self.db_obj.insert_info_batch(src_table, insert_data_list)
        del insert_data_list[:]

        self.log.info("finish_fygg_etl,process_num:%s\ttime_cost:%s" % (num, time.time() - begin_time))

        return True

    def multi_etl(self):
        '''清洗总入口'''
        begin_time   = time.time()
        # 1 获取源数据
        src_table  = self.conf.get('topic2table').get(str(self.topic_id), '')
        last_table = src_table + "_last"

        ret = self.db_obj.cur_table_bak(src_table,last_table)
        if not ret:
            return False

        bak_table = self.db_obj.get_last_table(last_table)
        cursor    = self.db_obj.db.select(bak_table, {})

        num = 0
        data_list = []
        insert_data_list = []
        pool = Pool(4)

        # 2 清洗数据
        insert_data_list = []
        num = 0
        try:
            for item in cursor:
                num += 1
                if item.has_key("bulletin_content"):
                    etl_data = self.process_data(item)
                else:
                    etl_data = item
                    data_list.append(etl_data)
                if len(data_list) >= self.max_process_data:
                    ret = pool.map(self.process_data, data_list)
                    del data_list[:]
                    for i in range(self.q_data.qsize()):
                        insert_data_list.append(self.q_data.get())
                    self.db_obj.insert_info_batch(src_table, insert_data_list)
                    del insert_data_list[:]

                if num % 500 == 0:
                    self.log.info("process_num:%s\ttime_cost:%s" % (num, time.time() - begin_time))
        except Exception as e:
            self.log.error(traceback.format_exc())
            return False

        ret = pool.map(self.process_data, data_list)
        del data_list[:]

        for i in range(self.q_data.qsize()):
            insert_data_list.append(self.q_data.get())
        self.db_obj.insert_info_batch(src_table, insert_data_list)
        del insert_data_list[:]

        self.log.info("finish_fygg_etl,process_num:%s\ttime_cost:%s" % (num, time.time() - begin_time))

        return True



