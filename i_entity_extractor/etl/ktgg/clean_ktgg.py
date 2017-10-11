# coding=utf8
import time
import traceback
import json
from i_entity_extractor.etl.base_etl import BaseEtl
from i_entity_extractor.extractors.ktgg.ktgg_extractor import KtggExtractor
from i_entity_extractor.entity_extractor_route import EntityExtractorRoute


class Ktgg(BaseEtl):
    def __init__(self, conf):
        BaseEtl.__init__(self, conf)
        route         = EntityExtractorRoute(conf)
        self.topic_id = 34
        topic_info    = route.all_topics.get(self.topic_id, None)
        self.ktgg_obj = KtggExtractor(topic_info, conf.get('log'))

    def process_data(self, item):
        etl_data = self.ktgg_obj.format_extract_data(item, self.topic_id)
        if isinstance(etl_data,list) and len(etl_data) > 0:
            etl_data = self.ktgg_obj.after_process(etl_data[0])
            # etl_data.pop("_id")
            # print json.dumps(etl_data, ensure_ascii=False, encoding='utf8')
        else:
            etl_data = {}

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

                etl_data = self.process_data(item)
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

        self.log.info("finish_ktgg_etl,process_num:%s\ttime_cost:%s" % (num, time.time() - begin_time))

        return True


