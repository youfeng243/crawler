#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  gikieng.li
# @Date    2017-03-27 18:10

from i_util.task_statistics_base import TaskStatisticsBase
from pymongo.errors import DuplicateKeyError
import datetime


class EntityStatistics(TaskStatisticsBase):
    def __init__(self, db_conf):
        metadatacols = ["topic_id", "date"]
        allowed_metrics = ["failure", "success"]
        TaskStatisticsBase.__init__(self, db_conf, metadatacols, allowed_metrics, 10)

    def inc_success_parse(self, topic_id):
        self.lazy_count({"topic_id":topic_id}, "success")

    def inc_fail_parse(self, topic_id):
        self.lazy_count({"topic_id":topic_id}, "failure")

    def send_statis_result(self):
        self._send_v += 1
        w_basket = {}
        with self._basket_lock:
            w_basket = self._task_basket
            self._task_basket = {}
        for key, value in w_basket.items():
            metadata = value['metadata']
            value.pop("metadata")
            retry_count = 100
            succ = False
            results = None
            while not succ and retry_count > 0:
                retry_count -= 1
                try:
                    results = self._collection.update_one(
                        filter={
                            'metadata': metadata,
                        },
                        update={
                            '$inc': value,
                            '$set': {"date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                        },
                        upsert=True
                    )
                except DuplicateKeyError, e:
                    # if upsert fail with DuplicateKeyError, just retry.
                    # if other exceptions occur, no retry
                    pass
                else:
                    succ = True
            if results and results.matched_count == 0:
                try:
                    init_doc = self._init_stats_document()
                    self._collection.update_one(filter={
                            "metadata":metadata,
                        },
                        update={
                            "$inc":init_doc,
                        },
                        upsert=True
                    )
                except Exception as e:
                    pass