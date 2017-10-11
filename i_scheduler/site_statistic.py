# -*- coding:utf-8 -*-

import json

from i_util.task_statistics_base import TaskStatisticsBase
from pymongo.errors import DuplicateKeyError
import datetime


class SiteStatistics(TaskStatisticsBase):
    def __init__(self, db_conf):
        metadatacols = ["site", "site_name", "date"]
        allowed_metrics = ["request_count", "response_count", "success_count", "fail_count"]
        TaskStatisticsBase.__init__(self, db_conf, metadatacols, allowed_metrics)

    def inc_request_count(self, site, site_name):
        self.lazy_count({"site": site, "site_name": site_name}, "request_count")

    def inc_response_count(self, site, site_name):
        self.lazy_count({"site": site, "site_name": site_name}, "response_count")

    def inc_success_count(self, site, site_name):
        self.lazy_count({"site": site, "site_name": site_name}, "success_count")

    def inc_fail_count(self, site, site_name):
        self.lazy_count({"site": site, "site_name": site_name}, "fail_count")

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
