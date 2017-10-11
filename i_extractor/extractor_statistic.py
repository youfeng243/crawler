#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  gikieng.li
# @Date    2017-03-27 18:10
import json

from i_util.task_statistics_base import TaskStatisticsBase
from pymongo.errors import DuplicateKeyError
import datetime


class ExtractorStatistics(TaskStatisticsBase):
    def __init__(self, db_conf):
        metadatacols = ["parser_id", "site", "date"]
        allowed_metrics = ["extract_fail", "extract_skip", "extract_success", "download_fail"]
        TaskStatisticsBase.__init__(self, db_conf, metadatacols, allowed_metrics)

    def inc_extract_fail(self, site, parser_id):
        self.lazy_count({"site":site, "parser_id":parser_id}, "extract_fail")

    def inc_extract_skip(self, site, parser_id):
        self.lazy_count({"site":site, "parser_id":parser_id}, "extract_skip")

    def inc_extract_success(self, site, parser_id):
        self.lazy_count({"site":site, "parser_id":parser_id}, "extract_success")

    def inc_download_fail(self, site, parser_id):
        self.lazy_count({"site":site, "parser_id":parser_id}, "download_fail")

    def task_stats(self, obj):
        parse_extends = {}
        if obj.parse_extends:
            parse_extends = json.loads(obj.parse_extends)
        parser_id = -1
        site = obj.base_info.site
        if parse_extends:
            parser_id = parse_extends.get('parser_id', -1)
        if not parser_id:
            parser_id = -1
        ex_status = obj.extract_info.ex_status
        crawl_status = obj.crawl_info.status_code
        if crawl_status == 1:
            self.inc_download_fail(site=site, parser_id=parser_id)
        elif ex_status == 2:
            self.inc_extract_success(site=site, parser_id=parser_id)
        elif ex_status == 1:
            self.inc_extract_skip(site=site, parser_id=parser_id)
        elif ex_status == 3:
            self.inc_extract_fail(site=site,parser_id=parser_id)

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
