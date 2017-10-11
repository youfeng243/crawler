#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  gikieng.li
# @Date    2017-03-27 18:10

from i_util.task_statistics_base import TaskStatisticsBase


class DataStatistics(TaskStatisticsBase):
    def __init__(self, db_conf):
        metadatacols = ["topic_id", "date"]
        allowed_metrics = ["failure", "success_insert", "success_update"]
        TaskStatisticsBase.__init__(self, db_conf, metadatacols, allowed_metrics, 10)

    def inc_success_insert(self, topic_id):
        self.lazy_count({"topic_id":topic_id}, "success_insert")

    def inc_success_update(self, topic_id):
        self.lazy_count({"topic_id":topic_id}, "success_update")

    def inc_failure(self, topic_id):
        self.lazy_count({"topic_id":topic_id}, "failure")