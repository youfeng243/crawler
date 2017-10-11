# -*- coding:utf-8 -*-
import itertools
import json
import threading

from pymongo.errors import DuplicateKeyError
from queue import Queue, Empty
from datetime import datetime
import time

import pymongo

delta_seconds = 2 * 60

class TaskStatisticsBase(threading.Thread):
    def __init__(self, db_conf, metadatacols, allowed_metric, ds=2*60):
        threading.Thread.__init__(self)
        self.daemon = True
        self._metadatacols = metadatacols[:]
        if "date" not in self._metadatacols:
            self._metadatacols.append("date")
        self._metadatacols.sort()
        self._allowed_metric = allowed_metric[:]
        self._basket_lock = threading.Lock()
        self._task_basket = {}
        self._send_v = 0
        self._queue = Queue()
        self._collection = None
        self._sig_block = Queue()
        self._delta_seconds = ds
        mongo_client = pymongo.MongoClient(db_conf['host'], db_conf['port'])
        if db_conf['username'] != '':
            mongo_client[db_conf['db']].authenticate(db_conf['username'], db_conf['password'])
        self._collection = mongo_client[db_conf['db']][db_conf['collection']]

    def _prepare_db(self):
        self._collection.create_index(
            [
                ("metadata.{}".format(meta),pymongo.ASCENDING) for meta in self._metadatacols
            ],
            background=True,
            unique=True
        )

    def _init_stats_document(self):
        hourly_fields = ['hourly.%s' %(i) for i in range(24)]
        daily_fields = ['daily']
        all_field_names = [
            '%s.%s' % (metric, field)
            for metric, field in itertools.product(self._allowed_metric, hourly_fields + daily_fields)
            ]
        stats_document = {}
        for field in all_field_names:
            stats_document[field] = 0
        return stats_document

    def lazy_count(self, metadata, metric):
        """
        :param metadata: 包含metadata的字典
        :param metric:  要递增的统计分量
        :return:
        """
        if not metric in self._allowed_metric:
            return False
        ctime = datetime.now()
        metadata['date'] = ctime.strftime("%Y-%m-%d")
        key = "\001\001".join([str(metadata.get(x)) for x in self._metadatacols])
        with self._basket_lock:
            if key not in self._task_basket:
                self._task_basket[key] = {"metadata":metadata}
            task_basket = self._task_basket[key]
            daily_field = "%s.daily" % (metric,)
            hourly_field = "%s.hourly.%d" %(metric, ctime.hour)
            task_basket[daily_field] = task_basket.get(daily_field, 0) + 1
            task_basket[hourly_field] = task_basket.get(hourly_field, 0) + 1
        return True

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
                            '$inc': value
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

    def stop(self):
        self._sig_block.put("stop")

    def run(self):
        self._prepare_db()
        while True:
            try:
                sig = self._sig_block.get(block=True, timeout=self._delta_seconds)
                if sig == "stop":
                    break
            except Empty:
                self.send_statis_result()
            except Exception as e:
                pass
        self.send_statis_result()


