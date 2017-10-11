from i_util.task_statistics_base import TaskStatisticsBase
from pymongo.errors import DuplicateKeyError
import datetime


class SeedStatistics(TaskStatisticsBase):
    def __init__(self, db_conf):
        metadatacols = ["site", "seed_id", "seed_name", "date"]
        allowed_metrics = ["download_count", "content_page_count", "download_success_count", "download_fail_count",
                           "download_content_success_count"]
        TaskStatisticsBase.__init__(self, db_conf, metadatacols, allowed_metrics)

    def inc_download_count(self, site, seed_id, seed_name):
        self.lazy_count({"site": site, "seed_id": seed_id, "seed_name": seed_name}, "download_count")

    def inc_content_page_count(self, site, seed_id, seed_name):
        self.lazy_count({"site": site, "seed_id": seed_id, "seed_name": seed_name}, "content_page_count")

    def inc_download_success_count(self, site, seed_id, seed_name):
        self.lazy_count({"site": site, "seed_id": seed_id, "seed_name": seed_name}, "download_success_count")

    def inc_download_fail_count(self, site, seed_id, seed_name):
        self.lazy_count({"site": site, "seed_id": seed_id, "seed_name": seed_name}, "download_fail_count")

    def inc_download_content_success_count(self, site, seed_id, seed_name):
        self.lazy_count({"site": site, "seed_id": seed_id, "seed_name": seed_name}, "download_content_success_count")

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
                            '$set': {"last_success_download_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
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
