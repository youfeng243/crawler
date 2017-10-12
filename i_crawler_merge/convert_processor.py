# coding:utf-8

import sys

from link_merge import LinkMerge
from webpage_merge import WebpageMerge

sys.path.append('..')

from i_util.tools import  get_url_info
import json, traceback
from libs.mongo_database import MongoLinkConnection as LinkConnection,\
                                MongoWebpageConnection as WebpageConnection

class ConvertProccessor(object):
    def __init__(self, conf):
        self.log = conf["log"]
        self.link_connection = LinkConnection(
                                        host=conf['linkattr_db']['host'],
                                        port=conf['linkattr_db']['port'],
                                        db=conf['linkattr_db']['db'],
                                        username=conf['linkattr_db']['username'],
                                        password=conf['linkattr_db']['password'])

        self.webpage_connection = WebpageConnection(
                                        host=conf['webpage_db']['host'],
                                        port=conf['webpage_db']['port'],
                                        db=conf['webpage_db']['db'],
                                        username=conf['webpage_db']['username'],
                                        password=conf['webpage_db']['password'])

    def start_convert(self, page_parseinfo):
        link_attr = None
        try:
            if not page_parseinfo: return None

            extractor_crawl_info = page_parseinfo.crawl_info
            base_info = page_parseinfo.base_info
            extract_info = page_parseinfo.extract_info
            url = base_info.url
            url_info = get_url_info(url)
            data_extends = page_parseinfo.data_extends
            self.log.info('merge_start\turl:{}'.format(url))
            #处理网页信息
            webpage_merge = WebpageMerge(self.webpage_connection)
            webpage_obj = webpage_merge.merge_webpage(base_info,
                                                      extractor_crawl_info,
                                                      page_parseinfo.data_extends,
                                                      page_parseinfo.parse_extends)
            # 将处理完的网页存回数据库
            webpage_merge.save_webepage(base_info.domain, webpage_obj)

            # scheduler应为json格式字符串
            scheduler_obj = {}
            try:
                scheduler_obj = json.loads(page_parseinfo.scheduler)
            except Exception as e:
                pass
            base_crawl_info = dict(extractor_crawl_info.__dict__)
            base_crawl_info.update(scheduler_obj)

            if base_crawl_info.get("status_code") != 0:
                return None

            link_merge = LinkMerge(self.link_connection, url_info, extract_info.links, self.log)
            link_attr = link_merge.merge_link_attr(base_info,
                                                   extract_info,
                                                   scheduler_obj,
                                                   base_crawl_info,
                                                   data_extends
                                                   )
            # 将处理完的链接信息放回数据库
            link_merge.save_link_attrs()

            self.log.info('merge url:{}'.format(base_info.url))
            if base_info.src_type == "webpage":
                return None
        except Exception as e:
            self.log.error("url:{}".format(traceback.format_exc()))
        return link_attr
