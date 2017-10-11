# coding:utf-8

import sys

sys.path.append('..')
from bdp.i_crawler.i_extractor.ttypes import BaseInfo, ExtractInfo, PageParseInfo, ExStatus, ExFailErrorCode
from bdp.i_crawler.i_extractor.ttypes import CrawlInfo as CrawlInfoOld
from bdp.i_crawler.i_extractor.ttypes import Link

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

from bdp.i_crawler.i_extractor.ttypes import PageParseInfo
import beanstalkc


class PyBeanstalk(object):
    def __init__(self, host, port=11300):
        self.host = host
        self.port = port
        self.__conn = beanstalkc.Connection(host, port)
        # self.__conn.watch('extract_info');
        # self.__conn.use('entity_info');

    def __del__(self):
        self.__conn.close()

    def put(self, tube, body, priority=2 ** 31, delay=0, ttr=10):
        self.__conn.use(tube)
        return self.__conn.put(body, priority, delay, ttr)

    def reserve(self, tube, timeout=20):
        # for t in self.__conn.watching():
        #    self.__conn.ignore(t)
        self.__conn.watch(tube)
        return self.__conn.reserve(timeout)

    def clear(self, tube):
        try:
            while 1:
                job = self.reserve(tube, 1)
                if job is None:
                    break
                else:
                    job.delete()
        except Exception, e:
            print e

    def to_string(self, page_parseinfo):
        tMemory_b = TMemoryBuffer()
        tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
        page_parseinfo.write(tBinaryProtocol_b)
        str_entity = tMemory_b.getvalue()
        return str_entity


class Data(object):
    def get_page_parseinfo(self):
        url = 'http://www.baidu.com'
        url_id = 0
        site = 'www.baidu.com'
        site_id = 0
        domain = None
        domain_id = 0
        segment_id = 0
        src_type = 'test src_type'
        base_info = BaseInfo(url=url, url_id=url_id, site=site, site_id=site_id, domain=domain, domain_id=domain_id,
                             segment_id=segment_id, src_type=src_type)

        status_code = 0
        http_code = 0
        download_time = 0
        redirect_url = 'test redirect_url'
        elapsed = 0
        content_type = 'test content_type'
        content = 'test content1'
        page_size = 0

        crawl_info = CrawlInfoOld(status_code=status_code, http_code=http_code, download_time=download_time,
                                  redirect_url=redirect_url, elapsed=elapsed, content_type=content_type,
                                  content=content,
                                  page_size=page_size)

        ex_status = ExStatus.kEsSuccess
        extract_error = ExFailErrorCode.KExFailPageTranscoding
        redirect_url = 'test redirect_url'
        next_page_type = True
        struct_type = 0
        compose_type = 0
        content_type = 0
        topic_id = 0
        extracted_body_time = 0
        content_time = 0
        html_tag_title = 'test html_tag_title'
        analyse_title = 'test analyse_title3'
        zone = 'test zone'
        page_text = 'test page_text'
        content_language = 'test content_language'
        second_navigate = 'test second_navigate'
        valid_pic_url = 'test valid_pic_url'
        digest = 'test digest'
        finger_feature = 'test finger_feature'
        content_finger = 0
        simhash_finger = 0
        link_finger = 0

        link1 = Link(url='http://www.baidu.com/url3', type=0)
        link2 = Link(url='http://www.baidu.com/url2')
        link3 = Link(url='http://www.baidusdf.com/url5', type=0)
        link4 = Link(url='http://www.baidusdf.com/url6', type=2)
        links = [link1, link2, link3, link4]
        extract_data = 'test extract_data'

        extract_info = ExtractInfo(ex_status=ex_status, extract_error=extract_error, redirect_url=redirect_url,
                                   next_page_type=next_page_type, struct_type=struct_type, compose_type=compose_type,
                                   content_type=content_type, topic_id=topic_id,
                                   extracted_body_time=extracted_body_time,
                                   content_time=content_time, html_tag_title=html_tag_title,
                                   analyse_title=analyse_title,
                                   zone=zone, page_text=page_text, content_language=content_language,
                                   second_navigate=second_navigate, valid_pic_url=valid_pic_url, digest=digest,
                                   finger_feature=finger_feature, content_finger=content_finger,
                                   simhash_finger=simhash_finger,
                                   link_finger=link_finger, links=links, extract_data=extract_data)

        parse_extends = 'b'
        data_extends = 'c'
        scheduler = 'd'

        page_parseinfo = PageParseInfo(base_info=base_info, crawl_info=crawl_info, extract_info=extract_info,
                                       parse_extends=parse_extends, data_extends=data_extends, scheduler=scheduler)
        return page_parseinfo

    def get_page_parseinfo2(self):
        url = 'http://movie.douban.com'
        url_id = 0
        site = 'www.baidu.com'
        site_id = 0
        domain = None
        domain_id = 0
        segment_id = 0
        src_type = 'test src_type'
        base_info = BaseInfo(url=url, url_id=url_id, site=site, site_id=site_id, domain=domain, domain_id=domain_id,
                             segment_id=segment_id, src_type=src_type)

        status_code = 0
        http_code = 0
        download_time = 0
        redirect_url = 'test redirect_url'
        elapsed = 0
        content_type = 'test content_type'
        content = 'test content'
        page_size = 0

        crawl_info = CrawlInfoOld(status_code=status_code, http_code=http_code, download_time=download_time,
                                  redirect_url=redirect_url, elapsed=elapsed, content_type=content_type,
                                  content=content,
                                  page_size=page_size)

        ex_status = ExStatus.kEsSuccess
        extract_error = ExFailErrorCode.KExFailPageTranscoding
        redirect_url = 'test redirect_url'
        next_page_type = True
        struct_type = 0
        compose_type = 0
        content_type = 0
        topic_id = 0
        extracted_body_time = 0
        content_time = 0
        html_tag_title = 'test html_tag_title'
        analyse_title = 'test analyse_title'
        zone = 'test zone'
        page_text = 'test page_text'
        content_language = 'test content_language'
        second_navigate = 'test second_navigate'
        valid_pic_url = 'test valid_pic_url'
        digest = 'test digest'
        finger_feature = 'test finger_feature'
        content_finger = 0
        simhash_finger = 0
        link_finger = 0

        link1 = Link(url='http://movie.douban.com/url3')
        link2 = Link(url='http://movie.douban.com/url4')

        links = [link1, link2]
        extract_data = 'test extract_data'

        extract_info = ExtractInfo(ex_status=ex_status, extract_error=extract_error, redirect_url=redirect_url,
                                   next_page_type=next_page_type, struct_type=struct_type, compose_type=compose_type,
                                   content_type=content_type, topic_id=topic_id,
                                   extracted_body_time=extracted_body_time,
                                   content_time=content_time, html_tag_title=html_tag_title,
                                   analyse_title=analyse_title,
                                   zone=zone, page_text=page_text, content_language=content_language,
                                   second_navigate=second_navigate, valid_pic_url=valid_pic_url, digest=digest,
                                   finger_feature=finger_feature, content_finger=content_finger,
                                   simhash_finger=simhash_finger,
                                   link_finger=link_finger, links=links, extract_data=extract_data)

        parse_extends = 'b'
        data_extends = 'c'
        scheduler = 'd'

        page_parseinfo = PageParseInfo(base_info=base_info, crawl_info=crawl_info, extract_info=extract_info,
                                       parse_extends=parse_extends, data_extends=data_extends, scheduler=scheduler)
        return page_parseinfo

    def get_page_parseinfo_list(self):
        # pybeanstalk = PyBeanstalk('101.201.102.37')
        pybeanstalk = PyBeanstalk('127.0.0.1')

        l = []
        for i in range(200):
            job = pybeanstalk.reserve('download_rsp')
            parse_info = PageParseInfo()
            tMemory_o = TMemoryBuffer(job.body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            parse_info.read(tBinaryProtocol_o)
            l.append(parse_info)
        return l


if __name__ == '__main__':
    data = Data()
    print data.get_page_parseinfo()
