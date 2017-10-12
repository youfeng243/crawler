# coding=utf-8
import json
import re
import sys
import time
import traceback

import HTMLParser
from lxml import etree

from i_util.tools import get_url_info, url_query_decode, base64_encode_json, base64_decode_json, get_md5
from libs.html_extractor import HTMLExtractor
from libs.json_extractor import Json2XmlExtractor as JsonExtractor
from libs.models import ExtractorConfig

sys.path.append('..')

from bdp.i_crawler.i_extractor.ttypes import *

from libs.config_handle import ConfigHandler
from i_extractor.libs.extract_exception import *
from i_util import tools
import urllib
from libs.plugin_handler import PluginHandler

class RealExtractor(object):
    PARSER_TYPE_XPATH = 'xpath'
    PARSER_TYPE_RE = 're'
    PARSER_TYPE_JSON = 'json'
    PARSER_TYPE_HTML = 'html'
    URLS_TYPE_JSON_REPLACE = 'json_var'

    NEXT_PAGE_TYPE_COUNT = 'count'
    NEXT_PAGE_TYPE_FORMAT = 'format'

    def __init__(self, content, content_type, base_url, parser_config, conf, debug):
        self.conf = conf
        self.log = self.conf['log']
        self.content = content
        self.base_url = base_url
        self.parser_config = parser_config
        self.content_type = content_type
        self.xpath_doc = None
        self.debug = debug
        self.json_content, self.json_obj = None, None
        if self.parser_config and self.parser_config.datas_type:
            if self.parser_config.datas_type == self.PARSER_TYPE_HTML:  # 整个网页进行xpath加载
                try:
                    self.xpath_doc = etree.fromstring(self.content, etree.HTMLParser(recover=True),
                                                      base_url=self.base_url)
                except Exception:
                    self.log.error(traceback.format_exc())
                    raise ParserErrorException('content parser xpath_doc failed.')
            elif self.parser_config.datas_type == self.PARSER_TYPE_JSON:  # 整个网页进行json加载
                try:
                    try:
                        self.json_content = content.replace('\n', '')
                        if self.json_content:
                            self.json_obj = json.loads(self.json_content)
                    except Exception as e:
                        self.json_obj = None
                        self.log.warning('convert content to json failed, try to find json like string in content.')
                    if not self.json_obj and content:
                        try:
                            self.json_content = self._deal_json_content(content)
                            if self.json_content:
                                self.json_obj = json.loads(self.json_content)
                        except Exception as e:
                            self.json_obj = None
                            raise e

                except Exception:
                    raise ParserErrorException('content parser json_obj failed.')
            elif not self.content_type or self.content_type.split(';')[0].lower() in [
                        'text/html', 'text/xml']:
                try:
                    self.xpath_doc = etree.fromstring(self.content, etree.HTMLParser(recover=True),
                                                      base_url=self.base_url)
                except Exception:
                    self.log.error(traceback.format_exc())
                    raise ParserErrorException('content parser xpath_doc failed.')
            elif self.content_type.split(';')[0].lower() in [
                    'application/json']:  # 整个网页进行json加载
                try:
                    self.json_content = self._deal_json_content(self.content)
                    if self.json_content:
                        self.json_obj = json.loads(self.json_content)
                except Exception:
                    raise ParserErrorException('content parser json_obj failed.')
    # 对网页进行json格式处理
    @staticmethod
    def _deal_json_content(content):
        json_start_index = content.find('{')
        list_start_index = content.find('[')
        if list_start_index > -1 and (json_start_index == -1 or list_start_index < json_start_index):
            list_finish_index = content.rfind(']')
            if list_finish_index > list_start_index:
                json_content = content[list_start_index: list_finish_index + 1]
                return json_content
        elif json_start_index > -1 and (list_start_index == -1 or json_start_index < list_start_index):
            json_finish_index = content.rfind('}')
            if json_finish_index > json_start_index:
                json_content = content[json_start_index: json_finish_index + 1]
                return json_content
        return content

    # 根据parser_type获取相应的解析器
    def _get_extractor(self, parser_type):
        if not parser_type:
            return None
        elif parser_type == self.PARSER_TYPE_HTML:
            if self.xpath_doc == None:
                return HTMLExtractor(self.content, self.base_url, debug=self.debug)
            else:
                return HTMLExtractor(self.xpath_doc, self.base_url, debug=self.debug)
        elif parser_type == self.PARSER_TYPE_JSON:
            return JsonExtractor(self.json_obj, base_url=self.base_url, debug=self.debug)
        return None

    # 统一处理提取出来的数据
    @staticmethod
    def _deal_value(value):
        if isinstance(value, unicode):
            value = str(value.encode('utf-8'))
        elif isinstance(value, list):
            return value
        elif not isinstance(value, str):
            value = str(value)
        if isinstance(value, basestring):
            value = value.replace('\t', '').replace('\r', '').replace('\n', '')
        return value.strip()

    # 从网页中提取最终组合数据，并进行检查
    def extract_content_datas(self):
        extractor = self._get_extractor(self.parser_config.datas_type)
        "只有新闻类型才解析正文"
        if self.parser_config.id == 142:
            results = extractor.extract(self.parser_config.datas_rule, extract_content=True)
        else:
            results = extractor.extract(self.parser_config.datas_rule)
        #self.log.info("extract_content results_size:%d" % (len(str(results))))
        return results
    def extract_links(self):
        if not self.parser_config.urls_rule:return []
        extractor = self._get_extractor(self.parser_config.datas_type)
        rets = {}
        if not extractor:return []
        try:
            urls_rule = {}
            parse_rule = []
            for ll in self.parser_config.urls_rule:
                if ll['$parse_method'] == "filter":continue
                ll['$name'] = ll['$parse_rule']
                parse_rule.append(ll)
                urls_rule[ll['$parse_rule']] = ll
            rets = extractor.extract(parse_rule=parse_rule, all=False)
        except Exception,e:
            self.log.warning("extract_links failed, because {}".format(str(e)))
        links = []
        for k, v in rets.items():
            try:
                link_type = str(urls_rule[k]['$link_type'])
                if not str.isdigit(link_type):
                    link_type = 0
                else:link_type  = int(link_type)
                parser_id = str(urls_rule[k]['$parser_id'])
                if not str.isdigit(parser_id):
                    parser_id = -1
                else:
                    parser_id = int(parser_id)
                for link in v:
                    if not isinstance(link, Link):
                        continue
                    link.type = link_type
                    link.parse_extends = json.dumps({'parser_id': parser_id})
                    links.append(link)
            except Exception,e:
                pass
        return links


    def _build_count_query(self, ori_query_info):
        next_page_rule = self.parser_config.next_page_rule
        if not isinstance(next_page_rule, dict) or not next_page_rule.get('count_param'):
            return None
        count_param = next_page_rule.get('count_param')
        start_param = str(next_page_rule.get('start_param', '1'))
        if len(start_param) == 0 or not str.isdigit(start_param):
            start_param = 1
        else:
            start_param = int(start_param)
        count_value = str(next_page_rule.get('count_value', '1'))
        if len(count_value) == 0 or not str.isdigit(count_value):
            count_value = 1
        else:
            count_value = int(count_value)
        end_condition = str(next_page_rule.get('end_condition', '1'))
        if len(end_condition) == 0 or not str.isdigit(end_condition):
            end_condition = 1
        else:
            end_condition = int(end_condition)
        if ori_query_info.get(count_param):
            current_count_value = int(ori_query_info.get(count_param))
            if current_count_value < start_param:
                current_count_value = start_param
            next_count_value = current_count_value + count_value
            if next_count_value > end_condition:
                return None
            else:
                ori_query_info[count_param] = str(next_count_value)
        else:
            ori_query_info[count_param] = str(start_param)
        return ori_query_info

        # 使用count规则提取post下一页
    def _get_next_page_by_post_count(self):
        next_page = self.base_url
        url_info = get_url_info(self.base_url)
        query_info = url_query_decode(url_info.get('query'))
        post_param_str = query_info.get('HZPOST', None)
        if not post_param_str:
            return None
        post_param = base64_decode_json(post_param_str)
        next_post_param = self._build_count_query(post_param)
        if not next_post_param:
            return None
        else:
            query_info['HZPOST'] = base64_encode_json(next_post_param)
            next_page = next_page.split("?")[0] + "?" + urllib.urlencode(query_info)
        return next_page

    # 使用get_count规则提取下一页
    def _get_next_page_by_get_count(self):
        next_page = self.base_url
        url_info = get_url_info(self.base_url)
        query_info = url_query_decode(url_info.get('query'))
        new_query = self._build_count_query(query_info)
        if not new_query:
            return None
        else:
            next_page = next_page.split('?')[0] + "?" + urllib.urlencode(new_query)
        return next_page

    # 判断下一页是否满足终止条件
    def _deal_end_condition(self, next_page, next_num):
        end_condition = self.parser_config.next_page_rule['end_condition']
        if str.isdigit(str(end_condition)):
            end_condition = int(end_condition)
            if next_num > end_condition:
                next_page = None
        return next_page

    # 使用split规则提取下一页
    def _get_next_page_by_split(self):
        if self.parser_config.next_page_rule.has_key('split_param'):
            split_param = self.parser_config.next_page_rule['split_param']
            next_page = self.base_url.split(split_param)[0]
        else:
            return None
        page_param_type = self.parser_config.next_page_rule['page_param_type']
        page_param_rule = self.parser_config.next_page_rule['page_param_rule']
        extractor = self._get_extractor(page_param_type)
        if extractor:
            page_param = extractor.extract(page_param_rule)
            next_page += page_param
            return next_page
        else:
            return None

    # 提取下一页，并进行检查
    def extractor_next_page(self, current_post_params={}):
        next_page = None
        next_post_params = {}
        if not self.parser_config.next_page_rule: return next_page, next_post_params
        if self.parser_config.next_page_type == 'post_count':
            next_page = self._get_next_page_by_post_count()
        elif self.parser_config.next_page_type == 'get_count':
            next_page = self._get_next_page_by_get_count()
        elif self.parser_config.next_page_type == 'format':
            next_page = self._get_next_page_by_format()
        elif self.parser_config.next_page_type == 'split':
            next_page = self._get_next_page_by_split()
        if next_page:
            link = Link()
            link.url = next_page
            link.type = LinkType.kNextPageLink
            return link, next_post_params
        else:
            return None, next_post_params
    # 检查下一页，处理特殊情况

class SiteRecordIDGenerator(object):
    from i_util.global_defs import SITE_RECORD_ID

    @staticmethod
    def gen_for_records(record):
        try:
            # 如果有提取数据
            if isinstance(record, dict) and record:
                # 如果是列表数据
                if isinstance(record.get('datas', None), (list, tuple)):
                    datas = record['datas']
                    # 遍历列表中的_site_record_id进行转换
                    for idx, item in enumerate(datas):
                        if item.has_key("_site_record_id"):
                            datas[idx]['_site_record_id'] = get_md5(datas[idx]['_site_record_id'])
                # 不是列表数据转换site_record_id
                elif record.has_key('_site_record_id'):
                        record['_site_record_id'] = get_md5(record['_site_record_id'])
        except Exception:
            raise ParserErrorException("count _site_record_id failed")

def singleton(cls, *args, **kw):
    instance = {}
    cls.singleton_lock.acquire()
    def _singleton(*args, **kw):
        if cls not in instance:
            instance[cls] = cls(*args, **kw)
        return instance[cls]
    cls.singleton_lock.release()
    return _singleton

class Extractor(object):
    CHARSET_PATTERN = re.compile('<meta[^>]*?(?:charset|CHARSET)=["\']?([a-zA-Z0-9\\-]+)["\']?[^>]*?>', re.I | re.S)
    ENCODING_PATTERN = re.compile('<\?xml[^>]*? (?:encoding="[a-zA-Z0-9\\-]+")[^>]*?\?>', re.I | re.S)
    BR_PATTERN = re.compile("</\s*br\s*>", re.I | re.S)
    CONTENT_THRESHOLD = 200
    def __init__(self, conf):
        self.log = conf['log']
        self.log.info('Extractor load start')
        self.conf = conf
        self.config_handler = ConfigHandler(conf, self.log)
        self.plugin_handler = PluginHandler()
        self.log.info('Extractor load finish')

    def _get_charset(self, content):
        nodes = re.findall(self.CHARSET_PATTERN, content)
        if nodes:
            return nodes[0]
        else:
            return None

    # 对网页进行编码
    def decode_body(self, body, link, download_type='simple'):
        if not body or isinstance(body, unicode):
            return body, None
        if download_type == 'phantom':  # phantomjs抓取的网页一定是utf-8编码
            try:
                return body.decode('utf-8'), 'utf-8'
            except:
                pass
        charset = self._get_charset(body)
        for try_charset in ['utf-8', 'gb18030',  'gbk', 'utf-16']:
            try:
                return body.decode(try_charset), try_charset
            except Exception as e:
                pass
        try:
            return body.decode(charset, errors = 'ignore'), charset
        except Exception as e:
            pass
        self.log.warning("the page from {} can't not correct decode".format(link))
        return None, None

    def pack_crawl_info(self, download_rsp):
        """
        :param DownloadRsp:
        :return CrawlInfo:
        """
        craw_info = CrawlInfo()
        craw_info.content = download_rsp.content
        craw_info.status_code = download_rsp.status
        craw_info.http_code = download_rsp.http_code
        craw_info.download_time = download_rsp.download_time
        craw_info.redirect_url = download_rsp.redirect_url
        craw_info.elapsed = download_rsp.elapsed
        craw_info.content_type = download_rsp.content_type
        craw_info.page_size = download_rsp.page_size
        return craw_info

    def pack_base_info(self, download_rsp):
        """
        :param DonwloadRsp:
        :return BaseInfo:
        """
        base_info = BaseInfo()
        base_info.url = download_rsp.url
        url_info = get_url_info(base_info.url)
        base_info.domain_id = url_info.get('domain_id')
        base_info.domain = url_info.get('domain')
        base_info.site = url_info.get('site')
        base_info.site_id = url_info.get('site_id')
        base_info.url_id = url_info.get('url_id')
        base_info.src_type = download_rsp.src_type
        return base_info

    def fix_links_info(self, links, custom_links, parser_config ):
        """
        :param Links:
        :param link_extend_rule: [{'rule':'', 'parser_id':1}}
        :return:
        """
        rets = []
        links_set = {}
        for link in links:
            links_set[link.url] = link
        for cl in custom_links:
            if links_set.has_key(cl.url):
                links_set[cl.url].parse_extends = cl.parse_extends
                links_set[cl.url].type = cl.type
            else:
                links_set[cl.url] = cl
        links = links_set.values()
        filter_rule = []
        try:
            for ll in parser_config.urls_rule:
                if ll['$parse_method'] == u"filter":
                    filter_rule.append(ll)
        except Exception, e:
            self.log.warning("filter_links failed, because {}".format(str(e)))
        for link in links:
            try:
                link.url = tools.url_encode(link.url)
                url_info = tools.get_url_info(link.url)
                link.domain = url_info.get('domain')
                link.site = url_info.get('site')
                link.site_id = url_info.get('site_id')
                link.domain_id = url_info.get('domain_id')
                link.url_id = url_info.get('url_id')
                if not link.type:
                    for r in filter_rule:
                        try:
                            if r.get('$parse_rule') and re.findall(r['$parse_rule'], link.url):
                                link_type = str(r['$link_type'])
                                if not str.isdigit(link_type):
                                    link_type = LinkType.kUnknownLink
                                else:
                                    link_type = int(link_type)
                                parser_id = str(r['$parser_id'])
                                if not str.isdigit(parser_id):
                                    parser_id = -1
                                else:
                                    parser_id = int(parser_id)
                                link.type = link_type
                                link.parse_extends = json.dumps({'parser_id': parser_id})
                                break
                        except Exception as e:
                            pass
                    if not link.type:
                        link.type = LinkType.kUnknownLink
                rets.append(link)
            except Exception as e:
                self.log.warning("fix_links_info of {} error, because of {}".format(link.url, e.message))

        return rets

    def pack_extract_info(self, extract_info, page_data, download_rsp, parser_config):
        """
        :param ExtractData:
        :param DownloadRsp:
        :return ExtractInfo:
        """
        extract_data = page_data.get('data', {})
        common_data = page_data.get('common_data')
        links = page_data.get('links')
        extract_info.ex_status = ExStatus.kEsNotExtract  # default not extract
        extract_info.redirect_url = download_rsp.redirect_url
        extract_info.content_time = common_data.get('public_time')
        extract_info.topic_id = parser_config.topic_id
        extract_info.html_tag_title = common_data.get('title')
        extract_info.page_text = common_data.get('content')
        extract_info.content_language = common_data.get('lang')
        extract_info.content_finger = common_data.get('content_finger')
        if not extract_info.content_finger and extract_info.page_text:
            extract_info.content_finger = tools.get_md5_i64(extract_info.page_text)
        extract_info.links = links
        extract_info.extract_data = json.dumps(extract_data)
        self.log.info("url:%s\textract_data_length:%d" %(str(download_rsp.url),len(extract_info.extract_data)))
        return extract_info

    def extract(self, download_rsp):
        """
        :param DownloadRsp:e
        :return PageParseInfo:
        """
        _start_time = time.time()
        crawl_info = self.pack_crawl_info(download_rsp)
        base_info = self.pack_base_info(download_rsp)
        pageparse_info = PageParseInfo()
        extract_info = ExtractInfo()
        parser_config = None
        parse_extends = None
        extract_info.struct_type = 0
        self.log.info("url:{}\tcrawl_status:{}\tcontent_type:{}".format(download_rsp.url, download_rsp.status, download_rsp.content_type))
        try:
            try:
                parse_extends = json.loads(download_rsp.parse_extends)
            except Exception as e:
                pass
                # self.log.warning('extract parser_extends is none or failed'.format(download_rsp.url))
            if not parse_extends: parse_extends = {}
            if parse_extends:  # 通过指定解析规则查找规则
                parser_id = parse_extends.get('parser_id')
                parser_config = self.config_handler.get_config_by_id(parser_id)
            if not parser_config:  # 通过url查找规则
                parser_config = self.config_handler.get_config_by_url(download_rsp.url)
            if not parser_config and download_rsp.redirect_url:  # 通过redirect_url查找规则
                parser_config = self.config_handler.get_config_by_url(download_rsp.redirect_url)
            if parser_config == None:  # 如果找不到解析规则抛出异常
                self.log.warning("not match any parserconfig, will be skip!".format(download_rsp.url))
                raise NoParserConfigException()
            parse_extends['parser_id'] = parser_config.id
            self.log.info("url:{}\tparser_config_id:{}".format(download_rsp.url, str(parser_config.id)))
            if download_rsp.status != 0:
                raise NoBodyException()
            if download_rsp.content_type and download_rsp.content_type.find("text") < 0 and download_rsp.content_type.find('json') < 0:
                raise NoNeedParseException()
            if download_rsp.content is None or len(download_rsp.content) < 1:
                self.log.error('haizhi- url = {} download_rsp_content:null'.format(download_rsp.url))
                raise NoBodyException()

            is_debug = parse_extends.get('debug', False)
            # 网页编码
            body, charset = self.decode_body(download_rsp.content, download_rsp.url)
            if body is None:
                self.log.error('haizhi- url = {} occur error when decoding download_rsp.content'.format(download_rsp.url))
                raise PageDecodeException()
            html_parser = HTMLParser.HTMLParser()
            body = html_parser.unescape(body)
            body = self.CHARSET_PATTERN.sub("", body)
            body = self.ENCODING_PATTERN.sub("", body)
            body = self.BR_PATTERN.sub("<br>", body)
            crawl_info.content = body.encode('utf-8')
            # 解析器初始化
            extrartor = RealExtractor(body, download_rsp.content_type, download_rsp.url, parser_config, self.conf, debug=is_debug)
            # 解析数据
            results = extrartor.extract_content_datas()
            data_extends = None
            try:
                if download_rsp.data_extends:
                    data_extends = json.loads(download_rsp.data_extends)
            except:
                pass
            # 将data_extends更新到结果
            if data_extends and isinstance(data_extends, dict):
                if not isinstance(results.get('data', None), dict):
                    results['data'] = {}
                results['data'].update(data_extends)
            self.log.info("ori_site_record_id:{}".format(results['data'].get("_site_record_id")))
            # 计算_site_record_id
            if not is_debug:
                SiteRecordIDGenerator.gen_for_records(results['data'])
            self.log.info("cal_site_record_id:{}".format(results['data'].get("_site_record_id")))

            custom_links = extrartor.extract_links()
            try:
                post_params = parse_extends.get('post_data', {})
                next_page_link, next_post_params = extrartor.extractor_next_page(post_params)
                #增加下一页到提链集
                if next_page_link:
                    next_page_link.anchor = "下一页"
                    custom_links.append(next_page_link)
                self.log.info("url:{}\tget_next_page:{}".format(download_rsp.url, str(next_page_link)))
            except Exception:
                self.log.error('haizhi- url = {} get next_page_link failed, because of: {}'.format(
                    download_rsp.url, traceback.format_exc()))

            if parser_config.plugin:
                try:
                    for plugin_info in parser_config.plugin:
                        extract_entry = self.plugin_handler.get_plugin_entry(plugin_info)
                        results = extract_entry(download_rsp.url, crawl_info.content, results)
                except Exception as e:
                    self.log.error("haizhi- url:{}\tplugin extract error, because of: {}".format(
                        download_rsp.url, traceback.format_exc()))
                    raise e
            results['links'] = self.fix_links_info(results['links'], custom_links, parser_config)
            link_cnt = 0
            for link in results['links']:
                if link.type > 0:link_cnt += 1
            self.log.info("url:{}\tcustom_links_length:{}".format(download_rsp.url, link_cnt))

            if not results or (not results.get("data") and not results.get('links')):
                raise ParseNothingException()
            extract_info = self.pack_extract_info(extract_info, results, download_rsp, parser_config)
            extract_info.links = results.get('links', [])
            self.log.info('url:{}\ttopic_id:{}'.format(download_rsp.url, str(extract_info.topic_id)))
            #如果有多条自定义的链接提取,则当成列表页
            if custom_links and len(custom_links) > 1:
                extract_info.struct_type |= StructType.index_type
            if extract_info.extract_data and len(extract_info.extract_data) > 1:
                extract_info.struct_type |= StructType.content_type
                # pageparse_info.extract_info = extract_info
        # TODO fix add more error code
        except NoParserConfigException as e:
            extract_info.ex_status = ExStatus.kEsNotExtract
            extract_info.extract_error = ExFailErrorCode.kExFailParseRuleLimit
        except NoBodyException as e:
            self.log.error(e.message)
            extract_info.ex_status = ExStatus.kEsFail
            extract_info.extract_error = ExFailErrorCode.kExFailNoContent
        except PageDecodeException as e:
            extract_info.ex_status = ExStatus.kEsFail
            self.log.error(e.message)
            extract_info.extract_error = ExFailErrorCode.KExFailPageTranscoding
        except ParseNothingException as e:
            extract_info.ex_status = ExStatus.kEsFail
            self.log.error(e.message)
            extract_info.extract_error = ExFailErrorCode.kExFailParseNothing
            extract_info.struct_type |= StructType.fail_type
        except ParserErrorException as e:
            extract_info.ex_status = ExStatus.kEsFail
            self.log.error(e.message)
            extract_info.extract_error = ExFailErrorCode.kExFailPageParse
        except NoNeedParseException as e:
            self.log.error(e.message)
            extract_info.ex_status = ExStatus.kEsSuccess
        except RequireException as e:
            self.log.error(e.message)
            extract_info.struct_type |= StructType.fail_type
            extract_info.ex_status = ExStatus.kEsFail
            extract_info.extract_error = ExFailErrorCode.KExFailRequireError
        except Exception as e:
            self.log.info('url:{}\tunknown exception occur'.format(download_rsp.url))
            self.log.warning(traceback.format_exc())
            extract_info.ex_status = ExStatus.kEsFail
        else:
            extract_info.ex_status = ExStatus.kEsSuccess
        finally:
            # del extrartor, body, results, parser_config
            self.log.debug(extract_info)
            if parser_config:
                extract_info.topic_id = parser_config.topic_id
            pageparse_info.extract_info = extract_info
            pageparse_info.base_info = base_info
            pageparse_info.crawl_info = crawl_info
            pageparse_info.parse_extends = json.dumps(parse_extends)
            pageparse_info.scheduler = download_rsp.scheduler
            self.log.info('haizhi- url:{}\tstruct_type:{}\tex_status:{}\terror_code:{}\tspend_time:{:.2f}ms'.format(
                download_rsp.url, extract_info.struct_type,extract_info.ex_status,
                extract_info.extract_error, (time.time() - _start_time)*1000))
            return pageparse_info

    def save_parser_config(self, config_json):
        extract_config = ExtractorConfig()
        try:
            if not isinstance(config_json, dict):
                config_json = json.loads(config_json)
            extract_config.id = config_json.get('id')
            extract_config.name = config_json.get('name')
            extract_config.topic_id = config_json.get('topic_id')
            extract_config.url_format = config_json.get('url_format')
            extract_config.datas_type = config_json.get('datas_type')
            extract_config.datas_rule = config_json.get('datas_rule')
            extract_config.urls_rule = config_json.get('urls_rule')
            extract_config.plugin = config_json.get('plugin')
            extract_config.http_method = config_json.get('http_method')
            extract_config.next_page_type = config_json.get('next_page_type')
            extract_config.next_page_rule = config_json.get('next_page_rule')
            extract_config.post_params = config_json.get('post_params')
            extract_config.label = config_json.get('label')
            self.log.info(extract_config.name)
            status, check_info = self.config_handler.upsert(extract_config)
            return ExtractRsp(status=str(status), message=json.dumps(check_info))
        except Exception as e:
            self.log.warning("save_config {} error".format(config_json))
            return ExtractRsp(status=str(False), message=json.dumps({'error':e.message}))

    def reload_parser_config(self, parser_id="-1"):
        try:
            self.config_handler.load_config_from_database(parser_id)
            return ExtractRsp(status='success', message="")
        except Exception,e:
            return ExtractRsp(status='failed', message=e.message)

