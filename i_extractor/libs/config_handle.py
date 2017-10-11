# coding=utf-8
import re
import sys
import traceback


reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('..')

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import ExtractorConfig,HistoryExtractorConfig
from pylru import lrucache
from plugin_handler import PluginHandler

def get_new_config_dict():
    return lrucache(1024 * 64)

class ConfigHandler(object):
    def __init__(self, conf, logger):
        self.logger = logger
        self.plugin_handler = PluginHandler()
        import threading
        self.mutex_lock = threading.Lock()
        try:
            engine = create_engine(conf['parser_config_database'])
            ExtractorConfig.metadata.create_all(engine)
            HistoryExtractorConfig.metadata.create_all(engine)
            self.Dsession = sessionmaker(bind=engine)
        except Exception as e:
            self.logger.error(e.message)
            raise e
        self.config_dict = get_new_config_dict()
        self.fresh_config_dict = get_new_config_dict()
        self.load_config_from_database()

    def check_config(self, config):
        if not isinstance(config, ExtractorConfig):
            raise Exception("Not invalid parseConfig: {}".format(type(config)))
        check_info = {}
        status = True
        if config.datas_rule and not isinstance(config.datas_rule, list):
            status = False
            check_info['datas_rule'] = "invalid datas_rule"
        if config.urls_rule and not isinstance(config.urls_rule, list):
            status = False
            check_info['urls_rule'] = "invalid urls_rule"
        if config.post_params and not isinstance(config.post_params, dict):
            status = False
            check_info['post_params'] = "invalid post_param"
        if config.next_page_type and config.next_page_type != "None" and not isinstance(config.next_page_rule, dict):
            status = False
            check_info['next_page_rule'] = "next_page_rule error"
        if config.name is None or not isinstance(config.name, basestring) or len(config.name) < 2:
            status = False
            check_info['name'] = "invalid name"
        if config.url_format is None or not isinstance(config.url_format, basestring) or len(config.url_format) < 8:
            status = False
            check_info['url_format'] = "invalid url_format"
        if config.datas_type not in ['html', 'json']:
            status = False
            check_info['datas_type'] = "invalid datas_type"
        return status, check_info
    #配置模板写库
    def upsert(self, config):
        success = True
        status, check_info = self.check_config(config)
        if status == False:
            return status, check_info
        session = self.Dsession()
        try:
            query = session.query(ExtractorConfig)
            his_config = HistoryExtractorConfig()
            record = query.filter(ExtractorConfig.id == config.id).one_or_none()
            if record:
                try:
                    tmp_config_d = vars(record)
                    for k, v in tmp_config_d.items():
                        if k == '_sa_instance_state':continue
                        his_config.__setattr__(k, v)
                    del his_config.id
                    del his_config.create_time
                    his_config.rel_id = record.id
                    session.add(his_config)
                except Exception,e:
                    self.logger.warning("save history_parser_config error, because of :{}".format(str(traceback.format_exc())))
                config_d = vars(config)
                del config._sa_instance_state
                for k, v in config_d.items():
                    record.__setattr__(k, v)
            else:
                session.add(config)
            session.flush()
            session.commit()
            check_info['id'] = config.id
            self.load_config_from_database(config.id)
            self.logger.info('upsert parser success {}'.format(str(vars(config))))
        except Exception as e:
            success = False
            check_info['upsert_error'] = str(e)
            self.logger.error(traceback.format_exc())
        finally:
            if session: session.close()
        return success, check_info
    # 从数据库加载配置模板
    def load_config_from_database(self, check_id = -1):
        if check_id == "-1":check_id = -1
        try:
            check_id = int(check_id)
        except Exception as e:
            check_id = -1
        #读库写内存加锁
        with self.mutex_lock:
            session = self.Dsession()
            self.fresh_config_dict.clear()
            try:
                if check_id == -1:
                    records = session.query(ExtractorConfig)
                    for r in records.all():
                        self.fresh_config_dict[r.id] = r
                        plugin = self.fresh_config_dict[r.id].plugin
                        if plugin:
                            plugin_infos = []
                            try:
                                plugin_infos = self.plugin_handler.get_plugin_module_info(plugin)
                                for plugin_info in plugin_infos:
                                    self.plugin_handler.load_plugin_module(plugin_info)
                            except Exception as e:
                                self.logger.warning("load plugin error:{}, because of {}".format(str(plugin), e.message))
                            self.fresh_config_dict[r.id].plugin = plugin_infos
                        self.logger.info("load parser_config:{}".format(r.name))
                    #交换两个配置字典
                    self.config_dict, self.fresh_config_dict = self.fresh_config_dict, self.config_dict
                else:
                    query = session.query(ExtractorConfig)
                    record = query.filter(ExtractorConfig.id == check_id).one_or_none()
                    if record:
                        self.config_dict[record.id] = record
                        plugin = self.config_dict[record.id].plugin
                        if plugin:
                            plugin_infos = []
                            try:
                                plugin_infos = self.plugin_handler.get_plugin_module_info(plugin)
                                for plugin_info in plugin_infos:
                                    self.plugin_handler.load_plugin_module(plugin_info)
                            except Exception as e:
                                self.logger.warning(
                                    "load plugin error:{}, because of {}".format(str(plugin), e.message))
                            self.config_dict[record.id].plugin = plugin_infos
                        self.logger.info("reload parser_config:{}".format(record.name))
                    #如果不存在该record说明是删除的。
                    elif self.config_dict.get(check_id):
                        record = self.config_dict[check_id]
                        del self.config_dict[record.id]
                        self.logger.info("delete parser_config:{}".format(record.name))
            except Exception as e:
                self.logger.error(traceback.format_exc())
            finally:
                if session: session.close()
            #return ret

    # 通过id查找配置模板
    def get_config_by_id(self, _id):
        check_id = _id
        if isinstance(check_id, basestring):
            check_id = str(check_id)
        elif not isinstance(check_id, int):
            return None
        if isinstance(check_id, basestring) and not str.isdigit(check_id):
            self.logger.warning('the parser_config.id required digit_str or interger, return None')
            return None
        if not isinstance(check_id, int) and not isinstance(check_id, basestring):
            self.logger.warning("the parser_config.id can't be type {} object, return None".format(type(check_id)))
            return None
        try:
            check_id = int(check_id)
        except Exception,e:
            return None
        record = self.config_dict.get(check_id, None)
        return record

    def get_config_by_url(self, url):
        if not isinstance(url, basestring) or len(url) < 3:
            self.logger.warning('url is too short or not basestring type')
            return None
        if not isinstance(url, unicode):
            url = unicode(url)
        config = None
        try:
            for v in self.config_dict.values():
                try:
                    if not v.weight:continue
                    if not v.url_format:continue
                    if re.match(v.url_format, url):
                        config = v
                        break
                except Exception as e:
                    self.logger.warning("invalid url_format:%s" % v.url_format)
            #更改下优先级,这里不能赋值,防止在查询的过程中修改了,只要取一下就能改变优先值
            if config and self.config_dict.get(config.id):
                pass
        except Exception, e:
            self.logger.warning('some unknown error occurs when get_config_by_url:{}'.format(str(traceback.format_exc())))
            return None
        return config

if __name__ == '__main__':
    import conf
    config_h = ConfigHandler(conf, conf.log)
    print config_h.get_config_by_url("http://www.gzg2b.gov.cn/Sites/_Layouts/ApplicationPages/News/News.aspx?ColumnID=AD750336-AF36-4CE6-BFA9-033F5AD7C776");
    print config_h.get_config_by_url("http://www.gzg2b.gov.cn/Sites/_Layouts/ApplicationPages/News/News.aspx?ColumnID=AD750336-AF36-4CE6-BFA9-033F5AD7C776");

    config_h.load_config_from_database(-1)
    print config_h.get_config_by_url("http://www.gzg2b.gov.cn/Sites/_Layouts/ApplicationPages/News/News.aspx?ColumnID=AD750336-AF36-4CE6-BFA9-033F5AD7C776");
