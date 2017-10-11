# coding=utf-8
import sys
import os
import traceback
import json

sys.path.append('..')
from extractors.default.default_extractor import DefaultExtractor

from common_parser_lib.parser_tool import parser_tool
from common.log import log
from common.mysql_utils import get_mysql_conn, dbrecord_to_dict
from common.topic_manager import TopicManager
from common_parser_lib import toolsutil
from i_util.tools import crawler_basic_path

class EntityExtractorRoute:
    def __init__(self, conf):
        self.conf = conf
        self.parser_tool = parser_tool
        self.conf = conf
        self.extractors = {}
        self.all_topics = {}
        self.modules_config = {}
        # self.current_path = os.getcwd()
        self.basic_path = crawler_basic_path
        self.src_dir = os.path.join(self.basic_path, "i_entity_extractor/extractors/template/")

        self.load_topics()
        self.load_extractors()
        self.add_extractors()

    def reload(self, topic_id=-1):
        '''重新加载topic和解析器 指定topic_id则只加载该topic'''

        all_topics_keys = self.load_topics(topic_id)
        all_extractors_keys = self.load_extractors(topic_id)
        ret = self.add_extractors(topic_id)
        module_list = []
        for key, extractor_info in self.modules_config.items():
            for key, value in extractor_info.items():
                if key == "f_module_name":
                    module_list.append(value)

        ret_data = {
            "topic_keys": all_topics_keys,
            "extractor_keys": all_extractors_keys,
            "module_list": module_list,

        }
        return ret_data

    def add_topic(self, topic_info):
        ''''''
        resp = toolsutil.result()
        try:
            id = topic_info['id']
            table_name = topic_info['table_name']
            schema = topic_info['schema']
            primary_keys = topic_info['primary_keys']
        except:
            log.error("topic_info error,\ttopic_info:%s\tret:%s" % (topic_info, traceback.format_exc()))
            resp['MSG'] = "topic_info error,\ttopic_info:%s\tret:%s" % (topic_info, traceback.format_exc())
            resp['CODE'] = -10000
            return resp

        sql = '''INSERT INTO topic (id, name, table_name, `schema`, primary_keys) VALUES(%s,%s,%s,%s,%s)'''
        param = (id, topic_info.get('name', ''), table_name, schema, primary_keys)

        mysql_db = get_mysql_conn(self.conf)
        try:
            cursor = mysql_db.cursor()
            cursor.execute(sql, param)
        except:
            resp['MSG'] = "insert data into mysql error,\ttopic_info:%s\tsql:%s\tret:%s" % (
                topic_info, sql, traceback.format_exc())
            resp['CODE'] = -10000
            mysql_db.rollback()
            mysql_db.close()
            return resp

        mysql_db.commit()
        mysql_db.close()
        resp['MSG'] = "insert data into mysql success, topic_info:%s, sql:%s" % (topic_info, sql)
        return resp

    def load_topics(self, topic_id=-1):
        '''加载topic'''
        mysql_db = get_mysql_conn(self.conf)
        if topic_id != -1:
            sql = '''select id, name, table_name, `schema`, primary_keys from topic where id=%s'''
        else:
            sql = '''select id, name, table_name, `schema`, primary_keys from topic'''
        try:
            cursor = mysql_db.cursor()
            if topic_id != -1:
                param = [str(topic_id)]
                cursor.execute(sql, param)
            else:
                cursor.execute(sql)
            result = cursor.fetchall()
            mysql_db.close()
        except:
            log.error("query data from mysql error, sql:%s, ret:%s" % (sql, traceback.format_exc()))
            mysql_db.close()
            return None

        for item in result:
            topic_info = dbrecord_to_dict(cursor.description, item)
            topic = {}
            try:
                topic['schema'] = json.loads(topic_info['schema'])
            except:
                topic['schema'] = None
                log.warning('schema parse failed[%s], topic[%s] is not writable!' % (
                    traceback.format_exc(), topic_info['name'].encode('utf8')))

            topic_id = topic_info['id']
            topic_id = int(topic_id)
            topic['topic_id'] = topic_id
            topic['name'] = topic_info['name'].encode('utf8')
            topic['table_name'] = topic_info.get('table_name')
            topic['primary_keys'] = [[]]
            if topic_info.get('primary_keys'):
                topic['primary_keys'] = json.loads(topic_info.get('primary_keys'))
            self.all_topics[topic_id] = topic

        log.info("load_topic_success, topic_info_keys:%s" % json.dumps(self.all_topics.keys()))

        return self.all_topics.keys()

    # extractor_info eg: {'topic_id':111, 'target_dir_name':'ssgg', 'extractor_name':"上市公告"}
    def insert_extractor(self, extractor_info):
        '''往数据库中写入解析器'''

        resp = toolsutil.result()
        try:
            topic_id = extractor_info['topic_id']
            target_dir_name = extractor_info['target_dir_name']
            extractor_name = extractor_info.get('extractor_name', '')
        except:
            log.error("extractor_info error,\textractor_info:%s\tret:%s" % (extractor_info, traceback.format_exc()))
            resp['MSG'] = "extractor_info error,\textractor_info:%s\tret:%s" % (extractor_info, traceback.format_exc())
            resp['CODE'] = -10000
            return resp

        moudule_path = "extractors/" + target_dir_name + "/" + target_dir_name + "_extractor.py"

        if target_dir_name:
            moudule_name = target_dir_name[0].upper() + target_dir_name[1:] + "Extractor"
        else:
            moudule_name = "TemplateExtractor"

        # 1 创建解析器默认文件和默认代码
        target_dir = self.basic_path + "i_entity_extractor/extractors/" + target_dir_name
        ret = self.copy_files(self.src_dir, target_dir)
        if not ret:
            resp['MSG'] = "copy_files_error,\textractor_info:%s\tret:%s" % (extractor_info, traceback.format_exc())
            resp['CODE'] = -10000
            return resp

        target_file = target_dir + "/" + target_dir_name + "_extractor.py"
        source_file = target_dir + "/template_extractor.py"
        open(target_file, "wb").write(open(source_file, "rb").read().replace("TemplateExtractor", moudule_name))
        os.remove(source_file)

        # 2 在数据库中添加解析器配置
        sql = '''INSERT INTO extractors (f_topic_id, f_module_path, f_module_name, f_extractor_name) VALUES(%s,%s,%s,%s)'''
        mysql_db = get_mysql_conn(self.conf)
        try:
            cursor = mysql_db.cursor()
            cursor.execute(sql, (topic_id, moudule_path, moudule_name, extractor_name))
        except:
            resp['MSG'] = "insert data into mysql error,\textractor_info:%s\tsql:%s\tret:%s" % (
                extractor_info, sql, traceback.format_exc())
            resp['CODE'] = -10000
            mysql_db.rollback()
            mysql_db.close()
            return resp

        mysql_db.commit()
        mysql_db.close()
        resp['MSG'] = "insert data into mysql success, extractor_info:%s, sql:%s" % (extractor_info, sql)
        return resp

    def load_extractors(self, topic_id=-1):
        '''加载解析器'''
        mysql_db = get_mysql_conn(self.conf)
        if topic_id != -1:
            sql = '''select f_topic_id, f_module_path, f_module_name, f_extractor_name from extractors where f_topic_id=%s'''
        else:
            sql = '''select f_topic_id, f_module_path, f_module_name, f_extractor_name from extractors'''
        try:
            cursor = mysql_db.cursor()
            if topic_id != -1:
                param = [str(topic_id)]
                cursor.execute(sql, param)
            else:
                cursor.execute(sql)
            result = cursor.fetchall()
            mysql_db.close()
            for item in result:
                extractor_info = dbrecord_to_dict(cursor.description, item)
                topic_id = int(extractor_info.get("f_topic_id", -1))
                self.modules_config[topic_id] = extractor_info
        except:
            log.error("query data from mysql error, sql:%s, ret:%s" % (sql, traceback.format_exc()))
            mysql_db.close()
            return None

        log.info("load_modules_success, moudle:%s" % json.dumps(self.modules_config))

        return self.modules_config.keys()

    def add_extractors(self, topic_id=-1):
        if topic_id != -1:
            extractor_info = self.modules_config.get(int(topic_id))
            if not extractor_info:
                topic = self.all_topics.get(topic_id, None)
                concreteExtractor = DefaultExtractor(topic, log)
                self.extractors[topic_id] = concreteExtractor
                return True
            ret = self.load_module(extractor_info)
            return ret
        else:
            for key, extractor_info in self.modules_config.items():
                try:
                    ret = self.load_module(extractor_info)
                except:
                    log.info("load_extractors_error, topic_id:%s\tret:%s" % (topic_id, traceback.format_exc()))

        log.info("load_extractors_success, extractors_info_keys:%s" % json.dumps(self.extractors.keys()))

        return True

    def load_module(self, extractor_info):
        if not extractor_info:
            return True
        topic_id = int(extractor_info.get('f_topic_id', -1))
        module_path = extractor_info.get('f_module_path', '')
        module_name = extractor_info.get('f_module_name', '')
        log.debug("Initialize extractor %d %s %s" % (
            topic_id, module_path, module_name))
        topic = self.all_topics.get(topic_id, None)
        if not topic:
            return False
        module_dir = module_path[0:module_path.find('/')]
        module_path = module_path.replace("/", '.').replace(".py", '')
        load_module = __import__(module_path, fromlist=[module_dir])
        ConcreteExtractor = getattr(load_module, module_name)
        concreteExtractor = ConcreteExtractor(topic, log)
        self.extractors[topic_id] = concreteExtractor
        log.debug("Initialize extractor %d %s %s success type %s" % (
        topic_id, module_path, module_name, str(type(concreteExtractor))))


        return True

    def get_extractor(self, topic_id=-1):

        topic = self.all_topics.get(topic_id, None)
        if not topic:
            return None
        extractor = None
        if self.extractors.has_key(topic_id):
            extractor = self.extractors[topic_id]
        else:
            extractor = DefaultExtractor(topic, log)
            self.extractors[topic_id] = extractor
        return extractor

    def copy_files(self, source_dir, target_dir):
        '''复制一个目录中的所有文件到指定目录'''
        source_dir = source_dir.strip().rstrip('/')
        target_dir = target_dir.strip().rstrip('/')
        if source_dir.find(".svn") > 0:
            return False
        for file in os.listdir(source_dir):
            try:
                source_file = os.path.join(source_dir, file)
                target_file = os.path.join(target_dir, file)
                if os.path.isfile(source_file):
                    if not os.path.exists(target_dir):
                        os.makedirs(target_dir)
                    if not os.path.exists(target_file) or (
                                os.path.exists(target_file) and (
                                        os.path.getsize(target_file) != os.path.getsize(source_file))):
                        open(target_file, "wb").write(open(source_file, "rb").read())

                if os.path.isdir(source_file):
                    First_Directory = False
                    self.copy_files(source_file, target_file)
            except:
                return False
        return True




if __name__ == "__main__":
    entity_route_obj = EntityExtractorRoute()
    extractor_info = {"topic_id": 137, "module_path": "fdfs", "moudule_name": "sdfsfd", "extractor_name": "dfsdfd"}
    primary_keys = json.dumps({})
    schema = json.dumps({})
    topic_info = {"id": 135, "name": "测试动态加载解析器", "schema": schema, "primary_keys": primary_keys}
    resp = entity_route_obj.read_topics()
    # entity_route_obj.add_topic(topic_info)

    resp = entity_route_obj.reload()
    for key, value in resp.items():
        print key, value

    for ret in resp[2]:
        print ret


