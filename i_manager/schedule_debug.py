# coding=utf-8
import sys
import os
import traceback
import json
import time
import dateparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from  background.models_sqlalchemy import Settings, Topic
import pymongo
import urllib2
from urllib import urlencode
import pickle
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer
sys.path.append("../")
from i_util.tools import get_url_info, url_encode
from bdp.i_crawler.i_crawler_merge.ttypes import LinkAttr
from i_entity_extractor.common_parser_lib.province_parser import ProvinceParser
from i_util.tools import crawler_basic_path

class ScheduleDebug:
    def __init__(self, logger, mysql_conf, crawl_conf = {}):
        self.logger = logger
        self.mysql_conf = mysql_conf;
        self.crawl_conf = crawl_conf
        self.mongodb_webpage = {};
        self.mongodb_linkbase = {};
        self.table_name = '__STATISTICS__'
        self.schedule_map = {
            0:1,
            1:7,
            2:30
        }
        
	    #初始化linkbase
        self.mongodb_linkbase = self.mongo_config("linkbase");
        self.linkbase_conn = pymongo.MongoClient(
        	host=self.mongodb_linkbase['host'],
            port=int(self.mongodb_linkbase['port'])
        );
        self.linkbase_db = self.linkbase_conn[self.mongodb_linkbase['database']];
        username = self.mongodb_linkbase.get('username','')
        password = self.mongodb_linkbase.get('password','')
        if username and password:
            self.linkbase_db.authenticate(username, password)
        #初始化webpage
        self.mongodb_webpage = self.mongo_config("webpage");
        self.webpage_conn = pymongo.MongoClient(
                                            host=self.mongodb_webpage['host'],
                                            port=int(self.mongodb_webpage['port'])
                            );
        self.webpage_db = self.webpage_conn[self.mongodb_webpage['database']];
        username = self.mongodb_webpage.get('username','')
        password = self.mongodb_webpage.get('password','')
        if username and password:
            self.webpage_db.authenticate(username, password)

        #初始化final_data
        self.mongodb_final_data = self.mongo_config("mongodb");
        self.final_data_conn = pymongo.MongoClient(
        	host=self.mongodb_final_data['host'],
            port=int(self.mongodb_final_data['port'])
        );
        self.final_data_db = self.final_data_conn[self.mongodb_final_data['database']];
        username = self.mongodb_final_data.get('username','')
        password = self.mongodb_final_data.get('password','')
        if username and password:
            self.final_data_db.authenticate(username, password)
        self.enterprise_data = self.final_data_db['enterprise_data_gov']

        province_city_conf = crawler_basic_path + '/i_entity_extractor/dict/province_city.conf'
        phonenum_conf_path = crawler_basic_path + '/i_entity_extractor/dict/phonenum_city.conf'
        region_conf_path   = crawler_basic_path + '/i_entity_extractor/dict/region_city.conf'
        city_conf_path     = crawler_basic_path + '/i_entity_extractor/dict/city.conf'

        self.province_parser = ProvinceParser(province_city_conf, phonenum_conf_path, region_conf_path, city_conf_path)


        #始化schedule_data
        self.mongodb_schedule_data = self.mongo_config("schedule_data")
        self.schedule_data_conn = pymongo.MongoClient(
        	host=self.mongodb_schedule_data['host'],
            port=int(self.mongodb_schedule_data['port'])
        );
        self.schedule_data_db = self.webpage_conn[self.mongodb_schedule_data['database']]
        username = self.mongodb_schedule_data.get('username','')
        password = self.mongodb_schedule_data.get('password','')
        if username and password:
            self.schedule_data_db.authenticate(username, password)
        self.schedule_data = self.schedule_data_db['enterprise_data']       
        self.enterprise_data_pending = self.schedule_data_db['enterprise_data_pending']       
 
    def mongo_config(self, mongo_item = "mongodb"):
        try:
            mongodb_conf = {}
            engine = create_engine(self.mysql_conf)
            Settings.metadata.create_all(engine)
            self.Dsession = sessionmaker(bind=engine)
            session = self.Dsession()
            query = session.query(Settings)
            records = query.filter(Settings.item == mongo_item).all()
            for record in records:
                mongodb_conf['host'] = '127.0.0.1'
                mongodb_conf['port'] = record.value['port']
                mongodb_conf['database'] = record.value['database']
                mongodb_conf['username'] = record.value['user']
                mongodb_conf['password'] = record.value['password']
                break;
            session.close();
            return mongodb_conf;
        except Exception as e:
            self.logger.error(traceback.format_exc())
            os._exit(1) 
    def to_LinkAttr(self, body):
        link_info = LinkAttr()
        try:
            tMemory_o = TMemoryBuffer(body)  
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)  
            link_info.read(tBinaryProtocol_o)
        except:
            print traceback.format_exc()
    def find_linkbase(self, link_url = "http://www.baidu.com/"):
        url_struct = get_url_info(link_url);
        domain = url_struct.get("domain", "baidu.com");
        urls_info =  self.linkbase_db[domain].find({'url':link_url})
        for url_info in urls_info:
            json_dict = {};
            link_str =  url_info['link_attr']
            link_attr = pickle.loads(link_str)
            if not link_attr:
                return None
            if not link_attr.url:
                link_attr.url = link_url
            link_attr = vars(link_attr)
            for key,val in link_attr.items():
                if not val:
                    json_dict[key] = None
                elif key == "crawl_info" or key == "parent_info" or key == "page_info" or key == 'extract_message':
                    json_dict[key] = vars(val)
                elif key == "normal_crawl_his" and val:
                    json_dict[key] = [] 
                    for his in val:
                        json_dict[key].append(vars(his))
                else:
                    json_dict[key] = str(val)
            return json_dict
    def find_webpage(self, link_url = "http://www.baidu.com/"):
        url_struct = get_url_info(link_url);
        domain = url_struct.get("domain", "baidu.com");
        urls_info =  self.webpage_db[domain].find({'url':link_url})
        for url_info in urls_info:
            url_info['_id'] = str(url_info['_id'])
            return url_info;
    def schedule_company(self, company_name, level, province):
        #if not province:
        province = self.province_parser.get_province(company_name)
        company_stat = {"update":0, 'for_schedule':0, 'in_schedule':0, 'company':company_name, 'level':level, 'province':province}
        company_infos = self.data_db[self.enterprise_table].find({"company":company_name})
        for company_info in company_infos:
            company_info['_id'] = str(company_info['_id'])
            company_stat['update'] += 1
            company_stat['in_schedule'] += 1
            company_stat['level'] = 1
            company_stat['province'] = company_info['province']
            company_stat['type'] = 'need_update'
            return company_stat

        branch_infos = self.company_data['enterprise_list_branch_invest'].find({"_id":company_name})
        for branch in branch_infos:
            company_stat['in_schedule'] += 1
            company_stat['level'] = 0
            company_stat['crawl_status'] = branch['crawl_status']
            company_stat['type'] = 'need_invest'
            return company_stat

        branch_infos = self.company_data['enterprise_list_zonggongsi'].find({"_id":company_name})
        for branch in branch_infos:
            company_stat['in_schedule'] += 1
            company_stat['level'] = 2
            company_stat['crawl_status'] = branch['crawl_status']
            company_stat['type'] = 'need_zonggongsi'
            return company_stat

        branch_infos = self.company_data['enterprise_list_diff_11_02'].find({"_id":company_name})
        for branch in branch_infos:
            company_stat['in_schedule'] += 1
            company_stat['level'] = 3
            company_stat['crawl_status'] = branch['crawl_status']
            company_stat['type'] = 'new_crawl'
            return company_stat

        branch_infos = self.company_data['enterprise_list_all'].find({"_id":company_name})
        for branch in branch_infos:
            company_stat['in_schedule'] += 1
            company_stat['level'] = 3
            company_stat['type'] = 'new_crawl'
            return company_stat
        """
        update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        data = {"company":company_name, "level":level, 'province':province, "_utime" : update_time}
        company_infos = self.data_db[self.enterprise_schedule_table].find({"company":company_name})
        for company_info in company_infos:
            data['_in_time'] = company_info.get('_in_time', update_time)
        if not data.has_key('_in_time'):
            data['_in_time'] = update_time
            self.data_db[self.enterprise_schedule_table].insert(data)
            company_stat['level']  = 4
            company_stat['for_schedule'] += 1
        else:
            company_stat['in_schedule'] += 1
            self.data_db[self.enterprise_schedule_table].update({"company":company_name}, data)
        """
        company_stat['level']  = 4
        company_stat['type'] = 'not_find'
        return company_stat

    def import_schedule(self, company = "", province = "", level = 0,  user = "", need_crawl=True):
        update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        company_infos = self.enterprise_data.find({'company':company})
        data = {"company":company, 'in_base':0}
        if not province:
            province = self.province_parser.get_province(company) 
        is_exit = 0
        for company_info in company_infos:
            data['in_base'] = 1
            is_exit = 1
        schedule_data = {"_id":company, "users":[], "_utime":update_time, 'province':province, 'level':level, 'exit':is_exit}
        company_infos = self.schedule_data.find({"_id":company})
        is_in = False
        for company_info in company_infos:
            is_in = True
            schedule_data["users"] = company_info["users"]
            if not (user in company_info["users"]) and user:
                schedule_data["users"].append(user)
        if not is_exit and self.enterprise_data_pending.count({'_id':company}) <= 0:
            self.enterprise_data_pending.insert(schedule_data)
        if not is_in:
            if user:
                schedule_data["users"].append(user)
            self.schedule_data.insert(schedule_data)
        else:
            self.schedule_data.update({"_id":company}, {'$set':schedule_data})
        if need_crawl:
            data['need_crawl'] = True
            #result = self.realtime_crawl(company);
            #data['crawl_status'] = result
        return data

    def get_schedule_list(self, user = "", start = 0, limit = 10):
        start = int(start)
        limit = int(limit)
        data = {"user":user, 'size':0, "total":0, "start":start, "limit":limit, "result":[]}
        data["total"] = self.schedule_data.find({"users":user}).count()
        company_infos = self.schedule_data.find({"users":user}).skip(start).limit(limit)
        for company_info in company_infos:
            data["size"] += 1
            update_time = company_info.get("_utime", "")
            company_name = company_info['_id'];
            level = company_info.get('level', 2)
            data["result"].append({"_name":company_name, "_utime":update_time, 'level':level})
            
        return data


    def import_companies(self, companies = "", user = ""):
        stat_info = {"in_base":0, "total":0}
        infos = companies.split("\n")
        company_name = ""
        province = "";
        level = 2
        for company in infos:
            pars = company.strip().split('\t')
            company_name = pars[0]
            if len(pars) >=2 and len(pars[1]) > 0:
                province = pars[1]
            if len(pars) >=3 and len(pars[2]) > 0:
                level = int(pars[2])
            stat_info["total"] += 1
            data = self.import_schedule(company_name, province, level, user, False)
            stat_info["in_base"] += data['in_base']
        return stat_info

    def schedule_companies(self, companies_str):
        companies_info = {}
        companies_name = []
        pars = companies_str.split('\n')
        for par in pars:
            company_info = {}
            company_info['level'] = 4
            company_info['province'] = ''
            company_info['company'] = ''
            companies_pars = par.split('\t');
            level = 4;
            if len(companies_pars) >= 1:
                company_info['company'] = companies_pars[0].strip()
            if len(companies_pars) >= 2:
                company_info['level'] = int(companies_pars[1].strip())
            if len(companies_pars) >= 3:
                company_info['province'] = companies_pars[2].strip()
            if not company_info['province']:
                company_info['province'] = self.province_parser.get_province(company_info['company'])
            if company_info['company']:
                company = company_info['company'];
                if not companies_info.has_key(company):
                    companies_name.append(company)
                companies_info[company] = company_info
        company_stat = {"update":0, 'for_schedule':0, 'in_schedule':0, 'all':len(companies_info)};
        cursor = self.data_db[self.enterprise_table].find({"company":{'$in':companies_name}},{'company':1, '_in_time':1, '_utime':1})
        for company_info in cursor:
            company_stat['update'] += 1
            company = company_info['company'].encode('utf8')
            u_time = time.mktime(dateparser.parse(company_info['_utime']).timetuple())
            if companies_info.has_key(company) and time.time() - u_time < 86400 * 7:
                del companies_info[company]
        update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        for company, company_info in companies_info.items():
            cursor = self.data_db[self.enterprise_schedule_table].find({"company":company})
            for info in cursor:
                company_info['_in_time'] = info.get('_in_time', update_time)
                break;
            if company_info.has_key('_in_time'):
                company_stat['in_schedule'] += 1
                company_info['_utime'] = update_time
                self.data_db[self.enterprise_schedule_table].update({"company":company}, company_info)
            else:
                company_info['_in_time'] = update_time
                company_info['_in_time'] = update_time
                self.data_db[self.enterprise_schedule_table].insert(company_info)
                company_stat['for_schedule'] += 1

        return company_stat

    def get_company(self, company):
        data_db = self.data_conn['final_data'];
        company_infos = []
        if company:
            cursor = data_db[self.enterprise_table].find({"company":company})
        else:
            cursor = data_db[self.enterprise_table].find().limit(5)
        for company_info in cursor:
            info = {}
            info['company'] = company_info['company']
            info['registered_code'] = company_info['registered_code']
            info['registered_date'] = company_info['registered_date']
            company_infos.append(info)
        return company_infos
    
    def get_news(self, company):
        data_db = self.data_conn['marketing'];
        company_infos = []
        if company:
            cursor = data_db['marketing_news'].find({"company":company})
        else:
            cursor = data_db[self.enterprise_table].find().limit(5)
        for company_info in cursor:
            del company_info['_id']
            company_infos.append(company_info)
        return company_infos

    def realtime_crawl(self, company):
        url = self.crawl_conf.get('api', '')
        query = self.crawl_conf.get('query', '')
        if url and query and company:
            url = url + "?" + query + "=" + company
            try:
                data = urllib2.urlopen(url, timeout = 10).read()
                if data and data.find('crawling') >= 0:
                    return {"status":True, "msg":data}
                else:
                    return {"status":False, "msg":data}
            except Exception as e:
                return {"status":False, "msg":traceback.format_exc()}
        return {"status":False, "msg":"not crawl_conf or company empty"}

    def clear_schedule_data(self):
        #级别为0:天级别更新  1:7天级别更新， 2:30天内更新
        company_infos = self.schedule_data.find({}).sort([("level",pymongo.ASCENDING),("_utime",pymongo.ASCENDING)])
        for company_info in company_infos:
            company_name = company_info.get('_id', '') 
            update_time = company_info.get('_utime', '') 
            level = int(company_info.get('level', '2'))
            if level < 0:
                level = 2 
            days_num = 30
            if level in self.schedule_map:
                days_num = self.schedule_map[level]
            now_time = time.time()
            company_info = self.enterprise_data.find({'company':company_name})
            need_crawl = 0
            mod_value = {}
            if company_info.count() > 0:
                update_time = company_info[0].get('_utime', "1985-12-17 00:00:00")
                province = company_info[0].get('province', "")
                utime = time.mktime(time.strptime(update_time,'%Y-%m-%d %H:%M:%S'))
                if utime < now_time -  86400 * days_num:
                    need_crawl = 1 
                mod_value['need_crawl'] = need_crawl
                if province:
                    mod_value['province'] = province
                self.schedule_data.update({'_id':company_name},{"$set":mod_value})
                self.logger.info("update\tcompany:%s\texit:1\tlevel:%s" % (company_name, level))
            else:
                mod_value = {}
                mod_value['need_crawl'] = need_crawl
                self.schedule_data.update({'_id':company_name},{"$set":mod_value})
                self.logger.info("crawl\tcompany:%s\texit:0\tneed_crawl:%s" % (company_name, 1)) 
        return None

def main(conf):
    realtime_crawl = {}
    if hasattr(conf, "realtime_crawl"):
        realtime_crawl = conf.realtime_crawl
    schedule = ScheduleDebug(conf.log, conf.MYSQL, realtime_crawl)
    #print json.dumps(schedule.find_linkbase("https://www.baidu.com/s?wd=%C9%EE%DB%DA%CA%D0%D3%AF%C8%F1%CA%FD%C2%EB%BF%C6%BC%BC%D3%D0%CF%DE%D4%F0%C8%CE%B9%AB%CB%BE"))
    #print json.dumps(schedule.find_linkbase("http://www.jxtb.org.cn/jxzbtb/zbgg/20160930/112122.htm"))
    #print json.dumps(schedule.find_webpage("http://www.jxtb.org.cn/jxzbtb/zbgg/20160930/082751.htm"))
    #print json.dumps(schedule.schedule_company('山西华瀚文化传播有限公司',1, '内蒙古'));
    #print json.dumps(schedule.schedule_companies("七星瑞光科技（深圳）有限公司\t1\t广东\n上海万耀企龙展览有限公司\t1\t上海"));
    #print json.dumps(schedule.get_company('七星瑞光科技（深圳）有限公司'));
    #print json.dumps(schedule.get_news('中国泛海控股集团有限公司'));
    #print json.dumps(schedule.import_schedule(company_name,'', 1, "admin"));
    #print json.dumps(schedule.get_schedule_list("admin"));
    #print json.dumps(schedule.import_companies("额济纳旗鼎泰实业有限公司\n兰西育兰牧业有限公司", "admin"));
    schedule.clear_schedule_data()
    #print json.dumps(schedule.realtime_crawl('山西华瀚文化传播有限公司'))

if __name__ == '__main__':
    import conf
    main(conf);
