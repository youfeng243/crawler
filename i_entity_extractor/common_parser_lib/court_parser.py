#coding=utf8

import esm
from i_entity_extractor.common_parser_lib import toolsutil

class CourtParser:
    def __init__(self,court_conf_path):
        court_list = open(court_conf_path).read().split('\n')
        self.court_index = esm.Index()
        for court in court_list:
            court = toolsutil.utf8_encode(court).strip()
            if not court:
                continue
            self.court_index.enter(court)
        self.court_index.fix()

    def get_court_list(self,content):
        '''获取法院列表'''
        court_map = {}
        content    = toolsutil.utf8_encode(content)
        court_ret  = self.court_index.query(content)
        if court_ret:
            for ret in court_ret:
                court_map[ret[1]] = len(ret[1])

        return toolsutil.dedup(court_map)

    def get_court(self,content):
        '''获取第一个出现的法院'''
        court_list = self.get_court_list(content)
        return court_list[0] if court_list != [] else ""


if __name__ == "__main__":
    obj = CourtParser("../dict/court.conf")
    content = "新疆生产建设兵团伊宁垦区人民法院-第一法庭"
    court = obj.get_court(content)
    print court




