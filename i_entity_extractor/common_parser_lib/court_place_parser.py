#coding=utf8

import esm
from i_entity_extractor.common_parser_lib import toolsutil

class CourtPlaceParser:
    def __init__(self,court_place_conf_path):
        court_place_list = open(court_place_conf_path).read().split('\n')
        self.court_place_index = esm.Index()
        for court_place in court_place_list:
            court_place = toolsutil.utf8_encode(court_place).strip()
            if not court_place:
                continue
            self.court_place_index.enter(court_place)
        self.court_place_index.fix()

    def get_court_place_list(self,content):
        '''获取法院列表'''
        court_place_map  = {}
        content          = toolsutil.utf8_encode(content)
        court_place_ret  = self.court_place_index.query(content)
        if court_place_ret:
            for ret in court_place_ret:
                court_place_map[ret[1]] = len(ret[1])

        return toolsutil.dedup(court_place_map)


    def get_court_place(self,content):
        '''获取第一个出现的法院'''
        court_place_list = self.get_court_place_list(content)
        return court_place_list[0] if court_place_list != [] else ""


if __name__ == "__main__":
    obj = CourtPlaceParser("../dict/court_place.conf")
    content = "本院定于二〇一六年十二月二日10:30到12:00在简易审判庭505室开庭审理(2016)渝0117民初9569号原告杨祖奎诉被告王道云,南充云丰水泥有限公司民间借贷纠纷一案。"
    court_place = obj.get_court_place(content)
    print court_place




