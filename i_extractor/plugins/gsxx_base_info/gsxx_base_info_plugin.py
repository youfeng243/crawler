# -*- coding:utf-8 -*-
import copy

path_list = __file__.split("/")[:-1]
path_list.append("mapping.conf")
mapping_conf_path = "/".join(path_list)
conf_dict = dict()

with open(mapping_conf_path) as fp:
    for line in fp.xreadlines():
        pars = line.strip().split(',')
        if len(pars) >= 2:
            key = pars[0].decode("utf8")
            value = pars[1].decode("utf8")
            conf_dict[key] = value

def extract(url, content, results):
    tmp_results = copy.deepcopy(results)
    data = tmp_results['data']
    base_info = None
    if data.has_key("base_info"):
        base_info = data['base_info']
        data.pop("base_info")
    if base_info:
        for item in base_info:
            key = item.get("key")
            value = item.get("value")
            if conf_dict.get(key) and not data.get(conf_dict[key]):
                data[conf_dict[key]] = value
    return tmp_results
