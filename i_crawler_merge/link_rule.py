# coding:utf-8

import sys
import json

sys.path.append('..')

import link_recall


class LinkRule(object):
    def __init__(self):
        self.dic_file = link_recall.dic_file

    def get_rule_list(self):
        link_recall.load_rule()
        return link_recall.rule_list

    def append_rule(self, json_data):
        with open(self.dic_file, 'a') as f:
            f.write(json.dumps(json_data, sort_keys=True) + '\n')
        link_recall.reload_rule()

    def change_rule(self, json_data_list):
        with open(self.dic_file, 'w') as f:
            for rule in json_data_list:
                f.write(json.dumps(rule, sort_keys=True) + '\n')
        link_recall.reload_rule()

    def clear_rule(self):
        with open(self.dic_file, 'w') as f:
            f.truncate()
        link_recall.reload_rule()
