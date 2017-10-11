import copy
import re

from lxml import etree


class BaseExtractor(object):

    RULE_TYPE_ONE = 'one'
    RULE_TYPE_ALL = 'all'
    RULE_TYPE_ARRAY = 'array'

    def __init__(self):
        pass

    def __del__(self):
        pass

    def _deal_rule(self, rule):
        rule_text = None
        rule_type = self.RULE_TYPE_ONE
        rule_part = ''
        if isinstance(rule, basestring):
            rule_text = rule
        elif isinstance(rule, list):
            if len(rule) > 0:
                rule_text = rule[0]
            if len(rule) > 1:
                rule_type = rule[1]
            if len(rule) > 2:
                rule_part = rule[2]
        return rule_text, rule_type, rule_part

    def _deal_items(self, items, rule_type=RULE_TYPE_ONE, rule_part=''):
        if items is None:
            return None
        elif rule_type == self.RULE_TYPE_ONE:
            if len(items) == 0:
                return None
            else:
                if isinstance(items, list):
                    for i in range(len(items)):
                        if len(items[i]) > 0:
                            return items[i]
                    return items[0]
                elif isinstance(items, etree._ElementUnicodeResult) or isinstance(items, etree._ElementStringResult):
                    return items
        elif rule_type == self.RULE_TYPE_ALL:
            if len(items) == 0:
                return None
            else:
                return rule_part.join(items)
        elif rule_type == self.RULE_TYPE_ARRAY:
            return items

class XpathExtractor(BaseExtractor):

    def __init__(self, element):
        if not etree.iselement(element):
            raise Exception('input is not a element!')
        self.element = element
        pass

    def __del__(self):
        pass

    def _extract(self, rule_text):
        if self.element is not None and rule_text is not None:
            if isinstance(rule_text, basestring):
                return self.element.xpath(rule_text.decode('utf8'))
            elif isinstance(rule_text, list):
                items = []
                for rule_node in rule_text:
                    t_items = self.element.xpath(rule_node)
                    items.extend(t_items)
                return items
        return None

    def reload(self, element):
        self.__init__(element)

    def extract(self, rule):
        if not rule:
            return None
        rule_text, rule_type, rule_part = self._deal_rule(rule)
        items = self._extract(rule_text)

        return self._deal_items(items, rule_type, rule_part)

    def extract_nodes(self, nodes_rule):
        if isinstance(nodes_rule, basestring):
            return self._extract(nodes_rule)
        elif isinstance(nodes_rule, list):
            return self._extract(nodes_rule[0])

class ReExtractor(BaseExtractor):

    def __init__(self, text):
        if not isinstance(text, basestring):
            raise Exception('input is not a basestring!')
        self.text = text
        pass

    def __del__(self):
        pass

    def _extract(self, rule_text):
        if self.text is not None and isinstance(rule_text, basestring):
            return re.findall(rule_text.decode('utf8'), self.text)
        else:
            return None

    def reload(self, text):
        self.__init__(text)
        pass

    def extract(self, rule):
        if not rule:
            return None
        rule_text, rule_type, rule_part = self._deal_rule(rule)
        items = self._extract(rule_text)
        return self._deal_items(items, rule_type, rule_part)

    def extract_nodes(self, nodes_rule):
        return self._extract(nodes_rule)

class JsonExtractor(BaseExtractor):

    def __init__(self, json_obj):
        if not isinstance(json_obj, dict) and not isinstance(json_obj, list):
            raise Exception('input is not a JSONDecoder!')
        self.json_obj = json_obj
        pass

    def __del__(self):
        pass

    def _get_val(self, rule_text):
        if self.json_obj is not None and isinstance(rule_text, basestring):
            if rule_text == '':
                return self.json_obj
            if rule_text.startswith('array|'):
                rule_text = rule_text.replace('array|', '')
                a1 = []
                a2 = []
                paths = rule_text.split('.')
                o = self.json_obj
                a1.append(o)
                for path in paths:
                    for o in a1:
                        if not isinstance(o, dict):
                            return None
                        t = o.get(path)
                        if t is None:
                            return None
                        if isinstance(t, list):
                            a2.extend(t)
                        else:
                            a2.append(t)
                    a1 = copy.copy(a2)
                    a2 = []
                return a1
            else:
                paths = rule_text.split('.')
                o = self.json_obj
                for path in paths:
                    if not isinstance(o, dict):
                        return None
                    t = o.get(path)
                    if t is None:
                        return None
                    o = t
                return o
        else:
            return None

    def _extract(self, rule_text):
        if isinstance(rule_text, basestring):
            return self._get_val(rule_text)
        elif isinstance(rule_text, list):
            items = []
            for rule_node in rule_text:
                item = self._get_val(rule_node)
                if isinstance(item, list):
                    items.extend(item)
                else:
                    items.append(item)
            return items
        return None

    def reload(self, json_obj):
        self.__init__(json_obj)
        pass

    def extract(self, rule):
        rule_text, rule_type, rule_part = self._deal_rule(rule)
        items = self._extract(rule_text)
        if isinstance(items, list):
            return self._deal_items(items, rule_type, rule_part)
        else:
            return items

    def extract_nodes(self, nodes_rule):
        return self._extract(nodes_rule)