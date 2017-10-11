# coding=utf-8
__author__ = 'zhangsl'

import re, traceback
import toolsutil


class DateParser:
    def __init__(self):
        self.date_pattern_list = [
            "\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}|\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}|\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}",
            "\d{4}-\d{1,2}-\d{1,2}",
            "\d{8}\s+\d{1,2}:\d{1,2}",
            "\d{8}\s+\d{1,2}",
            "\d{8}",

        ]
        self.date_regex_list = [re.compile(x) for x in self.date_pattern_list]

        self.replace_list = ['\t', '_', '。']
        self.replace_space_list = ['上午', '中午','下午', '日']

    def _norm_date(self, date_values):
        '''规划时间'''
        if date_values == "" or date_values == None or date_values == "null":
            return ""

        date_values = toolsutil.utf8_encode(date_values)

        for replace_str in self.replace_list:
            date_values = date_values.replace(replace_str, '')
        for replace_space in self.replace_space_list:
            date_values = date_values.replace(replace_space, ' ')

        date_values = date_values.strip().replace('．', '-').replace('/', '-').replace('.', '-')
        date_values = date_values.replace("年", "-").replace("月", "-").replace("点", ":").replace("时", ":").replace("分", ":").replace("：", ":")


        return date_values

    def get_date_list(self, content):
        date_values = []
        find_flag   = False
        content     = str(content)
        if unicode(content).find(u'下午') != -1:
            find_flag = True

        content = self._norm_date(content)

        for regex in self.date_regex_list:
            results = toolsutil.re_findall(regex, content)
            if results:
                date_values.append(list(set(results)))


        date_list = []
        for i in date_values:
            for j in i:
                ss = re.sub('\s+(?!$)',' ',j)
                if ss != "":
                    date_list.append(ss)
        if len(date_list) > 0:
            date_value = date_list[0]
            if find_flag:
                tmp_list = date_list[0].split()
                if len(tmp_list) == 2:
                    time_list  = tmp_list[1].split(':')
                    hour_value = time_list[0]
                    if int(hour_value) < 12:
                        hour_value = int(hour_value) + 12
                        time_list.pop(0)

                        hour_value = str(hour_value) + ':' + ':'.join(time_list)
                        date_value = tmp_list[0] + " " + hour_value

            return date_value
        else:
            return content



if __name__ == "__main__":
    obj = DateParser()
    date_value = u"公告日期：2016-07-14 3:33 "
    # date_value = "null"
    date_value = 1111
    obj.get_date_list(date_value)
    date = toolsutil.norm_date_time(obj.get_date_list(date_value))
    print date

    #print obj.process_date_result(date)
