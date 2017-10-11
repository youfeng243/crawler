# coding=utf8
# --author:zhangsl

import sys
sys.path.append('../')
import pandas

class ExcelParser:
    def __init__(self):
        self.rate = 0.4

    def find_table_head(self, df, keyword):
        '''通过关键词寻找表头'''
        head_row_index = 0
        keyword_flag = False
        none_flag = False
        head_keys = []
        for index in range(len(df.values)):
            src_head_keys = df.values[index]
            for item in src_head_keys:
                if keyword in unicode(item):
                    keyword_flag = True
                if 'none' == item:
                    none_flag = True
            if keyword_flag and none_flag:
                head_row_index = index + 1
                head_keys = df.values[head_row_index]
                for i in range(len(head_keys)):
                    if head_keys[i] == 'none':
                        head_keys[i] = src_head_keys[i]
                return head_row_index, head_keys
            if keyword_flag and not none_flag:
                head_row_index = index
                return head_row_index, src_head_keys

        df_value = df.values.tolist()
        if df_value:
            head_keys = df_value[head_row_index]
        return head_row_index, head_keys

    def read_data(self, df, keyword):
        '''读取数据'''
        df = df.fillna('none')
        head_row_index, head_keys = self.find_table_head(df, keyword)

        result_list = []

        for index in range(len(df.values)):
            if index <= head_row_index:
                continue
            row_map = {}
            none_num = 0
            for col in range(len(df.values[index])):
                head_key = head_keys[col]
                row_map[head_key] = df.values[index][col]
                if df.values[index][col] == 'none':
                    none_num += 1

            if float(len(head_keys) - none_num) / len(head_keys) >= self.rate:
                result_list.append(row_map)

        return result_list

    def read_html_table(self, html_table, istransport):
        '''解析网页表格'''

        if html_table == "":
            return

        if html_table.find('thead') != -1:
            thead_first_found = True
            tbody_first_found = True
            html_list = []
            for line in html_table.split('\n'):
                if line.find('thead') != -1 and thead_first_found:
                    line = line.replace('thead', 'tbody')
                    html_list.append(line)
                    thead_first_found = False
                    continue
                if line.find('thead') != -1 and not thead_first_found:
                    continue
                if line.find('tbody') != -1 and tbody_first_found:
                    tbody_first_found = False
                    continue
                if line.find('tbody') != -1 and not tbody_first_found:
                    html_list.append(line)
                    continue
                html_list.append(line)

            html_table = ''.join(html_list)
            dfs = pandas.read_html(html_table, encoding='utf-8')
        else:
            dfs = pandas.read_html(html_table, encoding='utf-8')

        if len(dfs) >= 1:
            df = dfs[0]
            if istransport:
                df = dfs[0].T
            result_list = self.read_data(df)
            return result_list

        return None

    def read_excel(self, filename, keyword=u"纳税人识别号"):
        '''读取excel'''
        df_dict = pandas.read_excel(filename, sheetname=None)
        result_list = []
        for key, value in df_dict.items():
            result_list += self.read_data(value, keyword)

        return result_list
