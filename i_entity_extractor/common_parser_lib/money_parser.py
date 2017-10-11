# coding=utf-8
__author__ = 'luojianbo'

import re, traceback
import toolsutil


class MoneyParser:
    def __init__(self):
        self.money_regex = re.compile(u'\d+\.\d+|\d+')
        self.money_map   = {u"亿": 100000000,u"万": 10000, u"M":1000000, u"千": 1000, u'百': 100,u'十': 10}
        self.money_type_list = [u'美元', u'欧元', u'港元', u'港币',u'英镑',u'澳元',u'日元',u'元']
        self.chs_money_map = {u'一': '1', u'二': '2', u'三': '3', u'四': '4', u'五': '5', u'六': '6', u'七': '7',u'八': '8', u'九': '9', u'十': '10',
                              u'零': '0', u'壹':'1',u'贰':'2',u'叁':'3',u'肆':'4',u'伍':'5',u'陆':'6',u'柒':'7',u'捌':'8',u'玖':'9',u'拾':'10',
                              u'万':u'万sep',u'千':u'千sep',u'百':u'百sep',
                              }

        self.money_type_kv = {u'美元': u'美元', u"USD": u"美元",
                           u"元": u"人民币", u"RMB": u"人民币", u"人民币": u"人民币",
                           u'欧元': u'欧元', u"EUR": u"欧元",
                           u'镑': u'英镑', u"GBP": u"英镑",
                           u'日元': u'日元', u"JPY": u"日元",
                           u'韩币': u'韩币', u"KRW": u"韩币",
                           u'港币': u'港币', u'HKD': u'港币'}


    def trans_money(self, src_money):
        '''转换3000万人民币成30000000'''
        money = ''
        digit = 0

        try:
            src_money = unicode(src_money).replace(',', '')
            ret = toolsutil.re_findone(self.money_regex, src_money)
            if ret:
                try:
                    digit = float(ret)
                except:
                    digit = 0
            else:
                return src_money
        except:
            return src_money

        flag = False
        for key, value in self.money_map.items():
            if key in unicode(src_money):
                money = digit * value
                flag = True
                break

        if not flag:
            money = digit

        result = ''
        found = False
        for money_type in self.money_type_list:
            if money_type in unicode(src_money):
                result = ("%.4f" % money) + "(" + money_type + ")"
                found = True
                break
        if not found:
            result = ("%.4f" % money) + "(元)"
        return result

    def transfer_money(self, src_money):
        '''转换3000万人民币成30000000'''
        money = ''
        money_unit = ''

        try:
            src_money = unicode(src_money).replace(',', '')
            ret = toolsutil.re_find_one(self.money_regex, src_money)
            if ret:
                try:
                    digit = float(ret)
                except:
                    digit = 0
            else:
                return (src_money, money_unit)
        except:
            return (src_money, money_unit)

        flag = False
        for key, value in self.money_map.items():
            if key in unicode(src_money):
                money = digit * value
                flag = True
                break

        if not flag:
            money = digit

        found = False
        for money_type in self.money_type_list:
            if money_type in unicode(src_money):
                money_unit = money_type
                found = True
                break
        if not found:
            money_unit = u'元'

        money = str(money)
        return (money, money_unit)

    def trans_chs_money(self,chs_money):
        '''转换中文金额'''
        src_money = chs_money
        if not chs_money:
            return chs_money
        chs_money = unicode(chs_money)
        for key,value in self.chs_money_map.items():
            chs_money = chs_money.replace(key,value)
        chs_money_list = chs_money.split('sep')

        summoney = 0
        for chs in chs_money_list:
            ret = self.transfer_money(chs)[0]
            if toolsutil.re_findone(self.money_regex,ret):
                summoney = summoney + float(ret)

        money      = 0
        money_unit = ''
        for money_type in self.money_type_list:
            if money_type in unicode(src_money):
                money = "%.4f" % summoney
                money_unit = money_type
                found = True
                break
        return (money, money_unit)

    def new_trans_money(self, src_money, out_money_unit=u'', skip_none=True):
        '''
        :param src_money:
        :param out_money_unit:
        :return: 金额,单位,货币
        '''
        digit = 0
        if src_money == None or src_money == '':
            src_money = digit
        ccy = u''
        try:
            src_money = unicode(src_money).replace(',', '')
            ret = toolsutil.re_findone(self.money_regex, src_money)
            if ret:
                try:
                    digit = float(ret)
                except:
                    pass
            if ret == None and not skip_none:
                return (src_money, out_money_unit, ccy)
        except:
            return (src_money, out_money_unit, ccy)

        money = digit
        for key, value in self.money_map.items():
            if key in unicode(src_money):
                money = digit * value
                break

        ccy = u'人民币'
        for money_type_k, money_type_v in self.money_type_kv.items():
            if money_type_k in unicode(src_money):
                ccy = money_type_v
                break

        money = str(money if out_money_unit == '' else money / self.money_map.get(out_money_unit))
        return money, out_money_unit, ccy




if __name__ == "__main__":
    content = "十万零十澳元元"
    obj = MoneyParser()
    ret = obj.trans_chs_money(content)
    print ret[0],ret[1]

