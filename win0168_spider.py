# !/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author   : wuda
# @Project  : win007_request
# @Software : PyCharm
# @File     : win0168_spider.py
# @Time     : 2019/1/25 20:10


# !/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author   : wuda
# @Project  : ershi
# @Software : PyCharm
# @File     : win007_spider.py
# @Time     : 2019/1/22 9:32

import re
import time
import json
import pymysql
import pymssql
import datetime
import requests
from copy import deepcopy
from lxml import etree
from useragent import randomUA, UA_TYPE_DESKTOP
from PyQt5.QtCore import pyqtSignal, QThread

MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_DB = 'win0168'
MYSQL_USER = 'root'
MYSQL_PWD = 'root'

CHAR_SET = 'utf8'
TABLE = 'win0168_data'

CREATE_TABLE_SQL = """CREATE TABLE IF NOT EXISTS win0168_data(
赛事类型 VARCHAR(100),
时间 datetime,
主队 VARCHAR(100),
客队 VARCHAR(100),
公司 VARCHAR(100),
全场比分 VARCHAR(20),
半场比分 VARCHAR(20),
初_主队 VARCHAR(50),
初_盘口 VARCHAR(50),
初_客队 VARCHAR(50),
初_总计_场 VARCHAR(50),
初_总计_主 VARCHAR(50),
初_总计_和 VARCHAR(50),
初_总计_客 VARCHAR(50),
初_五大联赛_场 VARCHAR(50),
初_五大联赛_主 VARCHAR(50),
初_五大联赛_和 VARCHAR(50),
初_五大联赛_客 VARCHAR(50),
初_以甲_场 VARCHAR(50),
初_以甲_主 VARCHAR(50),
初_以甲_和 VARCHAR(50),
初_以甲_客 VARCHAR(50),
初_场次_总 VARCHAR(50),
初_主胜_总 VARCHAR(50),
初_和局_总 VARCHAR(50),
初_客胜_总 VARCHAR(50),
初_历史_总 VARCHAR(50),
初_赔率_总 VARCHAR(10),
初_场次_五 VARCHAR(50),
初_主胜_五 VARCHAR(50),
初_和局_五 VARCHAR(50),
初_客胜_五 VARCHAR(50),
初_历史_五 VARCHAR(50),
初_赔率_五 VARCHAR(10),
初_场次_比 VARCHAR(50),
初_主胜_比 VARCHAR(50),
初_和局_比 VARCHAR(50),
初_客胜_比 VARCHAR(50),
初_历史_比 VARCHAR(50),
初_赔率_比 VARCHAR(10),
即_主队 VARCHAR(50),
即_盘口 VARCHAR(50),
即_客队 VARCHAR(50),
即_总计_场 VARCHAR(50),
即_总计_主 VARCHAR(50),
即_总计_和 VARCHAR(50),
即_总计_客 VARCHAR(50),
即_五大联赛_场 VARCHAR(50),
即_五大联赛_主 VARCHAR(50),
即_五大联赛_和 VARCHAR(50),
即_五大联赛_客 VARCHAR(50),
即_以甲_场 VARCHAR(50),
即_以甲_主 VARCHAR(50),
即_以甲_和 VARCHAR(50),
即_以甲_客 VARCHAR(50),
即_场次_总 VARCHAR(50),
即_主胜_总 VARCHAR(50),
即_和局_总 VARCHAR(50),
即_客胜_总 VARCHAR(50),
即_历史_总 VARCHAR(50),
即_赔率_总 VARCHAR(10),
即_场次_五 VARCHAR(50),
即_主胜_五 VARCHAR(50),
即_和局_五 VARCHAR(50),
即_客胜_五 VARCHAR(50),
即_历史_五 VARCHAR(50),
即_赔率_五 VARCHAR(10),
即_场次_比 VARCHAR(50),
即_主胜_比 VARCHAR(50),
即_和局_比 VARCHAR(50),
即_客胜_比 VARCHAR(50),
即_历史_比 VARCHAR(50),
即_赔率_比 VARCHAR(10),
finger_print  VARCHAR(100),
PRIMARY KEY (finger_print)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;"""


import logging
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
# handler = RotatingFileHandler("log.txt", maxBytes=100 * 1024, backupCount=5)
handler = logging.FileHandler('win0168.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

import hashlib
HASH_SHA1 = hashlib.md5()


class Spider(QThread):

    logsignal = pyqtSignal(str)
    endsignal = pyqtSignal(str)

    def __init__(self, parent=None):
        super(Spider, self).__init__(parent)

        self.session = requests.session()
        self.session.headers['User_Agent'] = randomUA(UA_TYPE_DESKTOP)
        self.code = 0

    def start_requests(self):
        start_url = 'http://op1.win007.com/bet007history.aspx'
        self.session.headers.update({
            'Host': 'op1.win007.com',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        })
        res = self.session.get(url=start_url)
        # print(res)
        html = etree.HTML(res.content.decode('utf-8', 'ignore'))
        self.__VIEWSTATE = html.xpath('//input[@id="__VIEWSTATE"]/@value')[0]
        # print(self.__VIEWSTATE)
        self.__VIEWSTATEGENERATOR = html.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value')[0]
        # print(self.__VIEWSTATEGENERATOR)
        self.__EVENTVALIDATION = html.xpath('//input[@id="__EVENTVALIDATION"]/@value')[0]
        # print(self.__EVENTVALIDATION)
        self.logsignal.emit("程序正在准备")

    def run(self):
        if self.update_code == 1:
            self.spider_new()
            self.spider_old()
        else:
            self.spider_old()

    def spider_new(self):
        # print(self.date_list)
        # self.start_requests()
        url_temp = "http://op1.win007.com/nextodds/cn/{0}.html"
        self.session.headers.update({
            'Host': 'op1.win007.com',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        })
        search_date_list = self.date_list[0].split('-')
        search_date_list[1] = search_date_list[1] if len(search_date_list[1]) == 2 else '0' + search_date_list[1]
        search_date_list[2] = search_date_list[2] if len(search_date_list[2]) == 2 else '0' + search_date_list[2]
        search_date = ''.join(search_date_list)
        # print(search_date)
        res = self.session.get(url=url_temp.format(search_date))
        # print(res.text)
        html = etree.HTML(res.content.decode('gb2312', 'ignore'))
        table = html.xpath('//table[@id="table_schedule"]//tr')
        for i in table:
            # print(i)
            if table.index(i) == 0:
                continue
            elif table.index(i) % 2 != 0:
                continue
            else:
                # print('-------' + str(table.index(i)))
                # print(etree.tostring(i))

                # 类型
                match_type = i.xpath('./td[2]//text()')[0]

                # 时间
                match_time = ' '.join(i.xpath('./td[3]//text()'))

                # 主队
                home_team = re.sub('\s*', '', str(i.xpath('./td[4]/a/text()')[0]))

                # 客队
                visit_team = re.sub('\s*', '', str(i.xpath('./td[12]/a/text()')[0]))

                # 全场比分
                all_score = '0-0'

                # 半场比分
                half_score = '0-0'


                # print(match_time)
                match_time = datetime.datetime.strptime(match_time, '%y-%m-%d %H:%M')
                # print(match_time)
                # print(meta)
                next_url_temp = i.xpath('./td[13]/a/@href')[0]
                # print(next_url_temp)

                sid = re.findall('\d+', str(next_url_temp))[0]

                url_send = 'http://1x2d.win007.com/{0}.js'.format(sid)
                self.session.headers.update({
                    'Host': '1x2d.win007.com',
                    'Connection': 'keep-alive',
                    'Pragma': 'no-cache',
                    'Cache-Control': 'no-cache',
                    'Upgrade-Insecure-Requests': '1',
                    # 'User-Agent': randomUA(UA_TYPE_DESKTOP),
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                })
                res_send = self.session.get(url=url_send)
                data_res = re.findall(r'"(.*?)"', re.findall(r'game=Array\((.*)\);', res_send.text)[0])
                for i in data_res:
                    company = i.split('|')[21]
                    # print(company)
                    meta = {
                        '赛事类型': match_type,
                        '时间': match_time,
                        '主队': home_team,
                        '客队': visit_team,
                        '公司': company,
                        '全场比分': all_score,
                        '半场比分': half_score

                    }
                    hash_dict = deepcopy(meta)
                    hash_dict.pop('全场比分')
                    hash_dict.pop('半场比分')
                    HASH_SHA1.update(''.join([str(i) for i in list(hash_dict.values())]).encode())
                    finger_print = HASH_SHA1.hexdigest()
                    meta['finger_print'] = finger_print
                    print(meta)
                    cid = i.split('|')[0]
                    # print(sid, cid)
                    self.session.headers.update({
                        'Host': 'vip.win007.com',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'zh-CN,zh;q=0.9',
                    })
                    url_next_start_z = 'http://vip.win007.com/count/goalCount.aspx?t=5&sid={sid}&cid={cid}&l=0'
                    url_next_immed_z = 'http://vip.win007.com/count/goalCount.aspx?t=6&sid={sid}&cid={cid}&l=0'
                    url_next_start_w = 'http://vip.win007.com/count/goalCount.aspx?t=5&sid={sid}&cid={cid}&t2=1&r=2&l=0'
                    url_next_immed_w = 'http://vip.win007.com/count/goalCount.aspx?t=6&sid={sid}&cid={cid}&t2=1&r=2&l=0'
                    url_next_start_y = 'http://vip.win007.com/count/goalCount.aspx?t=5&sid={sid}&cid={cid}&t2=1&r=3&l=0'
                    url_next_immed_y = 'http://vip.win007.com/count/goalCount.aspx?t=6&sid={sid}&cid={cid}&t2=1&r=3&l=0'
                    meta = self.end_page(url=url_next_start_z.format(sid=sid, cid=cid), code=5, meta=meta)
                    meta = self.end_page(url=url_next_immed_z.format(sid=sid, cid=cid), code=6, meta=meta)
                    meta = self.end_page_each(url=url_next_start_w.format(sid=sid, cid=cid), code=5, meta=meta, index=1)
                    meta = self.end_page_each(url=url_next_immed_w.format(sid=sid, cid=cid), code=6, meta=meta, index=1)
                    meta = self.end_page_each(url=url_next_start_y.format(sid=sid, cid=cid), code=5, meta=meta, index=2)
                    meta = self.end_page_each(url=url_next_immed_y.format(sid=sid, cid=cid), code=6, meta=meta, index=2)
                    # print(meta)
                    key_list = ','.join(list(meta.keys()))
                    value_list = ','.join(['''"{0}"'''.format(i) for i in meta.values()])
                    sql_temp = """INSERT INTO win0168_data ({0}) VALUES ({1}); """.format(key_list, value_list)
                    print(sql_temp)
                    self.logsignal.emit('正在存储最新数据' + value_list)
                    # insert into mysql database
                    try:
                        self.mysql_conn(sql=sql_temp)
                    except Exception as e:
                        # print(e)
                        update_sql_temp = """UPDATE win0168_data SET {sql_in} WHERE finger_print='{finger_print}';"""
                        sql_in = ', '.join(['''{k}="{v}"'''.format(k=k, v=v) for k, v in meta.items() if not k=='finger_print'])
                        sql = update_sql_temp.format(sql_in=sql_in, finger_print=meta['finger_print'])
                        print(sql)
                        # self.mysql_conn(sql=sql)
                        # parimary key error
                        self.logsignal.emit('正在更新数据' + value_list)

        self.endsignal.emit('数据采集完成！')

    def spider_old(self):
        self.start_requests()
        url_temp = "http://op1.win007.com/bet007history.aspx"
        # url_temp = 'http://op1.win007.com/overodds/cn/{time}.html'
        # for i in self.date_list:
        for i in self.date_list:
            self.session.headers.update({
                'Host': 'op1.win007.com',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Origin': 'http://op1.win007.com',
                'Upgrade-Insecure-Requests': '1',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Referer': 'http://op1.win007.com/bet007history.aspx',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9',
            })
            data = {
                '__VIEWSTATE': self.__VIEWSTATE,
                '__VIEWSTATEGENERATOR': self.__VIEWSTATEGENERATOR,
                '__EVENTVALIDATION': self.__EVENTVALIDATION,
                'checkboxleague_54': '54',
                'checkboxleague_81': '81',
                'checkboxleague_84': '84',
                'checkboxleague_59': '59',
                'checkboxleague_108': '108',
                'checkboxleague_178': '178',
                'checkboxleague_1183': '1183',
                'checkboxleague_273': '273',
                'checkboxleague_41': '41',
                'checkboxleague_1817': '1817',
                'checkboxleague_947': '947',
                'checkboxleague_1366': '1366',
                'checkboxleague_142': '142',
                'checkboxleague_163': '163',
                'checkboxleague_167': '167',
                'checkboxleague_1849': '1849',
                'checkboxleague_303': '303',
                'checkboxleague_321': '321',
                'checkboxleague_1132': '1132',
                'checkboxleague_1873': '1873',
                'checkboxleague_1683': '1683',
                'checkboxleague_1620': '1620',
                'checkboxleague_213': '213',
                'checkboxleague_511': '511',
                'checkboxleague_971': '971',
                'checkboxleague_960': '960',
                'checkboxleague_354': '354',
                'checkboxleague_961': '961',
                'checkboxleague_962': '962',
                'checkboxleague_957': '957',
                'checkboxleague_963': '963',
                'checkboxleague_987': '987',
                'checkboxleague_1858': '1858',
                'checkboxleague_964': '964',
                'checkboxleague_181': '181',
                'checkboxleague_973': '973',
                'checkboxleague_722': '722',
                'checkboxleague_729': '729',
                'checkboxleague_1379': '1379',
                'checkboxleague_1709': '1709',
                'checkboxleague_1240': '1240',
                'checkboxleague_763': '763',
                'checkboxleague_1743': '1743',
                'checkboxleague_1364': '1364',
                'checkboxleague_1527': '1527',
                'checkboxleague_932': '932',
                'checkboxleague_1685': '1685',
                'checkboxleague_783': '783',
                'checkboxleague_1456': '1456',
                'checkboxleague_1834': '1834',
                'checkboxleague_978': '978',
                'checkboxleague_199': '199',
                'checkboxleague_1531': '1531',
                'checkboxleague_1378': '1378',
                'matchdate': i,
                'key': '',
                'sclassKey': '',
                'drp_selectOdds': '1',
                'drp_select': '1',
                'min': '',
                'max': '',
            }
            # time.sleep(2)
            self.session.headers['User_Agent'] = randomUA(UA_TYPE_DESKTOP)
            res = self.session.post(url=url_temp, data=data)
            # print(res)
            # print(res.content)
            try:
                html = etree.HTML(res.content.decode('gb2312', 'ignore'))
            except Exception as e:
                logger.error('解析出错  ' + url_temp + "  " + str(e))
                continue

            # with open('1.html', 'w') as f:
            #     f.write(res.content.decode('gb2312', 'ignore'))
            # print(res.content.decode('gb2312', 'ignore'))
            table = html.xpath('//table[@id="table_schedule"]//tr')
            # print(len(table))
            if len(table) < 3:
                self.logsignal.emit('当前日期没有数据！')
                continue
            for i in table:
                # print(i)
                if table.index(i) == 0:
                    continue
                elif table.index(i) % 2 != 0:
                    continue
                else:
                    # print('-------' + str(table.index(i)))
                    # print(etree.tostring(i))

                    # 类型
                    match_type = i.xpath('./td[1]//text()')[0]

                    # 时间
                    match_time = ' '.join(i.xpath('./td[2]//text()'))

                    # 主队
                    home_team = re.sub('\s*', '', str(i.xpath('./td[3]/a/text()')[0]))

                    # 客队
                    visit_team = re.sub('\s*', '', str(i.xpath('./td[11]/a/text()')[0]))

                    # 全场比分
                    all_score = i.xpath('./td[12]/font[@color="red"]/text()')[0]

                    # 半场比分
                    half_score = i.xpath('./td[12]//font[@color="black"]/text()')[0]


                    # print(match_time)
                    match_time = datetime.datetime.strptime(match_time, '%y-%m-%d %H:%M')
                    # print(match_time)
                    # print(meta)
                    next_url_temp = i.xpath('./td[13]/a/@href')[0]
                    # print(next_url_temp)

                    sid = re.findall('\d+', str(next_url_temp))[0]

                    url_send = 'http://1x2d.win007.com/{0}.js'.format(sid)
                    self.session.headers.update({
                        'Host': '1x2d.win007.com',
                        'Connection': 'keep-alive',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Upgrade-Insecure-Requests': '1',
                        # 'User-Agent': randomUA(UA_TYPE_DESKTOP),
                        'Accept': '*/*',
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'zh-CN,zh;q=0.9',
                    })
                    res_send = self.session.get(url=url_send)
                    data_res = re.findall(r'"(.*?)"', re.findall(r'game=Array\((.*)\);', res_send.text)[0])
                    for i in data_res:
                        company = i.split('|')[21]
                        # print(company)
                        meta = {
                            '赛事类型': match_type,
                            '时间': match_time,
                            '主队': home_team,
                            '客队': visit_team,
                            '公司': company,
                            '全场比分': all_score,
                            '半场比分': half_score
                        }
                        hash_dict = deepcopy(meta)
                        hash_dict.pop('全场比分')
                        hash_dict.pop('半场比分')
                        HASH_SHA1.update(''.join([str(i) for i in list(hash_dict.values())]).encode())
                        finger_print = HASH_SHA1.hexdigest()
                        meta['finger_print'] = finger_print
                        print(meta)
                        cid = i.split('|')[0]
                        # print(sid, cid)
                        self.session.headers.update({
                            'Host': 'vip.win007.com',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                            'Accept-Encoding': 'gzip, deflate',
                            'Accept-Language': 'zh-CN,zh;q=0.9',
                        })
                        url_next_start_z = 'http://vip.win007.com/count/goalCount.aspx?t=5&sid={sid}&cid={cid}&l=0'
                        url_next_immed_z = 'http://vip.win007.com/count/goalCount.aspx?t=6&sid={sid}&cid={cid}&l=0'
                        url_next_start_w = 'http://vip.win007.com/count/goalCount.aspx?t=5&sid={sid}&cid={cid}&t2=1&r=2&l=0'
                        url_next_immed_w = 'http://vip.win007.com/count/goalCount.aspx?t=6&sid={sid}&cid={cid}&t2=1&r=2&l=0'
                        url_next_start_y = 'http://vip.win007.com/count/goalCount.aspx?t=5&sid={sid}&cid={cid}&t2=1&r=3&l=0'
                        url_next_immed_y = 'http://vip.win007.com/count/goalCount.aspx?t=6&sid={sid}&cid={cid}&t2=1&r=3&l=0'
                        meta = self.end_page(url=url_next_start_z.format(sid=sid, cid=cid), code=5, meta=meta)
                        meta = self.end_page(url=url_next_immed_z.format(sid=sid, cid=cid), code=6, meta=meta)
                        meta = self.end_page_each(url=url_next_start_w.format(sid=sid, cid=cid), code=5, meta=meta, index=1)
                        meta = self.end_page_each(url=url_next_immed_w.format(sid=sid, cid=cid), code=6, meta=meta, index=1)
                        meta = self.end_page_each(url=url_next_start_y.format(sid=sid, cid=cid), code=5, meta=meta, index=2)
                        meta = self.end_page_each(url=url_next_immed_y.format(sid=sid, cid=cid), code=6, meta=meta, index=2)
                        # print(meta)
                        key_list = ','.join(list(meta.keys()))
                        value_list = ','.join(['''"{0}"'''.format(i) for i in meta.values()])
                        sql_temp = """INSERT INTO win0168_data ({0}) VALUES ({1}); """.format(key_list, value_list)
                        print(sql_temp)
                        self.logsignal.emit('正在存储最新数据' + value_list)
                        # insert into mysql database
                        try:
                            self.mysql_conn(sql=sql_temp)
                        except Exception as e:
                            # print(e)
                            update_sql_temp = """UPDATE win0168_data SET {sql_in} WHERE finger_print='{finger_print}';"""
                            sql_in = '全场比分="{0}", 半场比分="{1}" '.format(meta['全场比分'], meta['全场比分'])
                            sql = update_sql_temp.format(sql_in=sql_in, finger_print=meta['finger_print'])
                            print(sql)
                            try:
                                self.mysql_conn(sql=sql)
                            except Exception as e:
                                print(e)
                            # parimary key error
                            self.logsignal.emit('正在更新数据' + value_list)

        self.endsignal.emit('数据采集完成！')

    def end_page_each(self, url, code, meta, index):
        res = self.session.get(url=url)
        if res.status_code != 200:
            # proxy requests
            # print('proxy')
            self.logsignal.emit('抓取数据速度过快，休息10s')
            time.sleep(10)
            # print('time wait end')
            res = self.session.get(url=url, timeout=20)
            # print(res)
        # print('end page',res)
        # print(res.content.decode('gb2312', 'ignore'))
        html = etree.HTML(res.content.decode('gb2312', 'ignore'))

        # history
        try:
            count = re.findall(r'\d+', html.xpath('//table[3]//tr[1]/td/text()')[0])[0]
        except Exception:
            count = 0
        try:
            home_win = html.xpath('//table[3]//tr[1]/td/font[1]/text()')[0]
        except Exception:
            home_win = 0
        try:
            hand_of = html.xpath('//table[3]//tr[1]/td/font[2]/text()')[0]
        except Exception:
            hand_of = 0
        try:
            away_win = html.xpath('//table[3]//tr[1]/td/font[3]/text()')[0]
        except Exception:
            away_win = 0

        history = ''.join(html.xpath('//table[3]//tr[2]/td[2]//text()'))

        if index == 1:
            if code == 5:
                # ManbetX欧指统计表
                meta['初_场次_五'] = count
                meta['初_主胜_五'] = home_win
                meta['初_和局_五'] = hand_of
                meta['初_客胜_五'] = away_win
                meta['初_历史_五'] = history
                try:
                    if float(home_win) == 0 and float(hand_of) == 0 and float(away_win) == 0:
                        meta['初_赔率_五'] = 0
                    else:
                        meta['初_赔率_五'] = 1
                except Exception:
                    if float(home_win.replace('%', '')) == 0 and float(hand_of.replace('%', '')) == 0 and float(away_win.replace('%', '')) == 0:
                        meta['初_赔率_五'] = 0
                    else:
                        meta['初_赔率_五'] = 1
            else:
                meta['即_场次_五'] = count
                meta['即_主胜_五'] = home_win
                meta['即_和局_五'] = hand_of
                meta['即_客胜_五'] = away_win
                meta['即_历史_五'] = history
                try:
                    if float(home_win) == 0 and float(hand_of) == 0 and float(away_win) == 0:
                        meta['即_赔率_五'] = 0
                    else:
                        meta['即_赔率_五'] = 1
                except Exception:
                    if float(home_win.replace('%', '')) == 0 and float(hand_of.replace('%', '')) == 0 and float(away_win.replace('%', '')) == 0:
                        meta['即_赔率_五'] = 0
                    else:
                        meta['即_赔率_五'] = 1
        else:
            if code == 5:
                # ManbetX欧指统计表
                meta['初_场次_比'] = count
                meta['初_主胜_比'] = home_win
                meta['初_和局_比'] = hand_of
                meta['初_客胜_比'] = away_win
                meta['初_历史_比'] = history
                try:
                    if float(home_win) == 0 and float(hand_of) == 0 and float(away_win) == 0:
                        meta['初_赔率_比'] = 0
                    else:
                        meta['初_赔率_比'] = 1
                except Exception:
                    if float(home_win.replace('%', '')) == 0 and float(hand_of.replace('%', '')) == 0 and float(away_win.replace('%', '')) == 0:
                        meta['初_赔率_比'] = 0
                    else:
                        meta['初_赔率_比'] = 1
            else:
                meta['即_场次_比'] = count
                meta['即_主胜_比'] = home_win
                meta['即_和局_比'] = hand_of
                meta['即_客胜_比'] = away_win
                meta['即_历史_比'] = history
                try:
                    if float(home_win) == 0 and float(hand_of) == 0 and float(away_win) == 0:
                        meta['即_赔率_比'] = 0
                    else:
                        meta['即_赔率_比'] = 1
                except Exception:
                    if float(home_win.replace('%', '')) == 0 and float(hand_of.replace('%', '')) == 0 and float(away_win.replace('%', '')) == 0:
                        meta['即_赔率_比'] = 0
                    else:
                        meta['即_赔率_比'] = 1

        return meta

    def end_page(self, url, code, meta):
        res = self.session.get(url=url)
        if res.status_code != 200:
            # proxy requests
            # print('proxy')
            self.logsignal.emit('抓取数据速度过快，休息10s')
            time.sleep(10)
            # print('time wait end')
            res = self.session.get(url=url, timeout=20)
            # print(res)
        # print('end page',res)
        # print(res.content.decode('gb2312', 'ignore'))
        html = etree.HTML(res.content.decode('gb2312', 'ignore'))

        # history
        try:
            count = re.findall(r'\d+', html.xpath('//table[3]//tr[1]/td/text()')[0])[0]
        except Exception:
            count = 0
        try:
            home_win = html.xpath('//table[3]//tr[1]/td/font[1]/text()')[0]
        except Exception:
            home_win = 0
        try:
            hand_of = html.xpath('//table[3]//tr[1]/td/font[2]/text()')[0]
        except Exception:
            hand_of = 0
        try:
            away_win = html.xpath('//table[3]//tr[1]/td/font[3]/text()')[0]
        except Exception:
            away_win = 0

        history = ''.join(html.xpath('//table[3]//tr[2]/td[2]//text()'))

        if code == 5:
            # ManbetX欧指统计表
            try:
                title = html.xpath('//table[2]//tr[1]//b/text()')[0]
            except Exception:
                pass

            try:
                s_home = html.xpath('//table[2]//tr[4]/td[2]/text()')[0]
            except Exception:
                s_home = 0
            try:
                s_dish = html.xpath('//table[2]//tr[4]/td[3]/text()')[0]
            except Exception:
                s_dish = 0
            try:
                s_away = html.xpath('//table[2]//tr[4]/td[4]/text()')[0]
            except Exception:
                s_away = 0

            s_c_c = html.xpath('//table[2]//tr[4]/td[5]/a/text()')[0]
            s_c_h = html.xpath('//table[2]//tr[4]/td[6]/a/text()')[0]
            s_c_d = html.xpath('//table[2]//tr[4]/td[7]/a/text()')[0]
            s_c_a = html.xpath('//table[2]//tr[4]/td[8]/a/text()')[0]

            s_w_c = html.xpath('//table[2]//tr[4]/td[9]/a/text()')[0]
            s_w_h = html.xpath('//table[2]//tr[4]/td[10]/a/text()')[0]
            s_w_d = html.xpath('//table[2]//tr[4]/td[11]/a/text()')[0]
            s_w_a = html.xpath('//table[2]//tr[4]/td[12]/a/text()')[0]

            s_y_c = html.xpath('//table[2]//tr[4]/td[13]/a/text()')[0]
            s_y_h = html.xpath('//table[2]//tr[4]/td[14]/a/text()')[0]
            s_y_d = html.xpath('//table[2]//tr[4]/td[15]/a/text()')[0]
            s_y_a = html.xpath('//table[2]//tr[4]/td[16]/a/text()')[0]

            try:
                i_home = html.xpath('//table[2]//tr[5]/td[2]/text()')[0]
            except Exception:
                i_home = 0
            try:
                i_dish = html.xpath('//table[2]//tr[5]/td[3]/text()')[0]
            except Exception:
                i_dish = 0
            try:
                i_away = html.xpath('//table[2]//tr[5]/td[4]/text()')[0]
            except Exception:
                i_away = 0

            i_c_c = html.xpath('//table[2]//tr[5]/td[5]/a/text()')[0]
            i_c_h = html.xpath('//table[2]//tr[5]/td[6]/a/text()')[0]
            i_c_d = html.xpath('//table[2]//tr[5]/td[7]/a/text()')[0]
            i_c_a = html.xpath('//table[2]//tr[5]/td[8]/a/text()')[0]

            i_w_c = html.xpath('//table[2]//tr[5]/td[9]/a/text()')[0]
            i_w_h = html.xpath('//table[2]//tr[5]/td[10]/a/text()')[0]
            i_w_d = html.xpath('//table[2]//tr[5]/td[11]/a/text()')[0]
            i_w_a = html.xpath('//table[2]//tr[5]/td[12]/a/text()')[0]

            i_y_c = html.xpath('//table[2]//tr[5]/td[13]/a/text()')[0]
            i_y_h = html.xpath('//table[2]//tr[5]/td[14]/a/text()')[0]
            i_y_d = html.xpath('//table[2]//tr[5]/td[15]/a/text()')[0]
            i_y_a = html.xpath('//table[2]//tr[5]/td[16]/a/text()')[0]

            # meta['公司'] = title
            meta['初_主队'] = s_home
            meta['初_盘口'] = s_dish
            meta['初_客队'] = s_away
            meta['初_总计_场'] = s_c_c
            meta['初_总计_主'] = s_c_h
            meta['初_总计_和'] = s_c_d
            meta['初_总计_客'] = s_c_a
            meta['初_五大联赛_场'] = s_w_c
            meta['初_五大联赛_主'] = s_w_h
            meta['初_五大联赛_和'] = s_w_d
            meta['初_五大联赛_客'] = s_w_a
            meta['初_以甲_场'] = s_y_c
            meta['初_以甲_主'] = s_y_h
            meta['初_以甲_和'] = s_y_d
            meta['初_以甲_客'] = s_y_a
            meta['初_场次_总'] = count
            meta['初_主胜_总'] = home_win
            meta['初_和局_总'] = hand_of
            meta['初_客胜_总'] = away_win
            meta['初_历史_总'] = history
            meta['即_主队'] = i_home
            meta['即_盘口'] = i_dish
            meta['即_客队'] = i_away
            meta['即_总计_场'] = i_c_c
            meta['即_总计_主'] = i_c_h
            meta['即_总计_和'] = i_c_d
            meta['即_总计_客'] = i_c_a
            meta['即_五大联赛_场'] = i_w_c
            meta['即_五大联赛_主'] = i_w_h
            meta['即_五大联赛_和'] = i_w_d
            meta['即_五大联赛_客'] = i_w_a
            meta['即_以甲_场'] = i_y_c
            meta['即_以甲_主'] = i_y_h
            meta['即_以甲_和'] = i_y_d
            meta['即_以甲_客'] = i_y_a
            try:
                if float(home_win) == 0 and float(hand_of) == 0 and float(away_win) == 0:
                    meta['初_赔率_总'] = 0
                else:
                    meta['初_赔率_总'] = 1
            except Exception:
                if float(home_win.replace('%', '')) == 0 and float(hand_of.replace('%', '')) == 0 and float(away_win.replace('%', '')) == 0:
                    meta['初_赔率_总'] = 0
                else:
                    meta['初_赔率_总'] = 1
        else:
            meta['即_场次_总'] = count
            meta['即_主胜_总'] = home_win
            meta['即_和局_总'] = hand_of
            meta['即_客胜_总'] = away_win
            meta['即_历史_总'] = history
            try:
                if float(home_win) == 0 and float(hand_of) == 0 and float(away_win) == 0:
                    meta['即_赔率_总'] = 0
                else:
                    meta['即_赔率_总'] = 1
            except Exception:
                if float(home_win.replace('%', '')) == 0 and float(hand_of.replace('%', '')) == 0 and float(away_win.replace('%', '')) == 0:
                    meta['即_赔率_总'] = 0
                else:
                    meta['即_赔率_总'] = 1

        return meta


    def start_task(self, data):
        # print(data)
        self.date_list = data['date_list']
        self.update_code = data['update_code']
        self.start()
        # self.run()



    def mysql_conn(self, sql):
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PWD,
            database=MYSQL_DB,
            charset=CHAR_SET
        )
        cur = conn.cursor()
        if not cur:
            pass
            # print(NameError, 'SQL Server Connect failed!')
        else:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(sql)
            conn.commit()
            conn.close()

if __name__ == '__main__':
    SPIDER = Spider().start_task()