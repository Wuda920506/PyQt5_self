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
from lxml import etree
from useragent import randomUA, UA_TYPE_DESKTOP
from PyQt5.QtCore import pyqtSignal, QThread

TYPE = [
    '英超', '英冠', '英甲', '英乙', '英足总杯', '英锦赛', '英联杯', '意甲', '意乙', '意杯', '西甲', '西乙', '西杯',
    '西联杯', '德甲', '德乙', '德丙', '德国杯', '法甲', '法乙', '法丙', '法国杯', '法联杯', '葡超', '葡甲', '葡杯',
    '葡联杯', '苏超', '苏冠', '苏总杯', '苏联杯', '荷甲', '荷乙', '荷兰杯', '比甲', '比乙', '比利时杯', '瑞典超',
    '瑞典甲', '瑞典杯', '芬超', '芬甲', '芬联杯', '挪超', '挪甲', '挪威杯', '丹麦超', '丹麦甲', '奥甲', '奥乙',
    '奥地利杯', '瑞士超', '瑞士甲', '瑞士杯', '爱超', '爱甲', '爱联杯', '北爱超', '俄超', '俄甲', '俄杯', '波兰超',
    '捷甲', '希腊超', '希腊杯', '罗甲', '罗杯', '罗联杯', '冰岛超', '土超', '土甲', '土杯', '克亚甲', '阿甲',
    '巴西甲', '巴西乙', '巴西杯', '巴圣锦标', '美职业', '智利甲', '墨西联', '墨西哥杯', '中超', '日职联', '日职乙',
    '日皇杯', '日足联', '韩K联', '澳洲甲', '世界杯', '欧洲预选', '亚洲预选', '南美预选', '非洲预选', '世俱杯',
    '欧洲杯', '欧冠杯', '欧罗巴杯', '亚洲杯', '亚冠杯', '美洲杯', '自由杯', '南球杯', '非洲杯'
]
COMPANY = [
    'LEON', 'Interwetten', 'Nordicbet', 'SNAI', 'Bwin', 'bwin', '明陞', '威廉希尔', '10BET', 'Singbet', '伟德', '易胜博',
    '利记sbobet', '竞彩官方', '立博', 'bet365', '金宝博', 'Betfair', 'Coral', 'Pinnacle', '5Dimes', 'bet-at-home',
    'Bet3000', 'GWbet', 'Expekt', 'Nike', 'Jetbull', 'e-stave', '澳门', '香港马会', 'Eurobet', 'IBCBET',
    'Oddset', 'STS', 'TOTO', '18Bet', 'Matchbook', 'Betsson', 'Norway', 'Pamestihima', 'Smarkets',
    'BINGOAL', 'iddaa'
]

MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_DB = 'soccer'
MYSQL_USER = 'root'
MYSQL_PWD = 'ershi123'
# MYSQL_HOST = '127.0.0.1'
# MYSQL_PORT = 3306
# MYSQL_DB = 'soccer'
# MYSQL_USER = 'root'
# MYSQL_PWD = 'root'

CHAR_SET = 'utf8'
TABLE = 't_data'

CREATE_TABLE_SQL = """CREATE TABLE IF NOT EXISTS t_data(
ID BIGINT AUTO_INCREMENT,
类型 VARCHAR(100),
时间 datetime,
比分 VARCHAR(50),
主队 VARCHAR(100),
客队 VARCHAR(100),
LEON_胜赔 VARCHAR(50),
LEON_平赔 VARCHAR(50),
LEON_负赔 VARCHAR(50),
Interwetten_胜赔 VARCHAR(50),
Interwetten_平赔 VARCHAR(50),
Interwetten_负赔 VARCHAR(50),
Nordicbet_胜赔 VARCHAR(50),
Nordicbet_平赔 VARCHAR(50),
Nordicbet_负赔 VARCHAR(50),
SNAI_胜赔 VARCHAR(50),
SNAI_平赔 VARCHAR(50),
SNAI_负赔 VARCHAR(50),
bwin_胜赔 VARCHAR(50),
bwin_平赔 VARCHAR(50),
bwin_负赔 VARCHAR(50),
明陞_胜赔 VARCHAR(50),
明陞_平赔 VARCHAR(50),
明陞_负赔 VARCHAR(50),
威廉希尔_胜赔 VARCHAR(50),
威廉希尔_平赔 VARCHAR(50),
威廉希尔_负赔 VARCHAR(50),
BET10_胜赔 VARCHAR(50),
BET10_平赔 VARCHAR(50),
BET10_负赔 VARCHAR(50),
Singbet_胜赔 VARCHAR(50),
Singbet_平赔 VARCHAR(50),
Singbet_负赔 VARCHAR(50),
伟德_胜赔 VARCHAR(50),
伟德_平赔 VARCHAR(50),
伟德_负赔 VARCHAR(50),
易胜博_胜赔 VARCHAR(50),
易胜博_平赔 VARCHAR(50),
易胜博_负赔 VARCHAR(50),
利记_胜赔 VARCHAR(50),
利记_平赔 VARCHAR(50),
利记_负赔 VARCHAR(50),
竞彩官方_胜赔 VARCHAR(50),
竞彩官方_平赔 VARCHAR(50),
竞彩官方_负赔 VARCHAR(50),
立博_胜赔 VARCHAR(50),
立博_平赔 VARCHAR(50),
立博_负赔 VARCHAR(50),
bet365_胜赔 VARCHAR(50),
bet365_平赔 VARCHAR(50),
bet365_负赔 VARCHAR(50),
金宝博_胜赔 VARCHAR(50),
金宝博_平赔 VARCHAR(50),
金宝博_负赔 VARCHAR(50),
Betfair_胜赔 VARCHAR(50),
Betfair_平赔 VARCHAR(50),
Betfair_负赔 VARCHAR(50),
Coral_胜赔 VARCHAR(50),
Coral_平赔 VARCHAR(50),
Coral_负赔 VARCHAR(50),
PinnacleSports_胜赔 VARCHAR(50),
PinnacleSports_平赔 VARCHAR(50),
PinnacleSports_负赔 VARCHAR(50),
Dimes_胜赔 VARCHAR(50),
Dimes_平赔 VARCHAR(50),
Dimes_负赔 VARCHAR(50),
betathome_胜赔 VARCHAR(50),
betathome_平赔 VARCHAR(50),
betathome_负赔 VARCHAR(50),
Bet3000_胜赔 VARCHAR(50),
Bet3000_平赔 VARCHAR(50),
Bet3000_负赔 VARCHAR(50),
GWbet_胜赔 VARCHAR(50),
GWbet_平赔 VARCHAR(50),
GWbet_负赔 VARCHAR(50),
Expekt_胜赔 VARCHAR(50),
Expekt_平赔 VARCHAR(50),
Expekt_负赔 VARCHAR(50),
Nike_胜赔 VARCHAR(50),
Nike_平赔 VARCHAR(50),
Nike_负赔 VARCHAR(50),
Jetbull_胜赔 VARCHAR(50),
Jetbull_平赔 VARCHAR(50),
Jetbull_负赔 VARCHAR(50),
e_stave_胜赔 VARCHAR(50),
e_stave_平赔 VARCHAR(50),
e_stave_负赔 VARCHAR(50),
澳门_胜赔 VARCHAR(50),
澳门_平赔 VARCHAR(50),
澳门_负赔 VARCHAR(50),
香港马会_胜赔 VARCHAR(50),
香港马会_平赔 VARCHAR(50),
香港马会_负赔 VARCHAR(50),
Eurobet_胜赔 VARCHAR(50),
Eurobet_平赔 VARCHAR(50),
Eurobet_负赔 VARCHAR(50),
IBCBET_胜赔 VARCHAR(50),
IBCBET_平赔 VARCHAR(50),
IBCBET_负赔 VARCHAR(50),
Oddset_胜赔 VARCHAR(50),
Oddset_平赔 VARCHAR(50),
Oddset_负赔 VARCHAR(50),
STS_胜赔 VARCHAR(50),
STS_平赔 VARCHAR(50),
STS_负赔 VARCHAR(50),
TOTO_胜赔 VARCHAR(50),
TOTO_平赔 VARCHAR(50),
TOTO_负赔 VARCHAR(50),
Bet18_胜赔 VARCHAR(50),
Bet18_平赔 VARCHAR(50),
Bet18_负赔 VARCHAR(50),
Matchbook_胜赔 VARCHAR(50),
Matchbook_平赔 VARCHAR(50),
Matchbook_负赔 VARCHAR(50),
Betsson_胜赔 VARCHAR(50),
Betsson_平赔 VARCHAR(50),
Betsson_负赔 VARCHAR(50),
Norway_胜赔 VARCHAR(50),
Norway_平赔 VARCHAR(50),
Norway_负赔 VARCHAR(50),
Pamestihima_胜赔 VARCHAR(50),
Pamestihima_平赔 VARCHAR(50),
Pamestihima_负赔 VARCHAR(50),
Smarkets_胜赔 VARCHAR(50),
Smarkets_平赔 VARCHAR(50),
Smarkets_负赔 VARCHAR(50),
BINGOAL_胜赔 VARCHAR(50),
BINGOAL_平赔 VARCHAR(50),
BINGOAL_负赔 VARCHAR(50),
iddaa_胜赔 VARCHAR(50),
iddaa_平赔 VARCHAR(50),
iddaa_负赔 VARCHAR(50),
PRIMARY KEY(ID)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

INSERT_SQL = """INSERT INTO t_data (
类型,
时间,
比分,
主队,
客队,
LEON_胜赔,
LEON_平赔,
LEON_负赔,
Interwetten_胜赔,
Interwetten_平赔,
Interwetten_负赔,
Nordicbet_胜赔,
Nordicbet_平赔,
Nordicbet_负赔,
SNAI_胜赔,
SNAI_平赔,
SNAI_负赔,
bwin_胜赔,
bwin_平赔,
bwin_负赔,
明陞_胜赔,
明陞_平赔,
明陞_负赔,
威廉希尔_胜赔,
威廉希尔_平赔,
威廉希尔_负赔,
BET10_胜赔,
BET10_平赔,
BET10_负赔,
Singbet_胜赔,
Singbet_平赔,
Singbet_负赔,
伟德_胜赔,
伟德_平赔,
伟德_负赔,
易胜博_胜赔,
易胜博_平赔,
易胜博_负赔,
利记_胜赔,
利记_平赔,
利记_负赔,
竞彩官方_胜赔,
竞彩官方_平赔,
竞彩官方_负赔,
立博_胜赔,
立博_平赔,
立博_负赔,
bet365_胜赔,
bet365_平赔,
bet365_负赔,
金宝博_胜赔,
金宝博_平赔,
金宝博_负赔,
Betfair_胜赔,
Betfair_平赔,
Betfair_负赔,
Coral_胜赔,
Coral_平赔,
Coral_负赔,
PinnacleSports_胜赔,
PinnacleSports_平赔,
PinnacleSports_负赔,
Dimes_胜赔,
Dimes_平赔,
Dimes_负赔,
betathome_胜赔,
betathome_平赔,
betathome_负赔,
Bet3000_胜赔,
Bet3000_平赔,
Bet3000_负赔,
GWbet_胜赔,
GWbet_平赔,
GWbet_负赔,
Expekt_胜赔,
Expekt_平赔,
Expekt_负赔,
Nike_胜赔,
Nike_平赔,
Nike_负赔,
Jetbull_胜赔,
Jetbull_平赔,
Jetbull_负赔,
e_stave_胜赔,
e_stave_平赔,
e_stave_负赔,
澳门_胜赔,
澳门_平赔,
澳门_负赔,
香港马会_胜赔,
香港马会_平赔,
香港马会_负赔,
Eurobet_胜赔,
Eurobet_平赔,
Eurobet_负赔,
IBCBET_胜赔,
IBCBET_平赔,
IBCBET_负赔,
Oddset_胜赔,
Oddset_平赔,
Oddset_负赔,
STS_胜赔,
STS_平赔,
STS_负赔,
TOTO_胜赔,
TOTO_平赔,
TOTO_负赔,
Bet18_胜赔,
Bet18_平赔,
Bet18_负赔,
Matchbook_胜赔,
Matchbook_平赔,
Matchbook_负赔,
Redbet_胜赔,
Redbet_平赔,
Redbet_负赔,
Betsson_胜赔,
Betsson_平赔,
Betsson_负赔,
Norway_胜赔,
Norway_平赔,
Norway_负赔,
Pamestihima_胜赔,
Pamestihima_平赔,
Pamestihima_负赔,
Smarkets_胜赔,
Smarkets_平赔,
Smarkets_负赔,
BINGOAL_胜赔,
BINGOAL_平赔,
BINGOAL_负赔,
iddaa_胜赔,
iddaa_平赔,
iddaa_负赔,
) VALUES ({0});"""

import logging
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
# handler = RotatingFileHandler("log.txt", maxBytes=100 * 1024, backupCount=5)
handler = logging.FileHandler('win007.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class Spider(QThread):

    endsignal = pyqtSignal(str)
    logsignal = pyqtSignal(str)

    def __init__(self, parent=None):
        super(Spider, self).__init__(parent)

        self.session = requests.session()
        self.session.headers['User_Agent'] = randomUA(UA_TYPE_DESKTOP)
        self.code = 0

    def date_list_get(self, date1, date2):
        """
        :param date1: [2010, 1, 1]
        :param date2: [2019, 1, 20]
        :return: date_list
        """
        self.logsignal.emit("正在生成日期")
        date_list = []
        if date1[0] == date2[0]:
            if date1[1] == date2[1]:
                if len(str(date1[1])) == 1:
                    mon = '0' + str(date1[1])
                    if date1[2] == date2[2]:
                        day = '0' + str(date1[2]) if len(str(date1[2])) == 1 else str(date1[2])
                        date_list.append(str(date1[0]) + mon + day)
                    else:
                        for j in range(int(date1[2]), int(date2[2])):
                            day = '0' + str(j) if len(str(j)) == 1 else str(j)
                            date_list.append(str(date1[0]) + mon + day)
                else:
                    mon = str(date1[1])
                    if date1[2] == date2[2]:
                        day = '0' + str(date1[2]) if len(str(date1[2])) == 1 else str(date1[2])
                        date_list.append(str(date1[0]) + mon + day)
                    else:
                        for j in range(int(date1[2], date2[2])):
                            day = '0' + str(j) if len(str(j)) == 1 else str(j)
                            date_list.append(str(date1[0]) + mon + day)
            else:
                self.logsignal.emit('不能跨月抓取，计算容易出现误差')
        else:
            for year in range(date1[0], date2[0]):
                for month in range(1, 13):
                    if month in [1, 3, 5, 7, 8, 10, 12]:
                        for day in range(1, 32):
                            if int(day) <= 31:
                                if len(str(month)) == 1:
                                    month = '0' + str(month)
                                if len(str(day)) == 1:
                                    day = '0' + str(day)
                                date_list.append(str(year) + str(month) + str(day))
                    elif month in [4, 6, 9, 11]:
                        for day in range(1, 32):
                            if int(day) <= 30:
                                if len(str(month)) == 1:
                                    month = '0' + str(month)
                                if len(str(day)) == 1:
                                    day = '0' + str(day)
                                date_list.append(str(year) + str(month) + str(day))
                    elif month == 2:
                        for day in range(1, 32):
                            if int(day) <= 29:
                                if len(str(month)) == 1:
                                    month = '0' + str(month)
                                if len(str(day)) == 1:
                                    day = '0' + str(day)
                                date_list.append(str(year) + str(month) + str(day))
        return date_list

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
        try:
            html = etree.HTML(res.content.decode('utf-8', 'ignore'))
        except Exception as e:
            logger.error(str(e))
            print(e)
        self.__VIEWSTATE = html.xpath('//input[@id="__VIEWSTATE"]/@value')[0]
        # print(self.__VIEWSTATE)
        self.__VIEWSTATEGENERATOR = html.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value')[0]
        # print(self.__VIEWSTATEGENERATOR)
        self.__EVENTVALIDATION = html.xpath('//input[@id="__EVENTVALIDATION"]/@value')[0]
        # print(self.__EVENTVALIDATION)
        self.logsignal.emit("程序正在准备")

    def parse(self):
        self.start_requests()
        url_temp = "http://op1.win007.com/bet007history.aspx"
        # url_temp = 'http://op1.win007.com/overodds/cn/{time}.html'
        for i in self.date_list:
            ttime = i
            # print(243)
            # print(i)
            # print(1234)
            # print(i[:4] + '-' + i[4:6] + '-' + i[6:])
            self.endsignal.emit(i + '\n正在进行')
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
                'matchdate': i[:4] + '-' + i[4:6] + '-' + i[6:],
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
            for i in table:
                length = len(table)
                index = table.index(i)
                if index == length - 1:
                    self.logsignal.emit('---------------【采集完成】----------')
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
                    print(match_time)

                    # 比分
                    score = i.xpath('./td[12]/font[@color="red"]/text()')[0]

                    # 主队
                    home_team = re.sub('\s*', '', str(i.xpath('./td[3]/a/text()')[0]))

                    # 客队
                    visit_team = re.sub('\s*', '', str(i.xpath('./td[11]/a/text()')[0]))

                    # print(123344)
                    if match_type.strip() not in TYPE:
                        # # print('match type is not in list')
                        continue
                    else:
                        # print(match_time)
                        match_time = datetime.datetime.strptime(match_time, '%y-%m-%d %H:%M')
                        print(match_time)
                        meta = {
                            '类型': match_type,
                            '时间': match_time,
                            '比分': score,
                            '主队': home_team,
                            '客队': visit_team
                        }
                        # print(meta)
                        next_url_temp = i.xpath('./td[13]/a/@href')[0]
                        # print(next_url_temp)
                        url_send = 'http://1x2d.win007.com/{0}.js'.format(re.findall('\d+', str(next_url_temp))[0])
                        # print(url_send)
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
                        try:
                            res_send = self.session.get(url=url_send, timeout=30)
                        except Exception as e:
                            print(e)
                            self.logsignal.emit(url_send + '连接超时')
                        # print(res_send.text)
                        data_res = re.findall(r'"(.*?)"', re.findall(r'game=Array\((.*)\);', res_send.text)[0])
                        for i in data_res:
                            try:
                                company_str = i.split('|')[21]
                            except Exception as e:
                                print(e)
                            print(company_str)
                            if '(' in company_str:
                                company = re.sub(r'\s+', '', re.findall(r'(.*)\(', company_str)[0])
                            else:
                                company = re.sub(r'\s+', '', company_str)
                            # print (company)
                            if company not in COMPANY:
                                continue
                            else:
                                print(company)
                                # input()
                                self.logsignal.emit('URL【{0}】找到公司【{1}】'.format(url_send, company))
                                if company == 'LEON':
                                    meta['LEON_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['LEON_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['LEON_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Interwetten':
                                    meta['Interwetten_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Interwetten_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Interwetten_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Nordicbet':
                                    meta['Nordicbet_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Nordicbet_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Nordicbet_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'SNAI':
                                    meta['SNAI_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['SNAI_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['SNAI_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Bwin':
                                    meta['bwin_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['bwin_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['bwin_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'bwin':
                                    meta['bwin_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['bwin_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['bwin_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '明陞':
                                    meta['明陞_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['明陞_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['明陞_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '威廉希尔':
                                    meta['威廉希尔_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['威廉希尔_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['威廉希尔_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '10BET':
                                    meta['BET10_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['BET10_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['BET10_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Singbet':
                                    meta['Singbet_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Singbet_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Singbet_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '伟德':
                                    meta['伟德_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['伟德_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['伟德_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '易胜博':
                                    meta['易胜博_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['易胜博_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['易胜博_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '利记sbobet':
                                    meta['利记_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['利记_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['利记_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '竞彩官方':
                                    meta['竞彩官方_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['竞彩官方_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['竞彩官方_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '立博':
                                    meta['立博_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['立博_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['立博_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'bet365':
                                    meta['bet365_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['bet365_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['bet365_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '金宝博':
                                    meta['金宝博_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['金宝博_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['金宝博_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Betfair':
                                    meta['Betfair_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Betfair_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Betfair_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Coral':
                                    meta['Coral_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Coral_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Coral_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Pinnacle':
                                    meta['PinnacleSports_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['PinnacleSports_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['PinnacleSports_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '5Dimes':
                                    meta['Dimes_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Dimes_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Dimes_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'bet-at-home':
                                    meta['betathome_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['betathome_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['betathome_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Bet3000':
                                    meta['Bet3000_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Bet3000_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Bet3000_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'GWbet':
                                    meta['GWbet_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['GWbet_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['GWbet_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Expekt':
                                    meta['Expekt_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Expekt_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Expekt_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Nike':
                                    meta['Nike_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Nike_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Nike_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Jetbull':
                                    meta['Jetbull_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Jetbull_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Jetbull_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'e-stave':
                                    meta['e_stave_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['e_stave_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['e_stave_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '澳门':
                                    meta['澳门_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['澳门_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['澳门_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '香港马会':
                                    meta['香港马会_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['香港马会_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['香港马会_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Eurobet':
                                    meta['Eurobet_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Eurobet_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Eurobet_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'IBCBET':
                                    meta['IBCBET_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['IBCBET_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['IBCBET_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Oddset':
                                    meta['Oddset_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Oddset_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Oddset_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'STS':
                                    meta['STS_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['STS_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['STS_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'TOTO':
                                    meta['TOTO_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['TOTO_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['TOTO_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == '18Bet':
                                    meta['Bet18_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Bet18_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Bet18_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Matchbook':
                                    meta['Matchbook_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Matchbook_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Matchbook_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Betsson':
                                    meta['Betsson_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Betsson_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Betsson_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Norway':
                                    meta['Norway_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Norway_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Norway_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Pamestihima':
                                    meta['Pamestihima_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Pamestihima_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Pamestihima_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'Smarkets':
                                    meta['Smarkets_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['Smarkets_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['Smarkets_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'BINGOAL':
                                    meta['BINGOAL_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['BINGOAL_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['BINGOAL_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                elif company == 'iddaa':
                                    meta['iddaa_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                    meta['iddaa_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                    meta['iddaa_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                                else:
                                    pass
                        # print(9999999)
                        key_list = list(meta.keys())
                        value_list = ["'{0}'".format(i) for i in list(meta.values())]

                        sql = """INSERT INTO {table} ({key}) VALUES ({value});""".format(
                            table=TABLE,
                            key=', '.join(key_list),
                            value=', '.join(value_list)
                        )
                        print(sql)
                        # print(index, length)
                        # input()
                        if not index == length - 1:
                            self.logsignal.emit('URL【{0}】正在存入数据库'.format(url_send))
                        else:
                            self.logsignal.emit('URL【{0}】正在存入数据库, 【采集完成】'.format(url_send))

                        # self.sql_conn(sql=sql)
                        try:
                            self.mysql_conn(sql=sql)
                        except Exception as e:
                            print(e)
                            logger.warning('数据库异常' + str(e))
                        try:
                            self.sql_conn(sql=sql)
                        except Exception as e:
                            print(e)
                            logger.warning('数据库入库出错' + str(e))
                            self.logsignal.emit("数据库存入异常")
                            self.logsignal.emit(str(e))
                        time.sleep(1)
                # print('end')
            try:
                self.endsignal.emit(ttime + '\n采集完毕')
            except Exception as e:
                print(e)

    def parse_ing(self):
        # self.start_requests()
        url_temp = "http://op1.win007.com/index.aspx"
        # url_temp = 'http://op1.win007.com/overodds/cn/{time}.html'
        self.session.headers.update({
            'Host': 'op1.win007.com',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        })
        # time.sleep(2)
        self.session.headers['User_Agent'] = randomUA(UA_TYPE_DESKTOP)
        res = self.session.get(url=url_temp)
        # print(res)
        # print(res.content.decode('gb2312', 'ignore'))
        try:
            html = etree.HTML(res.content.decode('gb2312', 'ignore'))
        except Exception as e:
            logger.error('解析出错  ' + url_temp + "  " + str(e))
            return
        table = html.xpath('//table[@id="table_schedule"]//tr')
        # print(len(table))
        for i in table:
            length = len(table)
            index = table.index(i)
            # print(index)
            if index == length - 1:
                self.logsignal.emit('---------------【采集完成】----------')
            if table.index(i) == 0:
                continue
            elif table.index(i) % 2 == 0:
                continue
            else:
                # print('-------' + str(table.index(i)))
                # print(etree.tostring(i))

                # 类型
                match_type = i.xpath('./td[2]//text()')[0]

                # 时间
                match_time = ' '.join(i.xpath('./td[3]//text()'))
                # print(match_time)

                # 比分
                # score = i.xpath('./td[12]/font[@color="red"]/text()')[0]
                score = ''

                # 主队
                home_team = re.sub('\s*', '', str(i.xpath('./td[4]/a/text()')[0]))

                # 客队
                visit_team = re.sub('\s*', '', str(i.xpath('./td[12]/a/text()')[0]))

                # print(123344)
                if match_type.strip() not in TYPE:
                    # print('match type is not in list', match_type.strip())
                    continue
                else:
                    print(match_type, match_time)
                    # print(match_time)
                    match_time = datetime.datetime.strptime(match_time.strip(), '%y-%m-%d %H:%M')
                    # print(match_time)
                    meta = {
                        '类型': match_type,
                        '时间': match_time,
                        '比分': score,
                        '主队': home_team,
                        '客队': visit_team
                    }
                    next_url_temp = i.xpath('./td[13]/a/@href')[0]
                    # print(next_url_temp)
                    url_send = 'http://1x2d.win007.com/{0}.js'.format(re.findall('\d+', str(next_url_temp))[0])
                    # print(url_send)
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
                    try:
                        res_send = self.session.get(url=url_send, timeout=30)
                    except Exception as e:
                        print(e)
                        print(url_send + '连接超时')
                        self.logsignal.emit(url_send + '连接超时')
                    # print(res_send.text)
                    data_res = re.findall(r'"(.*?)"', re.findall(r'game=Array\((.*)\);', res_send.text)[0])
                    for i in data_res:
                        try:
                            company_str = i.split('|')[21]
                        except Exception as e:
                            print(e)
                            print(company)
                        # print(company_str)
                        if '(' in company_str:
                            company = re.sub(r'\s+', '', re.findall(r'(.*)\(', company_str)[0])
                        else:
                            company = re.sub(r'\s+', '', company_str)
                        # print (company)
                        if company not in COMPANY:
                            continue
                        else:
                            # print(company)
                            # input()
                            self.logsignal.emit('URL【{0}】找到公司【{1}】'.format(url_send, company))
                            if company == 'LEON':
                                meta['LEON_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['LEON_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['LEON_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Interwetten':
                                meta['Interwetten_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Interwetten_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Interwetten_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Nordicbet':
                                meta['Nordicbet_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Nordicbet_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Nordicbet_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'SNAI':
                                meta['SNAI_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['SNAI_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['SNAI_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Bwin':
                                meta['bwin_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['bwin_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['bwin_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'bwin':
                                meta['bwin_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['bwin_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['bwin_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '明陞':
                                meta['明陞_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['明陞_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['明陞_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '威廉希尔':
                                meta['威廉希尔_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['威廉希尔_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['威廉希尔_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '10BET':
                                meta['BET10_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['BET10_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['BET10_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Singbet':
                                meta['Singbet_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Singbet_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Singbet_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '伟德':
                                meta['伟德_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['伟德_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['伟德_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '易胜博':
                                meta['易胜博_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['易胜博_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['易胜博_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '利记sbobet':
                                meta['利记_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['利记_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['利记_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '竞彩官方':
                                meta['竞彩官方_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['竞彩官方_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['竞彩官方_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '立博':
                                meta['立博_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['立博_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['立博_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'bet365':
                                meta['bet365_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['bet365_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['bet365_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '金宝博':
                                meta['金宝博_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['金宝博_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['金宝博_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Betfair':
                                meta['Betfair_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Betfair_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Betfair_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Coral':
                                meta['Coral_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Coral_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Coral_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Pinnacle':
                                meta['PinnacleSports_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['PinnacleSports_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['PinnacleSports_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '5Dimes':
                                meta['Dimes_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Dimes_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Dimes_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'bet-at-home':
                                meta['betathome_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['betathome_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['betathome_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Bet3000':
                                meta['Bet3000_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Bet3000_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Bet3000_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'GWbet':
                                meta['GWbet_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['GWbet_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['GWbet_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Expekt':
                                meta['Expekt_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Expekt_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Expekt_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Nike':
                                meta['Nike_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Nike_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Nike_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Jetbull':
                                meta['Jetbull_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Jetbull_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Jetbull_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'e-stave':
                                meta['e_stave_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['e_stave_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['e_stave_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '澳门':
                                meta['澳门_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['澳门_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['澳门_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '香港马会':
                                meta['香港马会_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['香港马会_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['香港马会_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Eurobet':
                                meta['Eurobet_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Eurobet_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Eurobet_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'IBCBET':
                                meta['IBCBET_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['IBCBET_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['IBCBET_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Oddset':
                                meta['Oddset_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Oddset_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Oddset_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'STS':
                                meta['STS_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['STS_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['STS_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'TOTO':
                                meta['TOTO_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['TOTO_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['TOTO_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == '18Bet':
                                meta['Bet18_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Bet18_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Bet18_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Matchbook':
                                meta['Matchbook_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Matchbook_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Matchbook_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Betsson':
                                meta['Betsson_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Betsson_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Betsson_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Norway':
                                meta['Norway_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Norway_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Norway_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Pamestihima':
                                meta['Pamestihima_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Pamestihima_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Pamestihima_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'Smarkets':
                                meta['Smarkets_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['Smarkets_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['Smarkets_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'BINGOAL':
                                meta['BINGOAL_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['BINGOAL_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['BINGOAL_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            elif company == 'iddaa':
                                meta['iddaa_胜赔'] = str('%.2f' % float(i.split('|')[3])) if i.split('|')[3] else ''
                                meta['iddaa_平赔'] = str('%.2f' % float(i.split('|')[4])) if i.split('|')[4] else ''
                                meta['iddaa_负赔'] = str('%.2f' % float(i.split('|')[5])) if i.split('|')[5] else ''
                            else:
                                pass
                    # print(9999999)
                    key_list = list(meta.keys())
                    value_list = ["'{0}'".format(i) for i in list(meta.values())]

                    sql = """INSERT INTO {table} ({key}) VALUES ({value});""".format(
                        table=TABLE,
                        key=', '.join(key_list),
                        value=', '.join(value_list)
                    )
                    print(sql)
                    # print(index, length)
                    # input()
                    if not index == length - 1:
                        self.logsignal.emit('URL【{0}】正在存入数据库'.format(url_send))
                    else:
                        self.logsignal.emit('URL【{0}】正在存入数据库, 【采集完成】'.format(url_send))

                    # self.sql_conn(sql=sql)
                    try:
                        self.mysql_conn(sql=sql)
                    except Exception as e:
                        print(e)
                        print('数据库异常')
                        logger.warning('数据库异常' + str(e))
                    try:
                        self.sql_conn(sql=sql)
                    except Exception as e:
                        print(e)
                        print('数据库入库出错')
                        logger.warning('数据库入库出错' + str(e))
                        self.logsignal.emit("数据库存入异常")
                        self.logsignal.emit(str(e))
                    time.sleep(1)
        try:
            self.endsignal.emit('采集完毕')
        except Exception as e:
            print(e)
        print('----采集完毕----')

    def start_task(self, data):
        # print(data)
        with open('setting.json', 'r') as f:
            data = f.read()
        meta = json.loads(data)
        self.host = meta["host"]
        self.port = meta["port"]
        self.user = meta["user"]
        self.pwd = meta["pwd"]
        self.database = meta["database"]
        self.table = meta["table"]
        self.parse_ing()
        # if meta["code"]:
        #     self.code = 1
        #     start_date = meta["start_date"]
        #     end_date = meta["end_date"]
        #     self.date_one = [[int(i) for i in start_date.split('-')], [int(j) for j in end_date.split('-')]]
        #     # print(self.date_one)
        #     # print(123)
        #     self.start()
        # else:
        #     self.code = 0
        #     self.start()

    def run(self):
        if self.code == 0:
            self.date_list = self.date_list_get([2010, 1, 1], [2019, 1, 22])
        else:
            self.date_list = self.date_list_get(self.date_one[0], self.date_one[1])
        print(self.date_list)
        self.parse()

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
            print (NameError, 'SQL Server Connect failed!')
        else:
            cur.execute(CREATE_TABLE_SQL)
            try:
                cur.execute(sql)
            except Exception as e:
                pass
            conn.commit()
            conn.close()

    def sql_conn(self, sql):
        # print(self.host, self.user, self.pwd, self.database)
        conn = pymssql.connect(
            host=self.host,
            user=self.user,
            password=self.pwd,
            database=self.database,
            charset=CHAR_SET
        )
        cur = conn.cursor()
        if not cur:
            raise (NameError, 'SQL Server Connect failed!')
        else:
            # cur.execute(CREATE_TABLE_SQL)
            # print(sql)
            # input()
            # sql = """INSERT INTO t_data (类型, 时间, 比分, 主队, 客队, bet365_胜赔, bet365_平赔, bet365_负赔, 威廉希尔_胜赔, 威廉希尔_平赔, 威廉希尔_负赔, 立博_胜赔, 立博_平赔, 立博_负赔, betathome_胜赔, betathome_平赔, betathome_负赔, 伟德_胜赔, 伟德_平赔, 伟德_负赔, 易胜博_胜赔, 易胜博_平赔, 易胜博_负赔, Eurobet_胜赔, Eurobet_平赔, Eurobet_负赔, Interwetten_胜赔, Interwetten_平赔, Interwetten_负赔, 竞彩官方_胜赔, 竞彩官方_平赔, 竞彩官方_负赔, BET10_胜赔, BET10_平赔, BET10_负赔, Bet18_胜赔, Bet18_平赔, Bet18_负赔, bwin_胜赔, bwin_平赔, bwin_负赔, Coral_胜赔, Coral_平赔, Coral_负赔, Expekt_胜赔, Expekt_平赔, Expekt_负赔, IBCBET_胜赔, IBCBET_平赔, IBCBET_负赔, Nike_胜赔, Nike_平赔, Nike_负赔, Nordicbet_胜赔, Nordicbet_平赔, Nordicbet_负赔, PinnacleSports_胜赔, PinnacleSports_平赔, PinnacleSports_负赔, SNAI_胜赔, SNAI_平赔, SNAI_负赔, STS_胜赔, STS_平赔, STS_负赔, TOTO_胜赔, TOTO_平赔, TOTO_负赔, 澳门_胜赔, 澳门_平赔, 澳门_负赔, 金宝博_胜赔, 金宝博_平赔, 金宝博_负赔, 利记_胜赔, 利记_平赔, 利记_负赔, 明陞_胜赔, 明陞_平赔, 明陞_负赔, 香港马会_胜赔, 香港马会_平赔, 香港马会_负赔, Betfair_胜赔, Betfair_平赔, Betfair_负赔, Betsson_胜赔, Betsson_平赔, Betsson_负赔, LEON_胜赔, LEON_平赔, LEON_负赔, Matchbook_胜赔, Matchbook_平赔, Matchbook_负赔, Smarkets_胜赔, Smarkets_平赔, Smarkets_负赔, Dimes_胜赔, Dimes_平赔, Dimes_负赔, Bet3000_胜赔, Bet3000_平赔, Bet3000_负赔, BINGOAL_胜赔, BINGOAL_平赔, BINGOAL_负赔, e_stave_胜赔, e_stave_平赔, e_stave_负赔, GWbet_胜赔, GWbet_平赔, GWbet_负赔, iddaa_胜赔, iddaa_平赔, iddaa_负赔, Jetbull_胜赔, Jetbull_平赔, Jetbull_负赔, Norway_胜赔, Norway_平赔, Norway_负赔, Pamestihima_胜赔, Pamestihima_平赔, Pamestihima_负赔, Singbet_胜赔, Singbet_平赔, Singbet_负赔) VALUES ('澳洲甲', '19-01-01', '0-2', '西悉尼流浪者', '墨尔本城', '2.8', '3.25', '2.5', '2.87', '3.1', '2.45', '2.7', '3.1', '2.45', '2.71', '3.14', '2.42', '2.8', '3.3', '2.55', '2.6', '3.4', '2.37', '2.8', '3.15', '2.45', '2.6', '3.1', '2.55', '2.43', '3.15', '2.5', '2.5', '3.55', '2.5', '2.85', '3.15', '2.45', '2.55', '3.2', '2.65', '2.7', '3.1', '2.45', '2.54', '3.55', '2.54', '2.7', '3.1', '2.46', '2.58', '3.02', '2.56', '2.75', '3.2', '2.5', '2.91', '3.25', '2.67', '2.75', '3.2', '2.45', '2.73', '3.2', '2.45', '2.7', '3.15', '2.42', '2.47', '3.13', '2.62', '2.77', '3.4', '2.35', '2.83', '3.15', '2.44', '2.79', '3.1', '2.38', '2.35', '3.2', '2.6', '2.52', '3.1', '2.56', '2.55', '3.2', '2.7', '2.63', '3.27', '2.67', '2.75', '3.28', '2.82', '2.62', '3.1', '2.7', '2.49', '3.03', '2.57', '2.7', '3.2', '2.6', '2.7', '3.2', '2.5', '2.55', '3.05', '2.6', '2.65', '3.1', '2.4', '2.4', '2.7', '2.4', '2.7', '3.2', '2.4', '2.3', '2.95', '2.5', '2.85', '3.3', '2.55', '2.59', '3.4', '2.49');"""
            # print(sql)
            try:
                cur.execute(sql)
            except Exception as e:
                print(e)
                logger.error(str(e))
            conn.commit()
            conn.close()


if __name__ == '__main__':
    SPIDER = Spider()
    SPIDER.code = 0
    SPIDER.start_task('')
    import time
    print('程序在30秒后自动退出')
    time.sleep(30)