# !/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author   : wuda
# @Project  : ershi
# @Software : PyCharm
# @File     : Start_Program_win007.py
# @Time     : 2019/1/22 14:32

# !/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2018/11/8/008 22:12
# @Author  : wuda
# @FileName: main_file.py
# @Software: PyCharm
import re
import os
import sys
import pymysql
import threading
from Main_win0168 import Ui_MainWindow
from PyQt5.QtWidgets import QMainWindow, QApplication, QMessageBox, QButtonGroup, QHeaderView
from PyQt5.QtGui import QStandardItemModel, QStandardItem
from PyQt5.QtCore import QDateTime, Qt
from win0168_spider import Spider

MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_DB = 'win0168'
MYSQL_USER = 'root'
MYSQL_PWD = 'root'

CHAR_SET = 'utf8'
TABLE = 'win0168_data'

title = ['公司', '初_主队', '初_盘口', '初_客队', '初_总计_场', '初_总计_主', '初_总计_和', '初_总计_客', '初_五大联赛_场',
         '初_五大联赛_主', '初_五大联赛_和', '初_五大联赛_客', '初_以甲_场', '初_以甲_主', '初_以甲_和', '初_以甲_客',
         '初_场次', '初_主胜', '初_和局', '初_客胜', '初_历史', '即_主队', '即_盘口', '即_客队', '即_总计_场', '即_总计_主',
         '即_总计_和', '即_总计_客', '即_五大联赛_场', '即_五大联赛_主', '即_五大联赛_和', '即_五大联赛_客', '即_以甲_场',
         '即_以甲_主', '即_以甲_和', '即_以甲_客', '即_场次', '即_主胜', '即_和局', '即_客胜', '即_历史']


title_cz = ['公司', '总计_场', '总计_主', '总计_和', '总计_客',
            '主胜', '和局', '客胜', '历史']
title_cw = ['公司', '五大联赛_场', '五大联赛_主', '五大联赛_和', '五大联赛_客',
            '主胜', '和局', '客胜', '历史']
title_cy = ['公司', '以甲_场', '以甲_主', '以甲_和', '以甲_客',
            '主胜', '和局', '客胜', '历史']
title_jz = ['公司', '总计_场', '总计_主', '总计_和', '总计_客',
            '主胜', '和局', '客胜', '历史']
title_jw = ['公司', '总计_场', '五大联赛_场', '五大联赛_主', '五大联赛_和',
         '五大联赛_客', '主胜', '和局', '客胜', '历史']
title_jy = ['公司',  '以甲_场', '以甲_主', '以甲_和', '以甲_客',
            '主胜', '和局', '客胜', '历史']

field = "公司,,初_总计_场,初_总计_主,初_总计_和,初_总计_客,初_五大联赛_场,初_五大联赛_主,初_五大联赛_和,初_五大联赛_客,初_以甲_场,初_以甲_主,初_以甲_和,初_以甲_客,初_场次,初_主胜,初_和局,初_客胜,初_历史,,即_总计_场,即_总计_主,即_总计_和,即_总计_客,即_五大联赛_场,即_五大联赛_主,即_五大联赛_和,即_五大联赛_客,即_以甲_场,即_以甲_主,即_以甲_和,即_以甲_客,即_场次,即_主胜,即_和局,即_客胜,即_历史"
field_cz = "公司,初_总计_场,初_总计_主,初_总计_和,初_总计_客,初_主胜_总,初_和局_总,初_客胜_总,初_历史_总"
field_cw = "公司,初_五大联赛_场,初_五大联赛_主,初_五大联赛_和,初_五大联赛_客,初_主胜_五,初_和局_五,初_客胜_五,初_历史_五"
field_cy = "公司,初_以甲_场,初_以甲_主,初_以甲_和,初_以甲_客,初_主胜_比,初_和局_比,初_客胜_比,初_历史_比"
field_jz = "公司,即_总计_场,即_总计_主,即_总计_和,即_总计_客,即_主胜_总,即_和局_总,即_客胜_总,即_历史_总"
field_jw = "公司,即_五大联赛_场,即_五大联赛_主,即_五大联赛_和,即_五大联赛_客,即_主胜_五,即_和局_五,即_客胜_五"
field_jy = "公司,即_以甲_场,即_以甲_主,即_以甲_和,即_以甲_客,即_主胜_比,即_和局_比,即_客胜_比,即_历史_比"

class Supervise_Main(QMainWindow, Ui_MainWindow):

    def __init__(self, parent=None):
        super(Supervise_Main, self).__init__(parent)
        self.setupUi(self)
        self.model = QStandardItemModel(0, 11)
        self.model.setHorizontalHeaderLabels(['赛事', '时间', '主队', '客队', '全', '半', '初主平', '初和平', '初客平', '即主平', '即和平', '即客平'])
        self.model1 = QStandardItemModel(0, 9)
        self.model1.setHorizontalHeaderLabels(title_cz)
        self.init_ui()

    def init_ui(self):
        self.search_result_team.setModel(self.model)
        self.search_result_team.setColumnWidth(0, 55)
        self.search_result_team.setColumnWidth(1, 55)
        self.search_result_team.setColumnWidth(4, 26)
        self.search_result_team.setColumnWidth(5, 26)
        self.search_result_team.setColumnWidth(6, 43)
        self.search_result_team.setColumnWidth(7, 43)
        self.search_result_team.setColumnWidth(8, 43)
        self.search_result_team.setColumnWidth(9, 43)
        self.search_result_team.setColumnWidth(10, 43)
        self.search_result_team.setColumnWidth(11, 43)
        # self.search_result_team.horizontalHeader().setStretchLastSection(True)
        # self.search_result_team.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.search_result_team.setColumnWidth(0, 60)
        self.search_result_data.setModel(self.model1)

        self.search_result_data.horizontalHeader().setStretchLastSection(True)
        # self.search_result_data.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.search_result_data.setColumnWidth(0, 120)
        self.search_result_data.setColumnWidth(1, 60)
        self.search_result_data.setColumnWidth(2, 60)
        self.search_result_data.setColumnWidth(3, 60)
        self.search_result_data.setColumnWidth(4, 60)
        self.search_result_data.setColumnWidth(5, 60)
        self.search_result_data.setColumnWidth(6, 60)
        self.search_result_data.setColumnWidth(7, 60)
        self.search_result_data.setColumnWidth(8, 60)
        self.search_result_data.setColumnWidth(9, 65)
        self.search_result_data.setColumnWidth(10, 65)
        self.search_result_data.setColumnWidth(11, 65)
        self.button_group = QButtonGroup()
        self.button_group.addButton(self.radioButton_by_month)
        self.button_group.addButton(self.radioButton_by_day)

        self.radioButton_by_day.setChecked(True)
        # self.month_comboBox.setEnabled(False)

        self.spider = Spider(self)
        self.spider.logsignal.connect(self.spider_log)
        self.start_spider_btn.clicked.connect(self.start_spider)

        self.start_search.clicked.connect(self.search_date)
        self.c_z.clicked.connect(self.choose_match_cz)
        self.c_w.clicked.connect(self.choose_match_cw)
        self.c_y.clicked.connect(self.choose_match_cy)
        self.j_z.clicked.connect(self.choose_match_jz)
        self.j_w.clicked.connect(self.choose_match_jw)
        self.j_y.clicked.connect(self.choose_match_jy)

    def choose_match_cz(self):
        self.model1.clear()
        self.model1.setHorizontalHeaderLabels(title_cz)

        self.search_result_data.horizontalHeader().setStretchLastSection(True)
        # self.search_result_data.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.search_result_data.setColumnWidth(0, 120)
        self.search_result_data.setColumnWidth(1, 60)
        self.search_result_data.setColumnWidth(2, 60)
        self.search_result_data.setColumnWidth(3, 60)
        self.search_result_data.setColumnWidth(4, 60)
        self.search_result_data.setColumnWidth(5, 60)
        self.search_result_data.setColumnWidth(6, 60)
        self.search_result_data.setColumnWidth(7, 60)
        self.search_result_data.setColumnWidth(8, 60)
        self.label_6.setText(str('<html><head/><body><p align="center"><span style=" font-size:12pt; font-weight:600; color:#00aa00;">初始总计数据</span></p></body></html>'))
        self.c_home_result.clear()
        self.c_hanld_result.clear()
        self.c_away_result.clear()
        self.j_home_result.clear()
        self.j_hanld_result.clear()
        self.j_away_result.clear()
        try:
            row = self.search_result_team.currentIndex().row()
            # column = self.search_result_team.currentIndex().column()
            season = self.model.data(self.model.index(row, 0))
            match_time = self.search_date_1 + ' ' + self.model.data(self.model.index(row, 1))
            # match_time = self.model.data(self.model.index(row, 1))
            home_team = self.model.data(self.model.index(row, 2))
            away_team = self.model.data(self.model.index(row, 3))
            self.home_team.setText(str('<html><head/><body><p align="right"><span style=" font-weight:600; color:#ff5500;">{0}</span></p></body></html>'.format(home_team)))
            self.away_team.setText(str('<html><head/><body><p><span style=" font-weight:600; color:#5555ff;">{0}</span></p></body></html>'.format(away_team)))
            self.season.setText(str('<html><head/><body><p align="right"><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(season)))
            self.game_time.setText((str('<html><head/><body><p><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(match_time))))
            sql_temp = """SELECT DISTINCT DISTINCT {4} FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            sql = sql_temp.format(season, match_time, home_team, away_team, field_cz)
            print(sql)
            res = self.mysql_conn(sql=sql)
            # print(res)
            length = len(res)
            self.home_team_2.setText(str('<html><head/><body><p><span style=" font-size:10pt; font-weight:600; color:#ff0000;">公司总数：{0}</span></p></body></html>'.format(str(length))))
            for temp in res:
                self.model1.appendRow([QStandardItem(str(i)) for i in temp])

            sql_c_h = """SELECT DISTINCT AVG(初_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_c = """SELECT DISTINCT AVG(初_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_a = """SELECT DISTINCT AVG(初_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            sql_j_h = """SELECT DISTINCT AVG(即_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_c = """SELECT DISTINCT AVG(即_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_a = """SELECT DISTINCT AVG(即_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""


            try:
                c_h = float('%.2f' % float(self.mysql_conn(sql=sql_c_h.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h = 0

            try:
                c_c = float('%.2f' % float(self.mysql_conn(sql=sql_c_c.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c = 0

            try:
                c_a =   float('%.2f' % float(self.mysql_conn(sql=sql_c_a.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a = 0

            try:
                j_h = float('%.2f' % float(self.mysql_conn(sql=sql_j_h.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h = 0

            try:
                j_c = float('%.2f' % float(self.mysql_conn(sql=sql_j_c.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c = 0

            try:
                j_a = float('%.2f' % float(self.mysql_conn(sql=sql_j_a.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a = 0

            sql_c_h_w = """SELECT DISTINCT AVG(初_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_c_w = """SELECT DISTINCT AVG(初_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_a_w = """SELECT DISTINCT AVG(初_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            sql_j_h_w = """SELECT DISTINCT AVG(即_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_c_w = """SELECT DISTINCT AVG(即_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_a_w = """SELECT DISTINCT AVG(即_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            try:
                c_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_w = 0

            try:
                c_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_w = 0

            try:
                c_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_w = 0

            try:
                j_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_w = 0

            try:
                j_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_w = 0

            try:
                j_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_w = 0

            sql_c_h_b = """SELECT DISTINCT AVG(初_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_c_b = """SELECT DISTINCT AVG(初_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_a_b = """SELECT DISTINCT AVG(初_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            sql_j_h_b = """SELECT DISTINCT AVG(即_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_c_b = """SELECT DISTINCT AVG(即_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_a_b = """SELECT DISTINCT AVG(即_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""


            try:
                c_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_b = 0

            try:
                c_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_b = 0

            try:
                c_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_b = 0

            try:
                j_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_b = 0

            try:
                j_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_b = 0

            try:
                j_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_b = 0

            # print(c_h, type(c_h))
            self.c_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h)))
            self.c_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c)))
            self.c_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a)))
            self.j_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h)))
            self.j_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c)))
            self.j_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a)))

            self.c_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_w)))
            self.c_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_w)))
            self.c_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_w)))
            self.j_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_w)))
            self.j_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_w)))
            self.j_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_w)))

            self.c_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_b)))
            self.c_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_b)))
            self.c_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_b)))
            self.j_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_b)))
            self.j_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_b)))
            self.j_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_b)))
        except Exception as e:
            QMessageBox.critical(self, '查看详细数据', '查看详细数据出错：\n错误原因：' + str(e) + '\n检查是否提前查询数据',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return

    def choose_match_cw(self):
        self.model1.clear()
        self.model1.setHorizontalHeaderLabels(title_cw)

        self.search_result_data.horizontalHeader().setStretchLastSection(True)
        # self.search_result_data.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.search_result_data.setColumnWidth(0, 120)
        self.search_result_data.setColumnWidth(1, 80)
        self.search_result_data.setColumnWidth(2, 80)
        self.search_result_data.setColumnWidth(3, 80)
        self.search_result_data.setColumnWidth(4, 80)
        self.search_result_data.setColumnWidth(5, 55)
        self.search_result_data.setColumnWidth(6, 55)
        self.search_result_data.setColumnWidth(7, 55)
        self.search_result_data.setColumnWidth(8, 55)
        self.label_6.setText(str('<html><head/><body><p align="center"><span style=" font-size:12pt; font-weight:600; color:#00aa00;">初始五大联赛数据</span></p></body></html>'))
        self.c_home_result.clear()
        self.c_hanld_result.clear()
        self.c_away_result.clear()
        self.j_home_result.clear()
        self.j_hanld_result.clear()
        self.j_away_result.clear()
        try:
            row = self.search_result_team.currentIndex().row()
            # column = self.search_result_team.currentIndex().column()
            season = self.model.data(self.model.index(row, 0))
            match_time = self.search_date_1 + ' ' + self.model.data(self.model.index(row, 1))
            home_team = self.model.data(self.model.index(row, 2))
            away_team = self.model.data(self.model.index(row, 3))
            self.home_team.setText(str('<html><head/><body><p align="right"><span style=" font-weight:600; color:#ff5500;">{0}</span></p></body></html>'.format(home_team)))
            self.away_team.setText(str('<html><head/><body><p><span style=" font-weight:600; color:#5555ff;">{0}</span></p></body></html>'.format(away_team)))
            self.season.setText(str('<html><head/><body><p align="right"><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(season)))
            self.game_time.setText((str('<html><head/><body><p><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(match_time))))
            sql_temp = """SELECT DISTINCT {4} FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql = sql_temp.format(season, match_time, home_team, away_team, field_cw)
            print(sql)
            res = self.mysql_conn(sql=sql)
            print(res)
            length = len(res)
            self.home_team_2.setText(str('<html><head/><body><p><span style=" font-size:10pt; font-weight:600; color:#ff0000;">公司总数：{0}</span></p></body></html>'.format(str(length))))
            for temp in res:
                self.model1.appendRow([QStandardItem(str(i)) for i in temp])

            sql_c_h = """SELECT DISTINCT AVG(初_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_c = """SELECT DISTINCT AVG(初_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_a = """SELECT DISTINCT AVG(初_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            sql_j_h = """SELECT DISTINCT AVG(即_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_c = """SELECT DISTINCT AVG(即_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_a = """SELECT DISTINCT AVG(即_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            c_h = float('%.2f' % float(self.mysql_conn(sql=sql_c_h.format(season, match_time, home_team, away_team))[0][0]))
            c_c = float('%.2f' % float(self.mysql_conn(sql=sql_c_c.format(season, match_time, home_team, away_team))[0][0]))
            c_a = float('%.2f' % float(self.mysql_conn(sql=sql_c_a.format(season, match_time, home_team, away_team))[0][0]))

            j_h = float('%.2f' % float(self.mysql_conn(sql=sql_j_h.format(season, match_time, home_team, away_team))[0][0]))
            j_c = float('%.2f' % float(self.mysql_conn(sql=sql_j_c.format(season, match_time, home_team, away_team))[0][0]))
            j_a = float('%.2f' % float(self.mysql_conn(sql=sql_j_a.format(season, match_time, home_team, away_team))[0][0]))

            sql_c_h_w = """SELECT DISTINCT AVG(初_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_c_w = """SELECT DISTINCT AVG(初_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_a_w = """SELECT DISTINCT AVG(初_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            sql_j_h_w = """SELECT DISTINCT AVG(即_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_c_w = """SELECT DISTINCT AVG(即_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_a_w = """SELECT DISTINCT AVG(即_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            print(sql_c_h_w.format(season, match_time, home_team, away_team))
            try:
                c_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_w = 0
            try:
                c_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_w = 0
            try:
                c_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_w = 0

            try:
                j_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_w = 0
            try:
                j_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_w = 0
            try:
                j_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_w = 0

            sql_c_h_b = """SELECT DISTINCT AVG(初_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_c_b = """SELECT DISTINCT AVG(初_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_a_b = """SELECT DISTINCT AVG(初_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            sql_j_h_b = """SELECT DISTINCT AVG(即_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_c_b = """SELECT DISTINCT AVG(即_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_a_b = """SELECT DISTINCT AVG(即_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            try:
                c_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_b = 0
            try:
                c_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_b = 0
            try:
                c_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_b = 0

            try:
                j_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_b = 0
            try:
                j_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_b = 0
            try:
                j_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_b = 0

            # print(c_h, type(c_h))
            self.c_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h)))
            self.c_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c)))
            self.c_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a)))
            self.j_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h)))
            self.j_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c)))
            self.j_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a)))

            self.c_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_w)))
            self.c_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_w)))
            self.c_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_w)))
            self.j_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_w)))
            self.j_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_w)))
            self.j_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_w)))

            self.c_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_b)))
            self.c_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_b)))
            self.c_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_b)))
            self.j_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_b)))
            self.j_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_b)))
            self.j_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_b)))
        except Exception as e:
            QMessageBox.critical(self, '查看详细数据', '查看详细数据出错：\n错误原因：' + str(e) + '\n检查是否提前查询数据',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return

    def choose_match_cy(self):
        self.model1.clear()
        self.model1.setHorizontalHeaderLabels(title_cy)

        self.search_result_data.horizontalHeader().setStretchLastSection(True)
        # self.search_result_data.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.search_result_data.setColumnWidth(0, 120)
        self.search_result_data.setColumnWidth(1, 60)
        self.search_result_data.setColumnWidth(2, 60)
        self.search_result_data.setColumnWidth(3, 60)
        self.search_result_data.setColumnWidth(4, 60)
        self.search_result_data.setColumnWidth(5, 60)
        self.search_result_data.setColumnWidth(6, 60)
        self.search_result_data.setColumnWidth(7, 60)
        self.search_result_data.setColumnWidth(8, 60)
        self.label_6.setText(str('<html><head/><body><p align="center"><span style=" font-size:12pt; font-weight:600; color:#00aa00;">初始以甲数据</span></p></body></html>'))
        self.c_home_result.clear()
        self.c_hanld_result.clear()
        self.c_away_result.clear()
        self.j_home_result.clear()
        self.j_hanld_result.clear()
        self.j_away_result.clear()
        try:
            row = self.search_result_team.currentIndex().row()
            # column = self.search_result_team.currentIndex().column()
            season = self.model.data(self.model.index(row, 0))
            match_time = self.search_date_1 + ' ' + self.model.data(self.model.index(row, 1))
            home_team = self.model.data(self.model.index(row, 2))
            away_team = self.model.data(self.model.index(row, 3))
            self.home_team.setText(str('<html><head/><body><p align="right"><span style=" font-weight:600; color:#ff5500;">{0}</span></p></body></html>'.format(home_team)))
            self.away_team.setText(str('<html><head/><body><p><span style=" font-weight:600; color:#5555ff;">{0}</span></p></body></html>'.format(away_team)))
            self.season.setText(str('<html><head/><body><p align="right"><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(season)))
            self.game_time.setText((str('<html><head/><body><p><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(match_time))))
            sql_temp = """SELECT DISTINCT {4} FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql = sql_temp.format(season, match_time, home_team, away_team, field_cy)
            # print(sql)
            res = self.mysql_conn(sql=sql)
            # print(res)
            length = len(res)
            self.home_team_2.setText(str('<html><head/><body><p><span style=" font-size:10pt; font-weight:600; color:#ff0000;">公司总数：{0}</span></p></body></html>'.format(str(length))))
            for temp in res:
                self.model1.appendRow([QStandardItem(str(i)) for i in temp])

            sql_c_h = """SELECT DISTINCT AVG(初_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_c = """SELECT DISTINCT AVG(初_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_a = """SELECT DISTINCT AVG(初_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            sql_j_h = """SELECT DISTINCT AVG(即_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_c = """SELECT DISTINCT AVG(即_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_a = """SELECT DISTINCT AVG(即_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            c_h = float('%.2f' % float(self.mysql_conn(sql=sql_c_h.format(season, match_time, home_team, away_team))[0][0]))
            c_c = float('%.2f' % float(self.mysql_conn(sql=sql_c_c.format(season, match_time, home_team, away_team))[0][0]))
            c_a = float('%.2f' % float(self.mysql_conn(sql=sql_c_a.format(season, match_time, home_team, away_team))[0][0]))

            j_h = float('%.2f' % float(self.mysql_conn(sql=sql_j_h.format(season, match_time, home_team, away_team))[0][0]))
            j_c = float('%.2f' % float(self.mysql_conn(sql=sql_j_c.format(season, match_time, home_team, away_team))[0][0]))
            j_a = float('%.2f' % float(self.mysql_conn(sql=sql_j_a.format(season, match_time, home_team, away_team))[0][0]))

            sql_c_h_w = """SELECT DISTINCT AVG(初_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_c_w = """SELECT DISTINCT AVG(初_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_a_w = """SELECT DISTINCT AVG(初_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            sql_j_h_w = """SELECT DISTINCT AVG(即_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_c_w = """SELECT DISTINCT AVG(即_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_a_w = """SELECT DISTINCT AVG(即_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            try:
                c_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_w = 0
            try:
                c_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_w = 0
            try:
                c_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_w = 0

            try:
                j_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_w = 0
            try:
                j_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_w = 0
            try:
                j_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_w = 0

            sql_c_h_b = """SELECT DISTINCT AVG(初_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_c_b = """SELECT DISTINCT AVG(初_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_a_b = """SELECT DISTINCT AVG(初_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            sql_j_h_b = """SELECT DISTINCT AVG(即_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_c_b = """SELECT DISTINCT AVG(即_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_a_b = """SELECT DISTINCT AVG(即_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            try:
                c_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_b = 0
            try:
                c_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_b = 0
            try:
                c_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_b = 0

            try:
                j_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_b = 0
            try:
                j_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_b = 0
            try:
                j_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_b = 0

            # print(c_h, type(c_h))
            self.c_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h)))
            self.c_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c)))
            self.c_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a)))
            self.j_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h)))
            self.j_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c)))
            self.j_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a)))

            self.c_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_w)))
            self.c_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_w)))
            self.c_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_w)))
            self.j_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_w)))
            self.j_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_w)))
            self.j_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_w)))

            self.c_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_b)))
            self.c_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_b)))
            self.c_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_b)))
            self.j_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_b)))
            self.j_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_b)))
            self.j_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_b)))
        except Exception as e:
            QMessageBox.critical(self, '查看详细数据', '查看详细数据出错：\n错误原因：' + str(e) + '\n检查是否提前查询数据',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return

    def choose_match_jz(self):
        self.model1.clear()
        self.model1.setHorizontalHeaderLabels(title_jz)

        self.search_result_data.horizontalHeader().setStretchLastSection(True)
        # self.search_result_data.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.search_result_data.setColumnWidth(0, 120)
        self.search_result_data.setColumnWidth(1, 60)
        self.search_result_data.setColumnWidth(2, 60)
        self.search_result_data.setColumnWidth(3, 60)
        self.search_result_data.setColumnWidth(4, 60)
        self.search_result_data.setColumnWidth(5, 60)
        self.search_result_data.setColumnWidth(6, 60)
        self.search_result_data.setColumnWidth(7, 60)
        self.search_result_data.setColumnWidth(8, 60)
        self.label_6.setText(str('<html><head/><body><p align="center"><span style=" font-size:12pt; font-weight:600; color:#00aa00;">即时总计数据</span></p></body></html>'))
        self.c_home_result.clear()
        self.c_hanld_result.clear()
        self.c_away_result.clear()
        self.j_home_result.clear()
        self.j_hanld_result.clear()
        self.j_away_result.clear()
        try:
            row = self.search_result_team.currentIndex().row()
            # column = self.search_result_team.currentIndex().column()
            season = self.model.data(self.model.index(row, 0))
            match_time = self.search_date_1 + ' ' + self.model.data(self.model.index(row, 1))
            home_team = self.model.data(self.model.index(row, 2))
            away_team = self.model.data(self.model.index(row, 3))
            self.home_team.setText(str('<html><head/><body><p align="right"><span style=" font-weight:600; color:#ff5500;">{0}</span></p></body></html>'.format(home_team)))
            self.away_team.setText(str('<html><head/><body><p><span style=" font-weight:600; color:#5555ff;">{0}</span></p></body></html>'.format(away_team)))
            self.season.setText(str('<html><head/><body><p align="right"><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(season)))
            self.game_time.setText((str('<html><head/><body><p><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(match_time))))
            sql_temp = """SELECT DISTINCT {4} FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql = sql_temp.format(season, match_time, home_team, away_team, field_jz)
            # print(sql)
            res = self.mysql_conn(sql=sql)
            # print(res)
            length = len(res)
            self.home_team_2.setText(str('<html><head/><body><p><span style=" font-size:10pt; font-weight:600; color:#ff0000;">公司总数：{0}</span></p></body></html>'.format(str(length))))
            for temp in res:
                self.model1.appendRow([QStandardItem(str(i)) for i in temp])

            sql_c_h = """SELECT DISTINCT AVG(初_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_c = """SELECT DISTINCT AVG(初_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_a = """SELECT DISTINCT AVG(初_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            sql_j_h = """SELECT DISTINCT AVG(即_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_c = """SELECT DISTINCT AVG(即_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_a = """SELECT DISTINCT AVG(即_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            c_h = float('%.2f' % float(self.mysql_conn(sql=sql_c_h.format(season, match_time, home_team, away_team))[0][0]))
            c_c = float('%.2f' % float(self.mysql_conn(sql=sql_c_c.format(season, match_time, home_team, away_team))[0][0]))
            c_a = float('%.2f' % float(self.mysql_conn(sql=sql_c_a.format(season, match_time, home_team, away_team))[0][0]))

            j_h = float('%.2f' % float(self.mysql_conn(sql=sql_j_h.format(season, match_time, home_team, away_team))[0][0]))
            j_c = float('%.2f' % float(self.mysql_conn(sql=sql_j_c.format(season, match_time, home_team, away_team))[0][0]))
            j_a = float('%.2f' % float(self.mysql_conn(sql=sql_j_a.format(season, match_time, home_team, away_team))[0][0]))

            sql_c_h_w = """SELECT DISTINCT AVG(初_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_c_w = """SELECT DISTINCT AVG(初_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_a_w = """SELECT DISTINCT AVG(初_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            sql_j_h_w = """SELECT DISTINCT AVG(即_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_c_w = """SELECT DISTINCT AVG(即_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_a_w = """SELECT DISTINCT AVG(即_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            try:
                c_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_w = 0
            try:
                c_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_w = 0
            try:
                c_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_w = 0

            try:
                j_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_w = 0
            try:
                j_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_w = 0
            try:
                j_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_w = 0

            sql_c_h_b = """SELECT DISTINCT AVG(初_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_c_b = """SELECT DISTINCT AVG(初_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_a_b = """SELECT DISTINCT AVG(初_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            sql_j_h_b = """SELECT DISTINCT AVG(即_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_c_b = """SELECT DISTINCT AVG(即_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_a_b = """SELECT DISTINCT AVG(即_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            try:
                c_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_b = 0
            try:
                c_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_b = 0
            try:
                c_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_b = 0

            try:
                j_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_b = 0
            try:
                j_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_b = 0
            try:
                j_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_b = 0

            # print(c_h, type(c_h))
            self.c_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h)))
            self.c_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c)))
            self.c_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a)))
            self.j_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h)))
            self.j_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c)))
            self.j_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a)))

            self.c_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_w)))
            self.c_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_w)))
            self.c_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_w)))
            self.j_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_w)))
            self.j_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_w)))
            self.j_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_w)))

            self.c_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_b)))
            self.c_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_b)))
            self.c_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_b)))
            self.j_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_b)))
            self.j_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_b)))
            self.j_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_b)))
        except Exception as e:
            QMessageBox.critical(self, '查看详细数据', '查看详细数据出错：\n错误原因：' + str(e) + '\n检查是否提前查询数据',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return

    def choose_match_jw(self):
        self.model1.clear()
        self.model1.setHorizontalHeaderLabels(title_jw)

        self.search_result_data.horizontalHeader().setStretchLastSection(True)
        # self.search_result_data.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.search_result_data.setColumnWidth(0, 120)
        self.search_result_data.setColumnWidth(1, 80)
        self.search_result_data.setColumnWidth(2, 80)
        self.search_result_data.setColumnWidth(3, 80)
        self.search_result_data.setColumnWidth(4, 80)
        self.search_result_data.setColumnWidth(5, 55)
        self.search_result_data.setColumnWidth(6, 55)
        self.search_result_data.setColumnWidth(7, 55)
        self.search_result_data.setColumnWidth(8, 55)
        self.label_6.setText(str('<html><head/><body><p align="center"><span style=" font-size:12pt; font-weight:600; color:#00aa00;">即时五大联赛数据</span></p></body></html>'))
        self.c_home_result.clear()
        self.c_hanld_result.clear()
        self.c_away_result.clear()
        self.j_home_result.clear()
        self.j_hanld_result.clear()
        self.j_away_result.clear()
        try:
            row = self.search_result_team.currentIndex().row()
            # column = self.search_result_team.currentIndex().column()
            season = self.model.data(self.model.index(row, 0))
            match_time = self.search_date_1 + ' ' + self.model.data(self.model.index(row, 1))
            home_team = self.model.data(self.model.index(row, 2))
            away_team = self.model.data(self.model.index(row, 3))
            self.home_team.setText(str('<html><head/><body><p align="right"><span style=" font-weight:600; color:#ff5500;">{0}</span></p></body></html>'.format(home_team)))
            self.away_team.setText(str('<html><head/><body><p><span style=" font-weight:600; color:#5555ff;">{0}</span></p></body></html>'.format(away_team)))
            self.season.setText(str('<html><head/><body><p align="right"><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(season)))
            self.game_time.setText((str('<html><head/><body><p><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(match_time))))
            sql_temp = """SELECT DISTINCT {4} FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql = sql_temp.format(season, match_time, home_team, away_team, field_jw)
            # print(sql)
            res = self.mysql_conn(sql=sql)
            print(res)
            length = len(res)
            self.home_team_2.setText(str('<html><head/><body><p><span style=" font-size:10pt; font-weight:600; color:#ff0000;">公司总数：{0}</span></p></body></html>'.format(str(length))))
            for temp in res:
                self.model1.appendRow([QStandardItem(str(i)) for i in temp])

            sql_c_h = """SELECT DISTINCT AVG(初_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_c = """SELECT DISTINCT AVG(初_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_a = """SELECT DISTINCT AVG(初_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            sql_j_h = """SELECT DISTINCT AVG(即_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_c = """SELECT DISTINCT AVG(即_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_a = """SELECT DISTINCT AVG(即_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            c_h = float('%.2f' % float(self.mysql_conn(sql=sql_c_h.format(season, match_time, home_team, away_team))[0][0]))
            c_c = float('%.2f' % float(self.mysql_conn(sql=sql_c_c.format(season, match_time, home_team, away_team))[0][0]))
            c_a = float('%.2f' % float(self.mysql_conn(sql=sql_c_a.format(season, match_time, home_team, away_team))[0][0]))

            j_h = float('%.2f' % float(self.mysql_conn(sql=sql_j_h.format(season, match_time, home_team, away_team))[0][0]))
            j_c = float('%.2f' % float(self.mysql_conn(sql=sql_j_c.format(season, match_time, home_team, away_team))[0][0]))
            j_a = float('%.2f' % float(self.mysql_conn(sql=sql_j_a.format(season, match_time, home_team, away_team))[0][0]))

            sql_c_h_w = """SELECT DISTINCT AVG(初_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_c_w = """SELECT DISTINCT AVG(初_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_a_w = """SELECT DISTINCT AVG(初_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            sql_j_h_w = """SELECT DISTINCT AVG(即_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_c_w = """SELECT DISTINCT AVG(即_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_a_w = """SELECT DISTINCT AVG(即_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            try:
                c_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_w = 0
            try:
                c_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_w = 0
            try:
                c_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_w = 0

            try:
                j_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_w = 0
            try:
                j_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_w = 0
            try:
                j_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_w = 0

            sql_c_h_b = """SELECT DISTINCT AVG(初_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_c_b = """SELECT DISTINCT AVG(初_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_a_b = """SELECT DISTINCT AVG(初_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            sql_j_h_b = """SELECT DISTINCT AVG(即_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_c_b = """SELECT DISTINCT AVG(即_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_a_b = """SELECT DISTINCT AVG(即_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            try:
                c_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_b = 0
            try:
                c_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_b = 0
            try:
                c_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_b = 0

            try:
                j_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_b = 0
            try:
                j_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_b = 0
            try:
                j_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_b = 0

            # print(c_h, type(c_h))
            self.c_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h)))
            self.c_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c)))
            self.c_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a)))
            self.j_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h)))
            self.j_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c)))
            self.j_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a)))

            self.c_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_w)))
            self.c_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_w)))
            self.c_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_w)))
            self.j_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_w)))
            self.j_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_w)))
            self.j_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_w)))

            self.c_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_b)))
            self.c_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_b)))
            self.c_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_b)))
            self.j_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_b)))
            self.j_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_b)))
            self.j_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_b)))
        except Exception as e:
            QMessageBox.critical(self, '查看详细数据', '查看详细数据出错：\n错误原因：' + str(e) + '\n检查是否提前查询数据',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return

    def choose_match_jy(self):
        self.model1.clear()
        self.model1.setHorizontalHeaderLabels(title_jy)

        self.search_result_data.horizontalHeader().setStretchLastSection(True)
        # self.search_result_data.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.search_result_data.setColumnWidth(0, 120)
        self.search_result_data.setColumnWidth(1, 60)
        self.search_result_data.setColumnWidth(2, 60)
        self.search_result_data.setColumnWidth(3, 60)
        self.search_result_data.setColumnWidth(4, 60)
        self.search_result_data.setColumnWidth(5, 60)
        self.search_result_data.setColumnWidth(6, 60)
        self.search_result_data.setColumnWidth(7, 60)
        self.search_result_data.setColumnWidth(8, 60)
        self.label_6.setText(str('<html><head/><body><p align="center"><span style=" font-size:12pt; font-weight:600; color:#00aa00;">即时比赛数据</span></p></body></html>'))

        self.c_home_result.clear()
        self.c_hanld_result.clear()
        self.c_away_result.clear()
        self.j_home_result.clear()
        self.j_hanld_result.clear()
        self.j_away_result.clear()
        try:
            row = self.search_result_team.currentIndex().row()
            # column = self.search_result_team.currentIndex().column()
            season = self.model.data(self.model.index(row, 0))
            match_time = self.search_date_1 + ' ' + self.model.data(self.model.index(row, 1))
            home_team = self.model.data(self.model.index(row, 2))
            away_team = self.model.data(self.model.index(row, 3))
            self.home_team.setText(str('<html><head/><body><p align="right"><span style=" font-weight:600; color:#ff5500;">{0}</span></p></body></html>'.format(home_team)))
            self.away_team.setText(str('<html><head/><body><p><span style=" font-weight:600; color:#5555ff;">{0}</span></p></body></html>'.format(away_team)))
            self.season.setText(str('<html><head/><body><p align="right"><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(season)))
            self.game_time.setText((str('<html><head/><body><p><span style=" font-size:8pt;">{0}</span></p></body></html>'.format(match_time))))
            sql_temp = """SELECT DISTINCT {4} FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql = sql_temp.format(season, match_time, home_team, away_team, field_jy)
            # print(sql)
            res = self.mysql_conn(sql=sql)
            # print(res)
            length = len(res)
            self.home_team_2.setText(str('<html><head/><body><p><span style=" font-size:10pt; font-weight:600; color:#ff0000;">公司总数：{0}</span></p></body></html>'.format(str(length))))
            for temp in res:
                self.model1.appendRow([QStandardItem(str(i)) for i in temp])

            sql_c_h = """SELECT DISTINCT AVG(初_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_c = """SELECT DISTINCT AVG(初_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_c_a = """SELECT DISTINCT AVG(初_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""

            sql_j_h = """SELECT DISTINCT AVG(即_主胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_c = """SELECT DISTINCT AVG(即_和局_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            sql_j_a = """SELECT DISTINCT AVG(即_客胜_总) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_总='1' AND 即_赔率_总='1';"""
            print(sql_c_h.format(season, match_time, home_team, away_team))
            c_h = float('%.2f' % float(self.mysql_conn(sql=sql_c_h.format(season, match_time, home_team, away_team))[0][0]))
            c_c = float('%.2f' % float(self.mysql_conn(sql=sql_c_c.format(season, match_time, home_team, away_team))[0][0]))
            c_a = float('%.2f' % float(self.mysql_conn(sql=sql_c_a.format(season, match_time, home_team, away_team))[0][0]))

            j_h = float('%.2f' % float(self.mysql_conn(sql=sql_j_h.format(season, match_time, home_team, away_team))[0][0]))
            j_c = float('%.2f' % float(self.mysql_conn(sql=sql_j_c.format(season, match_time, home_team, away_team))[0][0]))
            j_a = float('%.2f' % float(self.mysql_conn(sql=sql_j_a.format(season, match_time, home_team, away_team))[0][0]))

            sql_c_h_w = """SELECT DISTINCT AVG(初_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_c_w = """SELECT DISTINCT AVG(初_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_c_a_w = """SELECT DISTINCT AVG(初_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            sql_j_h_w = """SELECT DISTINCT AVG(即_主胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_c_w = """SELECT DISTINCT AVG(即_和局_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""
            sql_j_a_w = """SELECT DISTINCT AVG(即_客胜_五) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_五='1' AND 即_赔率_五='1';"""

            try:
                c_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_w = 0
            try:
                c_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_w = 0
            try:
                c_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_w = 0

            try:
                j_h_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_w = 0
            try:
                j_c_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_w = 0
            try:
                j_a_w = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_w.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_w = 0

            sql_c_h_b = """SELECT DISTINCT AVG(初_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_c_b = """SELECT DISTINCT AVG(初_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_c_a_b = """SELECT DISTINCT AVG(初_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            sql_j_h_b = """SELECT DISTINCT AVG(即_主胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_c_b = """SELECT DISTINCT AVG(即_和局_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""
            sql_j_a_b = """SELECT DISTINCT AVG(即_客胜_比) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' AND 初_赔率_比='1' AND 即_赔率_比='1';"""

            try:
                c_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_h_b = 0
            try:
                c_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_c_b = 0
            try:
                c_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_c_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                c_a_b = 0

            try:
                j_h_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_h_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_h_b = 0
            try:
                j_c_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_c_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_c_b = 0
            try:
                j_a_b = float('%.2f' % float(self.mysql_conn(sql=sql_j_a_b.format(season, match_time, home_team, away_team))[0][0]))
            except Exception:
                j_a_b = 0

            # print(c_h, type(c_h))
            self.c_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h)))
            self.c_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c)))
            self.c_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a)))
            self.j_home_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h)))
            self.j_hanld_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c)))
            self.j_away_result.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a)))

            self.c_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_w)))
            self.c_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_w)))
            self.c_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_w)))
            self.j_home_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_w)))
            self.j_hanld_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_w)))
            self.j_away_result_2.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_w)))

            self.c_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_h_b)))
            self.c_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_c_b)))
            self.c_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(c_a_b)))
            self.j_home_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_h_b)))
            self.j_hanld_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_c_b)))
            self.j_away_result_3.setText(str('<html><head/><body><p align="center">{0}%</p></body></html>'.format(j_a_b)))
        except Exception as e:
            QMessageBox.critical(self, '查看详细数据', '查看详细数据出错：\n错误原因：' + str(e) + '\n检查是否提前查询数据',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return

    def search_date(self):
        self.model.clear()
        self.model.setHorizontalHeaderLabels(['赛事', '时间', '主队', '客队', '全', '半', '初主', '初和', '初客', '即主', '即和', '即客'])
        self.search_result_team.setColumnWidth(0, 55)
        self.search_result_team.setColumnWidth(1, 55)
        self.search_result_team.setColumnWidth(4, 26)
        self.search_result_team.setColumnWidth(5, 26)
        self.search_result_team.setColumnWidth(6, 43)
        self.search_result_team.setColumnWidth(7, 43)
        self.search_result_team.setColumnWidth(8, 43)
        self.search_result_team.setColumnWidth(9, 43)
        self.search_result_team.setColumnWidth(10, 43)
        self.search_result_team.setColumnWidth(11, 43)
        search_date = self.search_time.text()
        if not re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', search_date):
            QMessageBox.critical(self, '搜索时间', '搜索时间格式出错：2000-1-1或2000-01-01，注意不要有空格',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return
        else:
            date_list = search_date.split('-')
            date_list[1] = date_list[1] if len(date_list[1]) == 2 else '0' + date_list[1]
            date_list[2] = date_list[2] if len(date_list[2]) == 2 else '0' + date_list[2]
            search_date_end = '-'.join(date_list)

            date_list_s = date_list
            date_list_s[2] = str(int(date_list_s[2]) + 1) if len(str(int(date_list_s[2]) + 1)) == 2 else '0' + str(int(date_list_s[2]) + 1)

            search_date_str = '-'.join(date_list_s)
            self.search_date_1 = search_date_end
            # sql_temp = """SELECT DISTINCT DISTINCT 赛事类型, 时间, 主队, 客队, 全场比分, 半场比分 FROM win0168_data WHERE 时间 BETWEEN '{0} 12:00:00' AND '{1} 11:59:00' ORDER BY 时间 ASC;""".format(
            #     search_date_end, search_date_str)
            sql_temp = """SELECT DISTINCT 赛事类型, 时间, 主队, 客队, 全场比分, 半场比分 FROM win0168_data WHERE 时间 LIKE '%{0}%' ORDER BY 时间 ASC;""".format(
                search_date_end)
            print(sql_temp)
            res = self.mysql_conn(sql=sql_temp)
            print(res)
            sql_temp_avg = """SELECT DISTINCT 赛事类型, 时间, 主队, 客队, 全场比分, 半场比分, AVG(初_主队), AVG(初_盘口), AVG(初_客队), AVG(即_主队), AVG(即_盘口), AVG(即_客队) FROM win0168_data WHERE 赛事类型='{0}' AND 时间='{1}' AND 主队='{2}' AND 客队='{3}' ORDER BY 时间 ASC;;"""
            for temp in res:
                res_temp = self.mysql_conn(sql=sql_temp_avg.format(temp[0], str(temp[1]), temp[2], temp[3]))
                res_temp_data = res_temp[0]
                res_temp_data = list(res_temp_data)
                res_temp_data[1] = str(res_temp_data[1]).split(' ')[1]
                res_temp_data[6] = float('%.2f' %float(res_temp_data[6]))
                res_temp_data[7] = float('%.2f' %float(res_temp_data[7]))
                res_temp_data[8] = float('%.2f' %float(res_temp_data[8]))
                res_temp_data[9] = float('%.2f' %float(res_temp_data[9]))
                res_temp_data[10] = float('%.2f' %float(res_temp_data[10]))
                res_temp_data[11] = float('%.2f' %float(res_temp_data[11]))
                self.model.appendRow([QStandardItem(str(i)) for i in res_temp_data])

    def spider_log(self, value):
        self.spiderEdit.append(value)

    def start_spider(self):
        if self.radioButton_by_month.isChecked():
            # self.month_comboBox.setEnabled(True)

            year = self.spider_input_Edit.text()
            if not re.match(r'^\d{4}$', year):
                QMessageBox.critical(self, '年份输入', '年份输入格式错误：2000，注意不要有空格',
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                return
            else:
                month = self.month_comboBox.currentText()
                if month == '2':
                    date_list = ['{year}-{month}-{day}'.format(year=year, month=month, day=str(day)) for day in
                                 range(1, 30)]
                elif month in ['1', '3', '5', '7', '8', '10', '12']:
                    date_list = ['{year}-{month}-{day}'.format(year=year, month=month, day=str(day)) for day in
                                 range(1, 32)]
                else:
                    date_list = ['{year}-{month}-{day}'.format(year=year, month=month, day=str(day)) for day in
                                 range(1, 31)]
        elif self.radioButton_by_day.isChecked():
            search_day = self.spider_input_Edit.text()
            if not re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', search_day):
                QMessageBox.critical(self, '搜索日期', '搜索日期格式出错：2000-1-1或者2000-01-01， 注意不要有空格',
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                return
            else:
                date_list = [search_day]

        new_data = 1 if self.checkBox_include_new_data.isChecked() else 0
        self.date_list = date_list
        self.new_data = new_data
        self.run_spider(date_list, new_data)

    def run_spider(self, date_list=None, new_data=None):
        date_list = date_list if date_list else self.date_list
        new_data = new_data if new_data else self.new_data

        self.spider_thread = threading.Thread(
            target=self.spider.start_task,
            args=[{'date_list': date_list, 'update_code': new_data}]
        )
        self.spider_thread.start()
        # if new_data == 1:
        #     threading.Timer(600, self.run_spider)

    def closeEvent(self, event):
        sys.exit(app.exec_())

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
            # print(NameError, 'SQL Server Connect failed!')
            return False
        else:
            cur.execute(sql)
            result = cur.fetchall()
            conn.commit()
            conn.close()
            return result


if __name__ == '__main__':
    app = QApplication(sys.argv)
    supervise = Supervise_Main()
    supervise.show()
    sys.exit(app.exec_())
