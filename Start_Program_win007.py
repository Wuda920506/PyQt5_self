# !/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author   : wuda
# @Project  : ershi
# @Software : PyCharm
# @File     : Start_Program_win007.py
# @Time     : 2019/1/22 14:32

#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2018/11/8/008 22:12
# @Author  : wuda
# @FileName: main_file.py
# @Software: PyCharm
import re
import os
import sys
import json
import ctypes
import inspect
import threading
from Main_win007 import Ui_MainWindow
from PyQt5.QtWidgets import QMainWindow, QApplication, QMessageBox
from PyQt5.QtGui import QStandardItemModel
from PyQt5.QtCore import QDateTime, Qt
from win007_spider import Spider


class Supervise_Main(QMainWindow, Ui_MainWindow):

    def __init__(self, parent=None):
        super(Supervise_Main, self).__init__(parent)
        self.setupUi(self)
        self.init_ui()

    def init_ui(self):
        with open('setting.json', 'r') as f:
            self.set_json = f.read()
            setting = json.loads(self.set_json)
        host = setting['host']
        user = setting['user']
        self.pwd = setting['pwd']
        port = setting['port']
        self.port = port
        database = setting['database']
        self.table = setting['table']
        self.hostedit.setText(host)
        self.useredit.setText(user)
        self.pwdedit.setText('自己去配置文件看')
        self.ipedit.setText(port)
        self.dbedit.setText(database)
        self.hostedit.setEnabled(False)
        self.useredit.setEnabled(False)
        self.pwdedit.setEnabled(False)
        self.ipedit.setEnabled(False)
        self.dbedit.setEnabled(False)
        self.rememberdata.setEnabled(False)
        self.sureupdate.setEnabled(False)
        self.starttime.setEnabled(False)
        self.endtime.setEnabled(False)
        self.startcrawl.setEnabled(False)
        self.changedata.stateChanged.connect(self.change)
        self.sureupdate.clicked.connect(self.update_setting)
        self.checkBox_crawl.stateChanged.connect(self.crawl_change)
        self.startcrawl.clicked.connect(self.crawlself)
        self.crawlall.clicked.connect(self.crawl)
        self.exit.clicked.connect(self.quit)
        self.spider = Spider(self)
        self.spider.logsignal.connect(self.logshow)
        self.spider.endsignal.connect(self.endshow)

    def endshow(self, value):
        self.label_8.clear()
        self.label_8.setText(value)

    def logshow(self, value):
        self.textEdit.append(value)

    def crawlself(self):
        start_date = self.starttime.text()
        if not re.match(r'^\d{4}-\d{2}-\d{2}', start_date):
            QMessageBox.critical(self, '开始时间', '开始时间格式错误：2000-01-01',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return
        end_date = self.endtime.text()
        if not re.match(r'^\d{4}-\d{2}-\d{2}', end_date):
            QMessageBox.critical(self, '结束时间', '结束时间格式错误：2000-01-01',
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            return
        host = self.hostedit.text()
        user = self.useredit.text()
        database = self.dbedit.text()
        send_data = {
            "start_date": start_date,
            "end_date": end_date,
            "conn": {
                "host": host,
                "port": self.port,
                "user": user,
                "pwd": self.pwd,
                "database": database,
                "table": self.table
            },
            "code": True
        }
        self.thread1 = threading.Thread(target=self.spider.start_task, args=[json.dumps(send_data)])
        self.thread1.start()

    def crawl(self):
        host = self.hostedit.text()
        user = self.useredit.text()
        database = self.dbedit.text()
        send_data = {
            "start_date": "",
            "end_date": "",
            "conn": {
                "host": host,
                "port": self.port,
                "user": user,
                "pwd": self.pwd,
                "database": database,
                "table": self.table
            },
            "code": False
        }
        self.thread2 = threading.Thread(target=self.spider.start_task, args=[json.dumps(send_data)])
        self.thread2.start()

    def update_setting(self):
        if self.rememberdata.isChecked():
            host = self.useredit.text()
            port = self.ipedit.text()
            if port == 920506:
                self.pwdedit.setText(self.pwd)
            user = self.useredit.text()
            database = self.dbedit.text()
            set_json = json.loads(self.set_json)
            set_json['host'] = host
            set_json['port'] = port
            set_json['user'] = user
            set_json['database'] = database
            save_json = json.dumps(set_json)
            with open('setting.json', 'w') as f:
                f.write(save_json)
            self.textEdit.append('出于安全考虑，如果想修改密码，请去配置文件修改')
        else:
            port = self.ipedit.text()
            if port == '920506':
                self.pwdedit.setText(self.pwd)

    def change(self, state):
        if state == Qt.Checked:
            self.hostedit.setEnabled(True)
            self.useredit.setEnabled(True)
            self.pwdedit.setEnabled(True)
            self.ipedit.setEnabled(True)
            self.dbedit.setEnabled(True)
            self.sureupdate.setEnabled(True)
            self.rememberdata.setEnabled(True)
        else:
            self.hostedit.setEnabled(False)
            self.useredit.setEnabled(False)
            self.pwdedit.setEnabled(False)
            self.ipedit.setEnabled(False)
            self.dbedit.setEnabled(False)
            self.sureupdate.setEnabled(False)
            self.rememberdata.setEnabled(False)

    def crawl_change(self, state):
        if state == Qt.Checked:
            self.starttime.setEnabled(True)
            self.endtime.setEnabled(True)
            self.startcrawl.setEnabled(True)
        else:
            self.starttime.setEnabled(False)
            self.endtime.setEnabled(False)
            self.startcrawl.setEnabled(False)

    def quit(self):
        try:
            if self.thread1.isAlive():
                self._async_raise(self.thread1.ident, SystemExit)
        except:
            pass
        try:
            if self.thread2.isAlive():
                self._async_raise(self.thread2.ident, SystemExit)
        except:
            pass
        sys.exit()

    def _async_raise(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")
        return


if __name__ == '__main__':
    app = QApplication(sys.argv)
    supervise = Supervise_Main()
    supervise.show()
    sys.exit(app.exec_())


