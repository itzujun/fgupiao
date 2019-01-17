# _*_ coding:utf-8 _*_

"""
分布式运算 抓取股票详情
"""

__time__ = "2019.01.15"
__author__ = "open_china"

import os
import sys
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from celery import group

from wtask import down


class GupiaoSpider(object):
    def __init__(self):
        self.baseurl = "http://quote.eastmoney.com/stocklist.html"
        self.Data = []
        self.Date = time.strftime('%Y%m%d')
        self.Recordpath = '.\\pythonData\\'
        self.filename = 'Data' + self.Date
        self.limit = 800  # 设置开启N个线程
        self.session = requests.Session()
        self.timeout = 100
        if not os.path.exists(self.Recordpath):
            os.makedirs(self.Recordpath)

    def getTotalUrl(self):
        try:
            req = self.session.get(self.baseurl, timeout=self.timeout)
            if int(req.status_code) != 200:
                return None
            req.encoding = "gbk"
            lis = BeautifulSoup(req.text, 'lxml').select("div.quotebody li")
            data_lis = []
            for msg in lis:
                cuturl = msg.a["href"].split("/")[-1].replace(".html", "")
                names = msg.text.split("(")
                name = names[0]
                code = names[1].replace(")", "")
                if not (cuturl.startswith("sz300") or cuturl.startswith("sh002")):
                    continue
                add = {"url": cuturl, "name": name, "code": code}
                data_lis.append(add)
            return data_lis
        except Exception as e:
            print(sys._getframe().f_lineno, e)
            return None

    def download(self, tups):
        print("start to down...")
        lis = list(tups)
        col = int(np.floor(len(lis) / self.limit))
        downlis = np.array(lis[0:col * self.limit]).reshape(col, self.limit).tolist()
        if col * self.limit < len(lis):
            downlis.append(lis[col * self.limit:])
        print(downlis)
        for urls in downlis:
            print("len:>>>>>>>>>>>>.", len(urls))
            g = group(down.s(parms["url"], parms["name"], parms["code"]) for parms in urls).apply_async()
            print(">>>>>>>>>>>>... :", len(g))
            for a in g:
                results = a.get()
                if results is not None: self.Data.append(results)
                print(">>>>>  ", results)
            print("批量下载结束...")
        self.save()

    def save(self):
        df = pd.DataFrame(self.Data)
        df.to_excel(self.Recordpath + self.filename + '.xls', index=False)  # 未排名
        df["涨幅"] = df["涨幅"].apply(lambda x: float(str(x).replace("%", "")))
        df = df.sort_values(by=["涨幅"], ascending=[False])
        df["涨幅"] = df["涨幅"].apply(lambda x: str(x) + "%")
        df.to_excel(self.Recordpath + self.filename + '排名.xls', index=False)
        print("保存文件成功：", self.Recordpath)


if __name__ == "__main__":
    t0 = time.time()
    spider = GupiaoSpider()
    urllis = spider.getTotalUrl()
    if urllis is not None:
        spider.download(urllis)
    t1 = time.time()
    print("used: ", str(t1 - t0))
    pass
