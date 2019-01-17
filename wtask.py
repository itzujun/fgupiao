# _*_ coding:utf-8_*_

""""
分布式爬虫
2019.01.15
"""

__author = "liuzujun"

import json
import time

import requests
from celery import Celery
import celery

app = Celery('wtask', backend='redis://127.0.0.1:6379/0', broker='redis://127.0.0.1:6379/0')
# app = Celery('wtask', backend='redis://192.168.3.2:6379/0', broker='redis://192.168.3.2:6379/0')

timeout = 10


class CountTask(celery.Task):
    count = 0

    def on_success(self, retval, task_id, args, kwargs):
        self.count = self.count + 1
        # 执行成功，运行该函数
        print("on_success>>>>>>>>>>>>>>>>>>>>>>>>>>..." + str(self.count))
        return self.count

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> on error...")


@app.task(bind=True)
def test_mes(self):
    for i in range(1, 11):
        time.sleep(0.1)
        print("update_state>>>>>>>>>>>>>>>>>>>>>>>>")
        self.update_state(state="PROGRESS", meta={'p': i * 10})
    return 'finish'


@app.task(base=CountTask)
def hello():
    return "hello world"


@app.task
def down(url, name, code):
    record_d = {}
    record_d["名称"] = name
    record_d["代码"] = code
    linkurl = "https://gupiao.baidu.com/api/stocks/stockdaybar?from=pc&os_ver=1&cuid=xxx&vv=100&format=json&stock_code=" + \
              url + "&step=3&start=&count=160&fq_type=no&timestamp=" + str(int(time.time()))
    try:
        resp = requests.get(linkurl, timeout=timeout).content
        js = json.loads(resp)
        lis = js.get("mashData", "-")
        msg = lis[0].get("kline")
        record_d["涨幅"] = str(format(float(msg.get("netChangeRatio", "-")), ".2f")) + "%"
        record_d["开盘"] = msg.get("open", "-")
        record_d["最高"] = msg.get("high", "-")
        record_d["最低"] = msg.get("low", "-")
        record_d["收盘"] = msg.get("close", "-")
        record_d["成交量"] = msg.get("volume", "-")
        record_d["昨收"] = msg.get("preClose", "-")
        record_d["收盘"] = msg.get("close", "-")
        print("完成数据:  " + name, code)
        return record_d
    except Exception as e:
        print(e, name, code)
        return None
    pass
