import requests
import re
import json
import pandas as pd
import time
import datetime


class GuPiao():
    def __init__(self):
        self.date = time.strftime('%Y%m%d')
        self.url = "http://55.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406092635132097686_1628574993000&pn={page}&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152"

    def get_data(self, page):
        requests.Session()
        resp = requests.get(self.url.format(page=page))
        text = resp.text

        start = resp.text.find('(')
        end = resp.text.find(')')
        new_text = (text[start + 1: end])
        inp_dict = json.loads(new_text)

        length = (len(inp_dict['data']['diff']))
        temp_li = []
        data = inp_dict['data']['diff']
        for i in range(length):
            temp = {}
            if str(data[i]["f3"]).__contains__("-") | str(data[i]["f17"]).__contains__("-"):
                continue
            temp["名称"] = data[i]['f14']
            temp["代码"] = data[i]['f12']
            temp["收盘价"] = data[i]['f2']
            temp["成交量"] = data[i]['f5']
            temp["振幅"] = data[i]['f7']
            temp["最高"] = data[i]['f15']
            temp["最低"] = data[i]['f16']
            temp["开盘价"] = data[i]['f17']
            temp["涨幅"] = data[i]['f3']
            temp_li.append(temp)
        return temp_li


if __name__ == "__main__":
    gu = GuPiao()
    li = []
    for i in range(180):
        temp_li = gu.get_data(i + 1)
        li = li + temp_li
        print("正在下载 {page} 数据 ...".format(page=len(li)))
        pass
    df = pd.DataFrame(li)
    df.to_excel("./files/" + gu.date + '排名.xlsx', index=False)
    print("save success")
