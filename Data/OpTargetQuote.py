# -*- coding: utf-8 -*-
# @Name     : OpTargetQuote.py
# @Date     : 2022/9/9 9:51
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
from datetime import datetime

import pandas
from jqdatasdk import get_price  # ,normalize_code

from utils.InfluxTime import SplitTime
from utils.JoinQuant import Authentication


class OpTargetQuote(metaclass=Authentication):
    code_pre = ['510050', '510300', '159919', '510500', '159915', '159901', '159922', '000852', '000016', '000300']

    # 0:time 1:code 2:close 3:pct

    def __init__(self):
        # self.code = normalize_code(self.code_pre)
        self.code = ['510050.XSHG', '510300.XSHG', '159919.XSHE', '510500.XSHG', '159915.XSHE', '159901.XSHE',
                     '159922.XSHE', '000852.XSHE', '000016.XSHE', '000300.XSHG', ]

        # print(self.code)
        self.df = None
        self.result = []
        self.final_results = []

    def get_data(self, start, end):
        self.df = get_price(security=self.code, start_date=start, end_date=end, fq='pre', frequency='minute',
                            fields=['close', 'pre_close'], panel=False)

        start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        self.df = self.df[(self.df["time"] >= start) & (self.df["time"] <= end)]

    def process_df(self):
        self.df["time"] -= pandas.Timedelta(minutes=1)

        self.df["time"] = pandas.to_datetime(self.df["time"]).values.astype(object)
        self.df["pct"] = (self.df["close"] - self.df["pre_close"]) / self.df["pre_close"]
        del self.df["pre_close"]

        self.result = self.df.values.tolist()
        print(len(self.result))

    def get(self, **kwargs):
        times = SplitTime().split(kwargs["start"], kwargs["end"], interval_day=7)
        for t in times:
            print(t)
            self.get_data(t[0], t[1])
            if len(self.df) != 0:
                self.process_df()
                self.final_results.extend(self.result)

        print(len(self.final_results))
        return self.result


if __name__ == "__main__":
    op = OpTargetQuote()
    a = op.get(start='2023-02-10 23:00:00', end='2023-02-10 23:01:00')
