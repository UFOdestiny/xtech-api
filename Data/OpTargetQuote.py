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
    def __init__(self):
        # self.code = normalize_code(self.code_pre)
        self.code = ['510050.XSHG', '510300.XSHG', '159919.XSHE', '510500.XSHG', '159915.XSHE', '159901.XSHE',
                     '159922.XSHE', '000852.XSHG', '000016.XSHG', '000300.XSHG', ]

        self.df = None

    def get_data(self, start, end):
        df = get_price(security=self.code, start_date=start, end_date=end, fq='pre', frequency='minute',
                       fields=['close', 'pre_close'], panel=False)
        if len(df) == 0:
            return

        start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        df["time"] -= pandas.Timedelta(minutes=1)
        df = df[(df["time"] >= start) & (df["time"] <= end)]
        return df

    def process_df(self, df):
        df["pct"] = (df["close"] - df["pre_close"]) / df["pre_close"]
        del df["pre_close"]

        print(len(df))
        if df is not None and len(df) > 0:
            self.df = pandas.concat([self.df, df])

    def get(self, **kwargs):
        times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=7)
        for t in times:
            print(t)
            df = self.get_data(t[0], t[1])
            if df is not None:
                self.process_df(df)

        self.df["time"] = pandas.DatetimeIndex(self.df["time"], tz='Asia/Shanghai')
        self.df.set_index("time", inplace=True)
        self.df.rename(columns={'code': 'targetcode', "close": 'price'}, inplace=True)
        tag_columns = ['targetcode']
        # pandas.set_option('display.max_rows', None)
        # print(self.df)

        return self.df, tag_columns


if __name__ == "__main__":
    op = OpTargetQuote()
    start = "2023-02-10 00:00:00"
    end = "2023-02-14 00:00:00"
    a = op.get(start=start, end=end)
