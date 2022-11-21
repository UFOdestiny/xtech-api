# -*- coding: utf-8 -*-
# @Name     : OpTargetQuote.py
# @Date     : 2022/9/9 9:51
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pandas
from jqdatasdk import get_price, normalize_code

from Data.JoinQuant import Authentication


class OpTargetQuote(metaclass=Authentication):
    code_pre = [510050, 510300, 159919, ]

    # 0:time 1:code 2:close 3:pct

    def __init__(self):
        self.code = normalize_code(self.code_pre)
        self.df = None
        self.result = []

    def get_data(self, start, end):
        self.df = get_price(security=self.code, start_date=start, end_date=end, fq='pre', frequency='minute',
                            fields=['close', 'pre_close'], panel=False)

    def process_df(self):
        self.df["time"] -= pandas.Timedelta(minutes=1)
        self.df["time"] = pandas.to_datetime(self.df["time"]).values.astype(object)
        self.df["pct"] = (self.df["close"] - self.df["pre_close"]) / self.df["pre_close"]
        del self.df["pre_close"]
        self.result = self.df.values.tolist()

    def get(self, **kwargs):
        self.get_data(kwargs["start"], kwargs["end"])
        self.process_df()
        return self.result


if __name__ == "__main__":
    op = OpTargetQuote()
    a = op.get(start='2022-11-01 00:00:00', end='2022-11-30 00:00:00')
