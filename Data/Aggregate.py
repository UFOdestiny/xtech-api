# -*- coding: utf-8 -*-
# @Name     : Aggregate.py
# @Date     : 2022/11/18 16:33
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pandas
from Data.WriteData import Write
from Data.OpContractInfo import OpContractInfo
from Data.OpContractQuote import OpContractQuote


class Aggregate:
    def __init__(self, start='2022-11-01 00:00:00', end='2022-11-30 23:00:00'):
        self.start = start
        self.end = end
        self.info = OpContractInfo()
        self.quote = OpContractQuote()

        self.code_expire = set()
        self.time_range = None

    def split_time(self):
        time_series = pandas.date_range(start=self.start, end=self.end, freq='7d')
        time_series = time_series
        time_range = []
        for i in range(len(time_series) - 1):
            start = f"{time_series[i]} 00:00:00"
            end = f"{time_series[i + 1]} 00:00:00"
            time_range.append((start, end))
        self.time_range = time_range

    def get_code_expire(self):
        for times in self.time_range:
            code = self.info.get_code_expire(times[0], times[1])
            self.code_expire = self.code_expire | set(code)
        self.code_expire = list(self.code_expire)
        # print(self.code_expire)

    def get_contractquote(self):
        for i in self.code_expire:
            w = Write()
            w(source="OpContractQuote", start=self.start, end=i[1].strftime('%Y-%m-%d'), code=i[0])
            print(i[0])

    def run(self):
        self.split_time()
        self.get_code_expire()
        self.get_contractquote()


if __name__ == "__main__":
    a = Aggregate()
    a.run()
