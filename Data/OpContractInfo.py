# -*- coding: utf-8 -*-
# @Name     : OpContractInfo.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 合约信息

import pandas
from jqdatasdk import opt, query

from utils.InfluxTime import SplitTime
from utils.JoinQuant import Authentication


class OpContractInfo(metaclass=Authentication):
    def __init__(self):
        self.underlying_symbol = None
        self.code = []
        self.df = None
        self.result = []

        self.final_results = []

    def get_data(self, start, end):
        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.exercise_price,
                  opt.OPT_DAILY_PREOPEN.contract_type,
                  opt.OPT_DAILY_PREOPEN.contract_unit,
                  opt.OPT_DAILY_PREOPEN.expire_date, ).filter(opt.OPT_DAILY_PREOPEN.date >= start,
                                                              opt.OPT_DAILY_PREOPEN.date <= end)

        self.df = opt.run_query(q)
        # print(self.df.drop_duplicates(subset="code"))
        # pandas.set_option('display.max_columns', None)
        # writer = pandas.ExcelWriter("2022-10-13.xlsx")  # 初始化一个writer
        # df.to_excel(writer, float_format='%.5f')  # table输出为excel, 传入writer
        # writer.save()

    def get_code_expire(self, start, end):
        self.get_data(start, end)
        self.df.drop_duplicates(subset=["code", "expire_date"], inplace=True)
        # self.df.drop(["date", "underlying_symbol", "exercise_price", "contract_type", "contract_unit"],
        #              axis=1, inplace=True)

        # self.underlying_symbol = list(self.df["underlying_symbol"].unique())
        # self.code = list(self.df["code"].unique())
        # return self.df.values.tolist()

        return zip(self.df["code"], self.df["expire_date"])

    def process_df(self):
        self.df['days'] = (self.df["expire_date"] - self.df["date"]).apply(lambda x: x.days)
        self.df["date"] = pandas.to_datetime(self.df["date"]).values.astype(object)
        del self.df["expire_date"]
        self.result = self.df.values.tolist()

        print(len(self.result))

    def get(self, **kwargs):
        times = SplitTime().split(kwargs["start"], kwargs["end"], interval_day=7)

        for t in times:
            print(t)
            self.get_data(t[0], t[1])
            self.process_df()
            self.final_results.extend(self.result)

        return self.final_results


if __name__ == "__main__":
    opc = OpContractInfo()
    opc.get(start='2019-01-01 00:00:00', end='2020-01-30 00:00:00')
