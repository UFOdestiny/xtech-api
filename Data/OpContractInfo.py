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
        self.df = None
        self.result = []

    @staticmethod
    def get_data(start, end):
        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.exercise_price,
                  opt.OPT_DAILY_PREOPEN.contract_type,
                  opt.OPT_DAILY_PREOPEN.contract_unit,
                  opt.OPT_DAILY_PREOPEN.expire_date, ).filter(opt.OPT_DAILY_PREOPEN.date >= start,
                                                              opt.OPT_DAILY_PREOPEN.date <= end)

        df = opt.run_query(q)
        return df

    def process_df(self, df):
        df['days'] = (df["expire_date"] - df["date"]).apply(lambda x: x.days)
        del df["expire_date"]
        if df is not None and len(df) > 0:
            self.df = pandas.concat([self.df, df])
        print(len(df))

    def get(self, **kwargs):
        times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=7)
        for t in times:
            print(t)
            df = self.get_data(t[0], t[1])
            self.process_df(df)

        self.df["date"] = pandas.DatetimeIndex(self.df["date"], tz='Asia/Shanghai')
        self.df.set_index(["date"], inplace=True)

        # ['code', 'underlying_symbol', 'exercise_price', 'contract_type','contract_unit', 'days']

        self.df.rename(columns={'code': 'opcode', "underlying_symbol": "targetcode", "contract_type": "type",
                                "contract_unit": "multiplier", "exercise_price": "strikeprice"}, inplace=True)

        tag_columns = ['opcode', 'targetcode', 'type']

        return self.df, tag_columns


if __name__ == "__main__":
    opc = OpContractInfo()
    opc.get(start='2023-02-10 00:00:00', end='2023-02-14 00:00:00')
