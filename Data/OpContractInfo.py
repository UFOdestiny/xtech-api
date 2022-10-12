# -*- coding: utf-8 -*-
# @Name     : OpContractInfo.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime

from jqdatasdk import opt, query

from JoinQuant import Authentication


# xshg 1-4496
class OpContractInfo(metaclass=Authentication):
    code_pre = ['510050.XSHG', '510300.XSHG', '159919.XSHE', ]

    def __init__(self):
        # self.code = normalize_code(self.code_pre)
        self.code = self.code_pre
        self.today = str(datetime.date.today())

        self.df = None
        # datetime targetcode price pct

        self.result = []

    def all_code(self):
        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.contract_unit,
                  opt.OPT_DAILY_PREOPEN.exercise_price,
                  opt.OPT_DAILY_PREOPEN.contract_type,
                  opt.OPT_DAILY_PREOPEN.last_trade_date)  # .filter(opt.OPT_DAILY_PREOPEN.code == '10001313.XSHG')
        self.df = opt.run_query(q)

    def one_code(self):
        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.contract_unit,
                  opt.OPT_DAILY_PREOPEN.exercise_price,
                  opt.OPT_DAILY_PREOPEN.contract_type,
                  opt.OPT_DAILY_PREOPEN.last_trade_date).filter(opt.OPT_DAILY_PREOPEN.date == self.today)
        # .filter(opt.OPT_DAILY_PREOPEN.list_date <= self.today,opt.OPT_DAILY_PREOPEN.expire_date >= self.today)
        df = opt.run_query(q)
        return df

    def process_df(self):
        self.result = self.df.values.tolist()

        for i in range(len(self.result)):
            close = self.result[i][-2]
            pre_close = self.result[i][-1]
            pct = (close - pre_close) / pre_close

            self.result[i].append(pct)

            origin_time = datetime.timestamp(self.result[i][0])
            # time_ = InfluxTime.to_influx_time(origin_time)
            self.result[i][0] = f"{origin_time * 1e9:.0f}"

    def get(self):
        print(self.one_code())

        # print(self.df)

        # self.process_df()
        # return self.result


if __name__ == "__main__":
    opc = OpContractInfo()
    a = opc.get()
