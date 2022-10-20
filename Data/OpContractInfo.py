# -*- coding: utf-8 -*-
# @Name     : OpContractInfo.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime
import time

from jqdatasdk import opt, query

from JoinQuant import Authentication


class OpContractInfo(metaclass=Authentication):
    def __init__(self):
        self.code = []
        self.today = str(datetime.date.today())
        self.df = None
        self.result = []

    def get_data(self, start=None, end=None):
        if not start and not end:
            start = self.today
            end = self.today

        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.underlying_name,
                  opt.OPT_DAILY_PREOPEN.exercise_price,
                  opt.OPT_DAILY_PREOPEN.contract_type,
                  opt.OPT_DAILY_PREOPEN.contract_unit,
                  opt.OPT_DAILY_PREOPEN.list_date,
                  opt.OPT_DAILY_PREOPEN.expire_date, ).filter(opt.OPT_DAILY_PREOPEN.date >= start,
                                                              opt.OPT_DAILY_PREOPEN.date <= end)

        self.df = opt.run_query(q)
        self.df['days'] = self.df["expire_date"] - self.df["date"]

        self.code = list(self.df["underlying_symbol"].unique())

        # pandas.set_option('display.max_rows', None)

        # writer = pandas.ExcelWriter("2022-10-13.xlsx")  # 初始化一个writer
        # df.to_excel(writer, float_format='%.5f')  # table输出为excel, 传入writer
        # writer.save()

    def process_df(self):
        self.result = self.df.values.tolist()

        for i in range(len(self.result)):
            origin_time1 = time.mktime(self.result[i][0].timetuple())
            self.result[i][0] = f"{origin_time1 * 1e9:.0f}"

            origin_time2 = time.mktime(self.result[i][7].timetuple())
            self.result[i][7] = f"{origin_time2 * 1e9:.0f}"

            origin_time3 = time.mktime(self.result[i][8].timetuple())
            self.result[i][8] = f"{origin_time3 * 1e9:.0f}"

            self.result[i][-1] = self.result[i][-1].days

    def get(self, start=None, end=None):

        self.get_data(start, end)
        self.process_df()
        return self.result


if __name__ == "__main__":
    opc = OpContractInfo()
    opc.get(start='2022-10-01 00:00:00', end='2022-10-17 23:00:00')
