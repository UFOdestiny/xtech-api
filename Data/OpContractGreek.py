# -*- coding: utf-8 -*-
# @Name     : OpContractGreek.py
# @Date     : 2022/11/5 12:35
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 希腊数字计算

import datetime
import time

import pandas
from jqdatasdk import get_ticks, query, opt

from JoinQuant import Authentication


class OpContractGreek(metaclass=Authentication):

    def __init__(self):
        self.code = []
        self.today = str(datetime.date.today())
        self.df = None
        self.result = []
        self.code = None

    def get_data(self, code='10004496.XSHG', start='2022-08-01 00:00:00', end='2022-11-01 23:00:00'):
        q = query(opt.OPT_RISK_INDICATOR.date,
                  opt.OPT_RISK_INDICATOR.code,
                  opt.OPT_RISK_INDICATOR.delta,
                  opt.OPT_RISK_INDICATOR.theta,
                  opt.OPT_RISK_INDICATOR.gamma,
                  opt.OPT_RISK_INDICATOR.vega,
                  opt.OPT_RISK_INDICATOR.rho,

                  ).filter(opt.OPT_RISK_INDICATOR.code == code,
                           opt.OPT_RISK_INDICATOR.date >= start,
                           opt.OPT_RISK_INDICATOR.date <= end)

        df = opt.run_query(q)
        self.code = code
        self.df = df

    def process_df(self):
        self.result = self.df.values.tolist()

        # for i in range(len(self.result)):
        #     origin_time1 = time.mktime(self.result[i][0].timetuple())
        #     self.result[i][0] = f"{origin_time1 * 1e9:.0f}"

    def write_excel(self):
        filename = self.code.replace(".", "")
        writer = pandas.ExcelWriter(filename + ".xlsx")

        df1 = pandas.DataFrame(self.result,
                               columns=['date', 'code', 'delta', 'theta', 'gamma', 'vega', 'rho'])

        df1.to_excel(writer, sheet_name='greeks', index=False)

        writer.save()

    def get(self):
        self.get_data()
        self.process_df()

        self.write_excel()


if __name__ == "__main__":
    opc = OpContractGreek()
    opc.get()
