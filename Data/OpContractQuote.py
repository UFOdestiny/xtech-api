# -*- coding: utf-8 -*-
# @Name     : OpContractQuote.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime

import pandas
from jqdatasdk import get_price, get_ticks

from JoinQuant import Authentication


class OpContractQuote(metaclass=Authentication):

    def __init__(self):
        self.code = []
        self.today = str(datetime.date.today())
        self.df = None
        self.result = []

    def get_data(self, code='510050.XSHG', start='2022-10-13 00:00:00', end='2022-10-14 20:00:00'):
        # df = get_price(code, start_date=start, end_date=end,
        #                frequency='minute',
        #                fields=['open', 'close', 'low', 'high', 'volume', 'money', "pre_close"])

        df = get_ticks(code, start_dt=start, end_dt=end,
                       fields=['time', 'current', 'high', 'low', 'volume', 'money', "a1_v", "a1_p", "b1_v",
                               "b1_p"])

        self.df = df
        # writer = pandas.ExcelWriter("2022-10-13 510050XSHG.xlsx")  # 初始化一个writer
        # df.to_excel(writer, float_format='%.5f')  # table输出为excel, 传入writer
        # writer.save()
        # pandas.set_option('display.max_columns', None)

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
        self.get_data()

        self.process_df()

        return self.result


if __name__ == "__main__":
    opc = OpContractQuote()
    opc.get()
