# -*- coding: utf-8 -*-
# @Name     : OpContractQuote.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime

import pandas
from jqdatasdk import opt, query, get_price

from JoinQuant import Authentication


class OpContractQuote(metaclass=Authentication):

    def __init__(self):
        self.code = []
        self.today = str(datetime.date.today())
        self.df = None
        self.result = []

    def one_code(self):
        df = get_price('510050.XSHG', start_date='2022-10-13 00:00:00', end_date='2022-10-13 20:00:00',
                       frequency='minute',
                       fields=['open', 'close', 'low', 'high', 'volume', 'money', "pre_close"])

        # df= get_ticks('510050.XSHG', start_dt='2022-10-13 00:00:00', end_dt='2022-10-13 20:00:00',
        #                fields=['time','current','high', 'low', 'volume', 'money',"a1_v","a1_p","b1_v","b1_p"])
        #
        # writer = pandas.ExcelWriter("2022-10-13 510050XSHG.xlsx")  # 初始化一个writer
        # df.to_excel(writer, float_format='%.5f')  # table输出为excel, 传入writer
        # writer.save()
        pandas.set_option('display.max_columns', None)
        print(df)

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
        self.all_code()
        # self.one_code()
        # print(self.df)

        # self.process_df()
        # return self.result


if __name__ == "__main__":
    opc = OpContractQuote()
    opc.get()
