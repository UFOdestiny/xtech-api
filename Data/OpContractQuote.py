# -*- coding: utf-8 -*-
# @Name     : OpContractQuote.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime
import time

import pandas
from jqdatasdk import get_ticks

from JoinQuant import Authentication


class OpContractQuote(metaclass=Authentication):

    def __init__(self):
        self.code = []
        self.today = str(datetime.date.today())
        self.df = None
        self.result = []
        self.final_result = None
        self.code = None

    def get_data(self, code='10004496.XSHG', start='2022-08-01 00:00:00', end='2022-10-01 00:00:00'):
        df = get_ticks(code, start_dt=start, end_dt=end,
                       fields=['time', 'current', 'volume', 'money', "a1_v", "a1_p", "b1_v",
                               "b1_p"])
        self.code = code
        self.df = df
        # writer = pandas.ExcelWriter("2022-10-13 510050XSHG.xlsx")  # 初始化一个writer
        # df.to_excel(writer, float_format='%.5f')  # table输出为excel, 传入writer
        # writer.save()
        # pandas.set_option('display.max_columns', None)

    def process_df(self):
        print(['time', 'current', 'volume', 'money', "a1_v", "a1_p", "b1_v", "b1_p"])
        self.result = self.df.values.tolist()

        start_time = self.result[0][0]
        end_time = self.result[-1][0]
        time_series = pandas.date_range(start=start_time.replace(second=0),
                                        end=end_time.replace(second=0),
                                        freq='1Min')
        # time open close high low vol oi amount
        final_result = [[i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ] for i in time_series]

        up = 0
        down = 0
        len_self_result = len(self.result)

        for i in range(len(final_result)):  # len(final_result)
            this_minute = final_result[i][0]

            high_p = 0
            min_p = float("inf")

            amount = 0  # 成交额
            oi = 0  # 持仓量
            vol = 0  # 成交量

            a1_v = 0
            b1_v = 0

            while down < len_self_result and self.result[down][0] - this_minute < datetime.timedelta(minutes=1):
                current = self.result[down][1]

                high_p = max(high_p, current)
                min_p = min(min_p, current)

                amount += self.result[down][3]
                # oi+=self.result[down][]
                vol += self.result[down][2]

                a1_v += self.result[down][4]

                b1_v += self.result[down][6]

                down += 1

            if up != down:
                # 1 open
                final_result[i][1] = self.result[up][1]
                # 2 close
                final_result[i][2] = self.result[down - 1][1]
                # 3 high
                final_result[i][3] = high_p
                # 4 low
                final_result[i][4] = min_p
                # 5 amount
                final_result[i][5] = amount
                # 6 vol
                final_result[i][6] = vol
                # 7 oi
                final_result[i][7] = 0
                # 8 a1_v
                final_result[i][8] = a1_v
                # 9 a1_p
                final_result[i][9] = self.result[down - 1][5]
                # 10 b1_v
                final_result[i][10] = b1_v
                # 11 b1_p
                final_result[i][11] = self.result[down - 1][7]
            else:
                final_result[i][1] = final_result[i - 1][1]
                final_result[i][2] = final_result[i - 1][2]
                final_result[i][3] = final_result[i - 1][3]
                final_result[i][4] = final_result[i - 1][4]

                final_result[i][5] = 0
                final_result[i][6] = 0

                final_result[i][7] = 0
                final_result[i][8] = 0
                final_result[i][9] = final_result[i - 1][9]
                final_result[i][10] = 0
                final_result[i][11] = final_result[i - 1][11]

            up = down

        self.final_result = final_result

    def write_excel(self):
        writer = pandas.ExcelWriter(self.code.replace(".", ""))
        df1 = pandas.DataFrame(self.final_result,
                               columns=['time', 'open', 'close', 'high', 'low', 'amount', 'vol', 'oi', 'a1_v', 'a1_p',
                                        'b1_v', 'b1_p'])

        df1.to_excel(writer, sheet_name='sheet1', index=False)

        df2 = pandas.DataFrame(self.result,
                               columns=['time', 'current', 'volume', 'money', "a1_v", "a1_p", "b1_v", "b1_p"])

        df2.to_excel(writer, sheet_name='sheet2', index=False)
        writer.save()

    def get(self):
        self.get_data()
        self.process_df()

        self.write_excel()


if __name__ == "__main__":
    opc = OpContractQuote()
    opc.get()
