# -*- coding: utf-8 -*-
# @Name     : OpContractQuote.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime
import math

import pandas
from jqdatasdk import get_ticks, opt, query, get_price

from JoinQuant import Authentication
from utils.Greeks import Greeks


class OpContractQuote(metaclass=Authentication):

    def __init__(self):

        self.code = []
        self.today = str(datetime.date.today())
        self.df = None
        self.result = []
        self.final_result = None
        self.code = None

        self.underlying_symbol = None

        self.his_vol = None
        self.price_days = None
        self.constant = None
        self.g = Greeks()

        self.df_final = None

    def get_underlying_symbol(self, code='10004496.XSHG'):
        q = query(opt.OPT_CONTRACT_INFO.underlying_symbol).filter(opt.OPT_CONTRACT_INFO.code == code)
        df = opt.run_query(q)
        self.underlying_symbol = df["underlying_symbol"][0]

    def get_S(self):
        df = get_price(self.underlying_symbol,
                       fields=['close'],
                       frequency='daily',
                       start_date='2022-07-10 00:00:00',
                       end_date='2022-11-08 23:00:00', )

        close = df["close"].to_list()
        length = len(close)
        rts = [0]
        avg = [0.0] * 21
        for i in range(1, length):
            rts.append(math.log(close[i] / close[i - 1]))

        avg_total = sum(rts[1:]) / (length - 1)

        avg0 = sum(rts[1:21]) / 20
        avg.append(avg0)

        for i in range(22, length):
            avg_i = avg[-1] + (rts[i - 1] - rts[i - 21]) / 20
            avg.append(avg_i)

        variance = [0.0] * 21
        for i in range(21, length):
            v = 0

            for j in range(i - 20, i):
                v += (avg[j] - avg_total) ** 2

            v = (v * 252 / 20) ** 0.5

            variance.append(v)

        # df["rts"] = rts
        # df["avg"] = avg

        df["his_vol"] = variance
        # pandas.set_option('display.max_rows', None)
        # print(df)

        del close
        self.his_vol = df

        # print(df)

    def get_exercise_price(self, code='10004496.XSHG'):
        q = query(opt.OPT_CONTRACT_INFO.date,
                  # opt.OPT_CONTRACT_INFO.code,
                  opt.OPT_CONTRACT_INFO.exercise_price,
                  opt.OPT_CONTRACT_INFO.expire_date, ).filter(
            opt.OPT_CONTRACT_INFO.code == code,
            opt.OPT_CONTRACT_INFO.date >= '2022-07-10 00:00:00',
            opt.OPT_CONTRACT_INFO.date <= '2022-11-08 23:00:00')

        df = opt.run_query(q)
        df['days'] = df["expire_date"] - df["date"]
        df['days'] = df['days'].apply(lambda x: x.days / 365)
        del df["expire_date"]
        df.set_index("date", inplace=True)
        self.price_days = df

    def process_constant(self):
        df = self.his_vol.join(self.price_days, how="inner")
        df.drop(df[df["his_vol"] == 0].index, inplace=True)
        self.constant = df

    def get_data(self, code='10004496.XSHG', start='2022-07-10 00:00:00', end='2022-11-08 23:00:00'):
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
        # print(['time', 'current', 'volume', 'money', "a1_v", "a1_p", "b1_v", "b1_p"])
        self.result = self.df.values.tolist()

        start_time = self.result[0][0]
        end_time = self.result[-1][0]
        pandas.date_range(start=start_time.replace(second=0, microsecond=0),
                          end=end_time.replace(second=0, microsecond=0),
                          freq='1Min')

        time_series1_am = pandas.date_range(start=start_time.replace(hour=9, minute=30, second=0, microsecond=0),
                                            end=start_time.replace(hour=11, minute=30, second=0, microsecond=0),
                                            freq='1Min')

        time_series1_pm = pandas.date_range(start=start_time.replace(hour=13, minute=0, second=0, microsecond=0),
                                            end=start_time.replace(hour=15, minute=0, second=0, microsecond=0),
                                            freq='1Min')

        time_series = time_series1_am.append(time_series1_pm)
        oneday = time_series.copy()

        days_offset = {
            (i[0].replace(hour=0, minute=0, second=0, microsecond=0) - start_time.replace(hour=0, minute=0, second=0,
                                                                                          microsecond=0)).days for i in
            self.result}

        for i in days_offset:
            if i != 0:
                time_series = time_series.append(oneday + datetime.timedelta(days=i))

        # time open close high low vol oi amount
        final_result = [[i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] for i in time_series]

        # pct 每天变化

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

                amount = self.result[down][3]
                # oi+=self.result[down][]
                vol = self.result[down][2]

                a1_v = self.result[down][4]

                b1_v = self.result[down][6]

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
                # 12 pct
                if i >= 1 and final_result[i - 1][2] != 0:
                    final_result[i][12] = (final_result[i][2] - final_result[i - 1][2]) / final_result[i - 1][2]
                else:
                    final_result[i][12] = 0

            else:

                final_result[i][1] = final_result[i - 1][1]
                final_result[i][2] = final_result[i - 1][2]
                final_result[i][3] = final_result[i - 1][3]
                final_result[i][4] = final_result[i - 1][4]

                final_result[i][12] = final_result[i - 1][12]

                if self.result[down - 1][0].day == this_minute.day:

                    final_result[i][5] = final_result[i - 1][5]
                    final_result[i][6] = final_result[i - 1][6]

                    final_result[i][7] = 0
                    final_result[i][8] = final_result[i - 1][8]
                    final_result[i][9] = final_result[i - 1][9]
                    final_result[i][10] = final_result[i - 1][10]
                    final_result[i][11] = final_result[i - 1][11]


                else:
                    final_result[i][5] = amount
                    final_result[i][6] = vol

                    final_result[i][7] = 0
                    final_result[i][8] = a1_v
                    final_result[i][9] = self.result[down - 1][5]
                    final_result[i][10] = b1_v
                    final_result[i][11] = self.result[down - 1][7]

            up = down

        self.final_result = final_result
        self.df_final = pandas.DataFrame(self.final_result,
                                         columns=['time', 'open', 'close', 'high', 'low', 'amount', 'vol', 'oi', 'a1_v',
                                                  'a1_p',
                                                  'b1_v', 'b1_p', 'pct'])

    def greek(self):
        self.df_final.drop(self.df_final[self.df_final["close"] == 0].index, inplace=True)
        self.df_final["exercise_price"] = 0
        self.df_final["days"] = 0
        self.df_final["his_vol"] = 0

        self.df_final["delta"] = 0
        self.df_final["gamma"] = 0
        self.df_final["vega"] = 0
        self.df_final["theta"] = 0

        days = self.constant.index
        for i in range(len(days)):
            day = days[i]
            index = (self.df_final["time"] > day) & (self.df_final["time"] < day + datetime.timedelta(days=1))
            self.df_final.loc[index, "exercise_price"] = self.constant["exercise_price"][i]
            self.df_final.loc[index, "days"] = self.constant["days"][i]
            self.df_final.loc[index, "his_vol"] = self.constant["his_vol"][i]

        self.df_final["delta"] = self.g.delta(self.df_final["close"], self.df_final["exercise_price"],
                                              self.df_final["days"], self.df_final["his_vol"])
        self.df_final["gamma"] = self.g.gamma(self.df_final["close"], self.df_final["exercise_price"],
                                              self.df_final["days"], self.df_final["his_vol"])
        self.df_final["vega"] = self.g.vega(self.df_final["close"], self.df_final["exercise_price"],
                                            self.df_final["days"], self.df_final["his_vol"])
        self.df_final["theta"] = self.g.theta(self.df_final["close"], self.df_final["exercise_price"],
                                              self.df_final["days"], self.df_final["his_vol"])

    def write_excel(self):
        filename = self.code.replace(".", "")
        writer = pandas.ExcelWriter(filename + ".xlsx")

        self.df_final.to_excel(writer, sheet_name='minute', index=False)

        df2 = pandas.DataFrame(self.result,
                               columns=['time', 'current', 'volume', 'money', "a1_v", "a1_p", "b1_v", "b1_p"])

        df2.to_excel(writer, sheet_name='tick', index=False)

        writer.save()

    def get(self):
        self.get_underlying_symbol()
        self.get_S()
        self.get_exercise_price()
        self.process_constant()

        self.get_data()
        self.process_df()
        self.greek()

        self.write_excel()


if __name__ == "__main__":
    opc = OpContractQuote()
    opc.get()
