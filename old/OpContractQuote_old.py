# -*- coding: utf-8 -*-
# @Name     : OpContractQuote.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime
import math

import numpy as np
import pandas
from jqdatasdk import get_ticks, opt, query, get_price

from Data.JoinQuant import Authentication
from utils.GreeksIV import Greeks, ImpliedVolatility


class OpContractQuote(metaclass=Authentication):

    def __init__(self):
        self.symbol_minute = None
        self.today = str(datetime.date.today())
        self.tick = None
        self.result = []
        self.final_result = None
        self.code = None
        self.underlying_symbol = None
        self.his_vol = None
        self.pre_open = None
        self.constant = None
        self.g = Greeks()
        self.iv = ImpliedVolatility()
        self.code_minute = None

    def daily_info(self, code, start_date, end_date):
        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.exercise_price,
                  opt.OPT_DAILY_PREOPEN.expire_date,
                  opt.OPT_DAILY_PREOPEN.contract_type, ).filter(
            opt.OPT_DAILY_PREOPEN.code == code,
            opt.OPT_DAILY_PREOPEN.date >= start_date,
            opt.OPT_DAILY_PREOPEN.date <= end_date)
        self.pre_open = opt.run_query(q)

        self.pre_open['days'] = (self.pre_open["expire_date"] - self.pre_open["date"]).apply(lambda x: x.days / 365)
        self.pre_open.set_index("date", inplace=True)

        self.code = code
        self.underlying_symbol = self.pre_open["underlying_symbol"][0]
        self.pre_open.drop(["expire_date", "underlying_symbol", "code"], axis=1, inplace=True)

    def get_his_vol(self, start_date, end_date):
        """
        根据期权代码获取日频的历史波动率
        :param start_date:
        :param end_date:
        :return:
        """

        b = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=40)
        start_date = str(b)

        """
        # 1、获取制定时间段内的日频价格
        """
        df = get_price(self.underlying_symbol,
                       fields=['close'],
                       frequency='daily',
                       start_date=start_date,
                       end_date=end_date, )

        close = df["close"].to_list()

        """
        # 2、计算收益率，收益率=ln(后一天的价格/前一天的价格)
        """
        length = len(close)
        rts = [0]
        for i in range(1, length):
            rts.append(math.log(close[i] / close[i - 1]))

        """
        # 3、计算某一天的前20天的平均收益率
        """
        sum(rts[1:]) / (length - 1)
        avg = [0.0] * 21
        avg0 = sum(rts[1:21]) / 20
        avg.append(avg0)

        for i in range(22, length):
            avg_i = avg[-1] + (rts[i - 1] - rts[i - 21]) / 20
            avg.append(avg_i)

        """
        # 4、计算某一天的前20天的历史收益率
        """

        variance = [0.0] * 21
        for i in range(21, length):
            v = 0
            # 方差
            for j in range(i - 20, i):
                v += (rts[j] - avg[i]) ** 2
            # 年化，乘根号252
            v = (v * 252 / 20) ** 0.5

            variance.append(v)

        df["his_vol"] = variance
        del df["close"]
        self.his_vol = df

    def process_constant(self):
        """
        聚合历史波动率和行权价，到期日
        :return:
        """
        self.constant = self.his_vol.join(self.pre_open, how="inner")
        self.constant.drop(self.constant[self.constant["his_vol"] == 0].index, inplace=True)
        del self.pre_open
        del self.his_vol

        self.constant.index += pandas.Timedelta(hours=9, minutes=30)
        self.constant["contract_type"].replace("CO", 1, inplace=True)
        self.constant["contract_type"].replace("PO", -1, inplace=True)

    def get_underlying_symbol_price(self, start_date, end_date):
        """
        根据期权代码获取日频的历史波动率
        :param start_date:
        :param end_date:
        :return:
        """
        self.symbol_minute = get_price(self.underlying_symbol,
                                       fields=['close'],
                                       frequency='minute',
                                       start_date=start_date,
                                       end_date=end_date, )

        self.symbol_minute.columns = ["symbol_minute"]
        self.symbol_minute.index -= pandas.Timedelta(minutes=1)

    def get_minute_price(self, code, start, end):
        self.code_minute = get_price(code, frequency='minute', start_date=start, end_date=end)
        self.code_minute.index -= pandas.Timedelta(minutes=1)
        self.code_minute["pct"] = np.log(self.code_minute["close"].div(self.code_minute["close"].shift()))
        self.code_minute["pct"][0] = 0

    def get_tick(self, code, start_date, end_date):
        """
        获取tick行情
        :param code:
        :param start_date:
        :param end_date:
        :return:
        """
        self.tick = get_ticks(code, start_dt=start_date, end_dt=end_date,
                              fields=['time', "a1_p", "a1_v", "b1_p", "b1_v"])  # 'current', 'volume', 'money',
        self.tick.set_index('time', inplace=True)

        self.code_minute[["a1_p", "b1_p"]] = self.tick[["a1_p", "b1_p"]].resample(rule='1Min').last()
        self.code_minute[["a1_v", "b1_v"]] = self.tick[["a1_v", "b1_v"]].resample(rule='1Min').sum()

        df = self.code_minute[["a1_p", "b1_p", "a1_v", "b1_v"]].replace(np.float64(0), np.nan)
        df.fillna(method='ffill', inplace=True)
        self.code_minute[["a1_p", "b1_p", "a1_v", "b1_v"]] = df

        del self.tick
        del df

    def process_df(self):
        self.code_minute["symbol_price"] = self.symbol_minute
        del self.symbol_minute
        self.code_minute["code"] = self.code
        self.code_minute["underlying_symbol"] = self.underlying_symbol

        self.code_minute[["his_vol", "exercise_price", "contract_type", "days"]] = self.constant

        df = self.code_minute[["his_vol", "exercise_price", "contract_type", "days"]].copy()
        df.fillna(method='ffill', inplace=True)
        self.code_minute[["his_vol", "exercise_price", "contract_type", "days"]] = df

    def process_df2(self):
        """
        处理tick行情，输出分钟频次的数据
        :return:
        """

        # 转为list，提高处理速度
        self.result = self.tick.values.tolist()
        # 取出开始时间与结束时间
        start_time = self.result[0][0]
        end_time = self.result[-1][0]

        # 构造从开始时间到结束时间的分钟频次时间序列，其中时间是交易日9:30-11:30,13:00-15:00，除去周末
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

        # 构造结果合集 time open close high low vol oi amount
        final_result = [[i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] for i in time_series]

        # 双指针
        up = 0  # 上指针
        down = 0  # 下指针

        len_self_result = len(self.result)

        """
        # 核心部分：迭代计算
        # 迭代构造好的分钟频时间序列（此时数据为空，开始填充的是0）
        """
        for i in range(len(final_result)):
            # 取出当前分钟
            this_minute = final_result[i][0]

            high_p = 0  # 最高价格
            min_p = float("inf")  # 最低价格

            amount = 0  # 成交额
            oi = 0  # 持仓量
            vol = 0  # 成交量

            a1_v = 0
            b1_v = 0

            """
            # 1、当下指针不越界，并且还在当前分钟时，进入循环
            """
            while down < len_self_result and self.result[down][0] - this_minute < datetime.timedelta(minutes=1):
                current = self.result[down][1]

                # 迭代寻找最大最小值
                high_p = max(high_p, current)
                min_p = min(min_p, current)

                # 这四个数以最后的一个tick为准
                amount = self.result[down][3]
                vol = self.result[down][2]
                a1_v = self.result[down][4]
                b1_v = self.result[down][6]  # oi+=self.result[down][]

                # 下指针移动
                down += 1

            """
            # 2、如果上下指针不相同，即这一分钟内有数据时进入执行
            """
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

            else:  # 如果上下指针相同，即这一分钟没有数据时进入执行，沿用上一分钟的数据
                final_result[i][1] = final_result[i - 1][1]
                final_result[i][2] = final_result[i - 1][2]
                final_result[i][3] = final_result[i - 1][3]
                final_result[i][4] = final_result[i - 1][4]

                final_result[i][12] = final_result[i - 1][12]

                # 如果下指针与该分钟在同一天
                if self.result[down - 1][0].day == this_minute.day:
                    final_result[i][5] = final_result[i - 1][5]
                    final_result[i][6] = final_result[i - 1][6]

                    final_result[i][7] = 0
                    final_result[i][8] = final_result[i - 1][8]
                    final_result[i][9] = final_result[i - 1][9]
                    final_result[i][10] = final_result[i - 1][10]
                    final_result[i][11] = final_result[i - 1][11]

                # 如果下指针与该分钟不在同一天
                else:
                    final_result[i][5] = amount
                    final_result[i][6] = vol

                    final_result[i][7] = 0
                    final_result[i][8] = a1_v
                    final_result[i][9] = self.result[down - 1][5]
                    final_result[i][10] = b1_v
                    final_result[i][11] = self.result[down - 1][7]

            # 处理完这一分钟后，指针归位，从下一分钟开始
            up = down

        self.final_result = final_result
        self.code_minute = pandas.DataFrame(self.final_result,
                                            columns=['time', 'open', 'close', 'high', 'low', 'amount', 'vol', 'oi',
                                                     'a1_v',
                                                     'a1_p', 'b1_v', 'b1_p', 'pct'])

    def greekiv(self):

        self.code_minute["delta"] = self.g.delta(self.code_minute["symbol_price"], self.code_minute["exercise_price"],
                                                 self.code_minute["days"], self.code_minute["his_vol"],
                                                 self.code_minute["contract_type"])
        self.code_minute["gamma"] = self.g.gamma(self.code_minute["symbol_price"], self.code_minute["exercise_price"],
                                                 self.code_minute["days"], self.code_minute["his_vol"])
        self.code_minute["vega"] = self.g.vega(self.code_minute["symbol_price"], self.code_minute["exercise_price"],
                                               self.code_minute["days"], self.code_minute["his_vol"])
        self.code_minute["theta"] = self.g.theta(self.code_minute["symbol_price"], self.code_minute["exercise_price"],
                                                 self.code_minute["days"], self.code_minute["his_vol"],
                                                 self.code_minute["contract_type"])
        self.code_minute["iv"] = 0

        self.code_minute["iv"] = self.code_minute.apply(
            lambda x: self.iv.find_vol(x["close"], x["contract_type"], x["symbol_price"], x["exercise_price"],
                                       x["days"]), axis=1)

        self.code_minute["timevalue"] = self.code_minute.apply(
            lambda x: max(0, x["close"] - x["contract_type"] * (x["symbol_price"] - x["exercise_price"])), axis=1)

        self.code_minute.drop(["symbol_price", "exercise_price", "days", "his_vol", "contract_type"],
                              axis=1, inplace=True)

    def write_excel(self):
        """
        将数据写入xlsx，表1为最终结果，表2为tick原始数据
        :return:
        """
        filename = self.code.replace(".", "")
        writer = pandas.ExcelWriter(filename + ".xlsx")

        self.code_minute.to_excel(writer, sheet_name='minute', index=False)

        # df2 = pandas.DataFrame(self.result,
        #                        columns=['time', 'current', 'volume', 'money', "a1_v", "a1_p", "b1_v", "b1_p"])
        #
        # df2.to_excel(writer, sheet_name='tick', index=False)

        writer.save()

    def get(self, **kwargs):
        """
        按流程执行
        :return:
        """
        code = kwargs["code"]
        start = kwargs["start"]
        end = kwargs["end"]

        self.daily_info(code, start, end)
        self.get_his_vol(start, end)
        self.process_constant()

        self.get_underlying_symbol_price(start, end)
        self.get_minute_price(code, start, end)

        self.get_tick(code, start, end)
        self.process_df()
        self.greekiv()

        self.code_minute["time"] = pandas.to_datetime(self.code_minute.index).values.astype(object)
        df = self.code_minute[['time', "code", "underlying_symbol", 'open', 'close', 'high', 'low', 'money', "volume",
                               'pct', 'a1_p', 'a1_v', 'b1_p', 'b1_v', 'delta', 'gamma', 'vega', 'theta', 'iv',
                               'timevalue']]

        return df.values.tolist()



if __name__ == "__main__":
    opc = OpContractQuote()
    opc.get(code="10004237.XSHG", start='2022-11-11 00:00:00', end='2022-11-21 23:00:00')
