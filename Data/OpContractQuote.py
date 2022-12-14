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
        self.tick = None
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
        df.fillna(method='bfill', inplace=True)
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
        df.dropna(how="any", inplace=True)

        # pandas.set_option('display.max_rows', None)
        # pandas.set_option('display.max_columns', None)
        # df.fillna(method='ffill', inplace=True)
        # df.fillna(method='bfill', inplace=True)
        if not df.isnull().values.any():
            return df.values.tolist()
        else:
            print(code, "error")


if __name__ == "__main__":
    opc = OpContractQuote()
    opc.get(code="10004242.XSHG", start='2022-11-01 00:00:00', end='2022-11-30 23:00:00')
