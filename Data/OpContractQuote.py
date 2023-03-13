# -*- coding: utf-8 -*-
# @Name     : OpContractQuote.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :


import datetime
import json
import math
import time

import numpy as np
import pandas
from jqdatasdk import get_ticks, opt, query, get_price

from service.InfluxService import InfluxService
from service.RedisCache import RedisCache
from utils.GreeksIV import Greeks, ImpliedVolatility
from utils.InfluxTime import InfluxTime
from service.JoinQuant import JQData
from config import FilePath


class OpContractQuote(JQData):
    def __init__(self):
        super().__init__()
        self.df_pre = None
        self.redis = RedisCache()

        self.symbol_minute = None
        self.code = None
        self.underlying_symbol = None
        self.his_vol = None
        self.pre_open = None
        self.constant = None

        self.df = None
        self.indicator = 1

    def daily_info(self, code, start_date, end_date):
        start_date = start_date[:11] + "00:00:00"

        self.get_adjust()
        q = query(opt.OPT_CONTRACT_INFO.code,
                  opt.OPT_CONTRACT_INFO.underlying_symbol,
                  opt.OPT_CONTRACT_INFO.exercise_price,
                  opt.OPT_CONTRACT_INFO.contract_type,
                  opt.OPT_CONTRACT_INFO.contract_unit,
                  opt.OPT_CONTRACT_INFO.expire_date,
                  opt.OPT_CONTRACT_INFO.is_adjust).filter(opt.OPT_CONTRACT_INFO.code == code)

        self.pre_open = opt.run_query(q)

        if len(self.pre_open) == 0:
            self.indicator = None
            return

        time_list = pandas.date_range(start_date, end_date)

        temp = self.pre_open.iloc[0]
        for i in range(len(time_list) - 1):
            self.pre_open.loc[self.pre_open.shape[0]] = temp

        temp_ad = self.adjust[self.adjust["adj_date"] >= InfluxTime.to_date(start_date)]

        self.pre_open = pandas.merge(left=self.pre_open, right=temp_ad, on="code", how="left")
        self.pre_open.set_index(time_list, inplace=True)

        if not np.isnan(self.pre_open.iloc[0]["ex_contract_unit"]):
            for i in range(len(self.pre_open)):
                index = self.pre_open.index[i]
                if index.date() < self.pre_open.loc[index, "adj_date"]:
                    self.pre_open.loc[index, "exercise_price"] = self.pre_open.loc[index, "ex_exercise_price"]
                    self.pre_open.loc[index, "contract_unit"] = self.pre_open.loc[index, "ex_contract_unit"]

        self.pre_open.drop(["is_adjust", "adj_date", "ex_exercise_price", "ex_contract_unit"], inplace=True, axis=1)
        self.pre_open["expire_date"] = pandas.to_datetime(self.pre_open["expire_date"])

        self.pre_open['days'] = (self.pre_open["expire_date"] - self.pre_open.index).apply(lambda x: x.days / 365)

        # self.pre_open.set_index("date", inplace=True)
        # self.redis[start_date] = self.pre_open

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
        df = self.get_price(security=self.underlying_symbol,
                            fields=['close'],
                            frequency='daily',
                            start_date=start_date,
                            end_date=end_date, )

        if len(df) == 0:
            self.indicator = None
            return

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
        # sum(rts[1:]) / (length - 1)

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

        self.redis[end_date] = df
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
        # print(self.constant)

        # self.constant.index += pandas.Timedelta(hours=9, minutes=30)
        self.constant["contract_type"].replace("CO", 1, inplace=True)
        self.constant["contract_type"].replace("PO", -1, inplace=True)

    def pre_set(self, code, start, end):
        if code in self.redis:
            self.underlying_symbol = self.redis[code]["underlying_symbol"]
            self.code = code
            self.constant = self.redis[code]["constant"]
            self.df_pre = self.redis[code]["df_pre"]
            return
        else:
            self.daily_info(code, start, end)
            if not self.indicator:
                return None, None
            self.get_his_vol(start, end)
            if not self.indicator:
                return None, None
            self.process_constant()
            self.get_pre_close(code, start, end)

            self.redis[code] = {"underlying_symbol": self.underlying_symbol,
                                "constant": self.constant,
                                "df_pre": self.df_pre}

    def get_underlying_symbol_price(self, start_date, end_date):
        """
        根据期权代码获取日频的历史波动率
        :param start_date:
        :param end_date:
        :return:
        """
        self.symbol_minute = self.get_price(security=self.underlying_symbol,
                                            fields=['close'],
                                            frequency='minute',
                                            start_date=start_date,
                                            end_date=end_date, )

        if len(self.symbol_minute) == 0:
            self.indicator = None
            return

        self.symbol_minute.columns = ["symbol_minute"]
        self.symbol_minute.index -= pandas.Timedelta(minutes=1)

    def get_pre_close(self, code, start, end):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        start_temp = start.replace(hour=9, minute=30, second=0, microsecond=0)
        end_temp = end.replace(hour=9, minute=31, second=0, microsecond=0)
        df_pre = self.get_price(security=code, frequency='minute', start_date=start_temp, end_date=end_temp,
                                fields=['open', 'close', 'high', 'low', 'volume', 'money', 'pre_close'])

        df_pre = df_pre["pre_close"].values.tolist()[0]
        self.df_pre = df_pre

    def get_minute_price(self, code, start, end):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        self.df = self.get_price(security=code, frequency='minute', start_date=start, end_date=end,
                                 fields=['open', 'close', 'high', 'low', 'volume', 'money', 'pre_close'])
        if len(self.df) == 0:
            print(start, end, code)
            self.indicator = None
            return

        self.df.fillna(method='ffill', inplace=True)
        self.df.fillna(method='bfill', inplace=True)

        self.df.index -= pandas.Timedelta(minutes=1)

        if self.df_pre:
            self.df["pre_close"] = self.df_pre

        else:
            temp = self.df.index[0]
            for i in range(len(self.df)):
                index = self.df.index[i]
                if datetime.time(9, 30) == index.time():
                    temp = self.df.loc[index, "pre_close"]
                else:
                    self.df.iloc[i, -1] = temp

        self.df["pct"] = (self.df["close"] - self.df["pre_close"]) / self.df["pre_close"]

    def get_tick(self, code, start_date, end_date):
        """
        获取tick行情
        :param code:
        :param start_date:
        :param end_date:
        :return:
        """
        # start_date = start_date[:11] + "00:00:00"

        tick = get_ticks(code, start_dt=start_date, end_dt=end_date, fields=['time', "a1_p", "a1_v", "b1_p", "b1_v"])
        tick.set_index('time', inplace=True)

        if len(tick) != 0:
            self.df[["a1_p", "b1_p"]] = tick[["a1_p", "b1_p"]].resample(rule='1Min').last()
            self.df[["a1_v", "b1_v"]] = tick[["a1_v", "b1_v"]].resample(rule='1Min').sum()

        else:
            tick = get_ticks(code, end_dt=end_date, count=1, fields=['time', "a1_p", "b1_p", "a1_v", "b1_v"])
            tick.set_index('time', inplace=True)
            tick.index -= pandas.Timedelta(minutes=1)

            self.df["a1_v"] = tick["a1_v"].tolist()[0]
            self.df["b1_v"] = tick["b1_v"].tolist()[0]
            self.df["a1_p"] = tick["a1_p"].tolist()[0]
            self.df["b1_p"] = tick["b1_p"].tolist()[0]

        df = self.df[["a1_p", "b1_p", "a1_v", "b1_v"]].replace(np.float64(0), np.nan)

        df.fillna(method='ffill', inplace=True)
        df.fillna(method='bfill', inplace=True)

        self.df[["a1_p", "b1_p", "a1_v", "b1_v"]] = df

        del tick
        del df

    def process_df(self):
        self.df["symbol_price"] = self.symbol_minute
        del self.symbol_minute
        self.df["code"] = self.code
        self.df["underlying_symbol"] = self.underlying_symbol

        # print(self.code_minute)
        # print(self.constant)

        for i in ["his_vol", "exercise_price", "contract_type", "days"]:
            for j in range(len(self.constant)):
                today = pandas.to_datetime(self.constant.index[j].date())
                tomorrow = pandas.to_datetime(self.constant.index[j].date() + datetime.timedelta(days=1))
                indexes = self.df[
                    (self.df.index >= today) & (self.df.index <= tomorrow)].index
                self.df.loc[indexes, i] = self.constant.iloc[j][i]

        df = self.df[["his_vol", "exercise_price", "contract_type", "days"]].copy()
        df.fillna(method='ffill', inplace=True)

        self.df[["his_vol", "exercise_price", "contract_type", "days"]] = df
        # print(self.df)

    def greekiv(self, start, end):
        # start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        # end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        # self.df = self.df[(self.df.index >= start) & (self.df.index <= end)]

        g = Greeks()
        iv = ImpliedVolatility()

        self.df["delta"] = g.delta(self.df["symbol_price"], self.df["exercise_price"], self.df["days"],
                                   self.df["his_vol"], self.df["contract_type"])

        self.df["gamma"] = g.gamma(self.df["symbol_price"], self.df["exercise_price"], self.df["days"],
                                   self.df["his_vol"])

        self.df["vega"] = g.vega(self.df["symbol_price"], self.df["exercise_price"], self.df["days"],
                                 self.df["his_vol"])

        self.df["theta"] = g.theta(self.df["symbol_price"], self.df["exercise_price"], self.df["days"],
                                   self.df["his_vol"], self.df["contract_type"])

        self.df["iv"] = 0.0

        self.df["iv"] = self.df.apply(
            lambda x: iv.find_vol(x["close"], x["contract_type"], x["symbol_price"], x["exercise_price"],
                                  x["days"]), axis=1)

        self.df["timevalue"] = self.df.apply(
            lambda x: max(float(0), x["close"] - x["contract_type"] * (x["symbol_price"] - x["exercise_price"])),
            axis=1)

        self.df.drop(["symbol_price", "days", "his_vol", "pre_close"],
                     axis=1, inplace=True)

    def get(self, **kwargs):
        """
        按流程执行
        :return:
        """
        code = kwargs["code"]
        end = kwargs["end"]
        start = kwargs["start"]

        # t = min(datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S"),
        #         datetime.datetime.strptime("2023-02-20 00:00:00", "%Y-%m-%d %H:%M:%S")) - datetime.timedelta(days=2)
        #
        # start = t.strftime("%Y-%m-%d %H:%M:%S")

        self.pre_set(code, start, end)

        self.get_underlying_symbol_price(start, end)
        if not self.indicator:
            return None, None

        # self.get_pre_close(code, start, end)
        self.get_minute_price(code, start, end)

        self.get_tick(code, start, end)
        self.process_df()
        self.greekiv(start, end)

        self.df.dropna(how="any", inplace=True)
        self.df.rename(columns={'code': 'opcode', "underlying_symbol": "targetcode",
                                "exercise_price": "strikeprice", "contract_type": "type"}, inplace=True)

        tag_columns = ['opcode', 'targetcode', 'type']

        self.df["type"].replace(1, "CO", inplace=True)
        self.df["type"].replace(-1, "PO", inplace=True)

        self.df.index = pandas.DatetimeIndex(self.df.index, tz='Asia/Shanghai')

        return self.df, tag_columns

    def collect_info(self, **kwargs):
        if kwargs["start"] in self.redis:
            return self.redis[kwargs["start"]]

        cmd = kwargs.get("cmd", None)

        if cmd:
            with open(FilePath.path, 'r') as load_f:
                subscribe = json.load(load_f)["code_list"]
                lst = [[i, kwargs["start"], kwargs["end"]] for i in subscribe]
                return lst

        pre_start = kwargs["start"]
        update = kwargs.get("update", None)
        db = InfluxService()
        time_ = InfluxTime.utc(kwargs["start"], timestamp_=True)

        filter_ = f"""|> filter(fn: (r) => r["_field"] == "expire_date" and r["_value"]>{time_})"""

        if update:
            kwargs["start"] = "2021-01-01 00:00:00"  # 2021

        df = db.query_influx(start=kwargs["start"], end=kwargs["end"], measurement="opcontractinfo", filter_=filter_,
                             keep=["_time", "opcode", "expire_date"], unique="opcode")

        if len(df) == 0:
            return None

        # lst = [i for i in lst if i <= "10003755.XSHG"]

        df["_time"] = pandas.DatetimeIndex(df["_time"], tz='Asia/Shanghai')
        df.drop_duplicates(subset=["opcode"], inplace=True)

        # df["days"] = df["days"].apply(lambda x: datetime.timedelta(days=int(x)))

        df["expire_date"] = df["expire_date"].apply(lambda x: time.strftime(InfluxTime.yearmd_hourms_format,
                                                                            time.localtime(float(x))))
        df["expire_date"] = pandas.DatetimeIndex(df["expire_date"], tz='Asia/Shanghai')

        df = df[["opcode", "_time", "expire_date"]]

        result = df.values.tolist()
        result = sorted(result, key=lambda x: x[0], reverse=True)

        if not update:
            filter_2 = """|> filter(fn: (r) => r["_field"] == "open")"""
            df2 = db.query_influx(start=kwargs["start"], end=kwargs["end"], measurement="opcontractquote",
                                  filter_=filter_2,
                                  keep=["opcode"], unique="opcode")
            if df2 is None:
                lst = []
            else:
                lst = list(df2["opcode"])
        else:
            lst = []

        result = [i for i in result if i[0] not in lst]

        if not update:
            for i in range(len(result)):
                for j in [1, 2]:
                    result[i][j] = result[i][j].strftime("%Y-%m-%d %H:%M:%S")
        else:
            for i in range(len(result)):
                result[i][1] = pre_start
                result[i][2] = kwargs["end"]

        self.redis[kwargs["start"]] = result
        return result


if __name__ == "__main__":
    # pandas.set_option('display.max_columns', None)
    opc = OpContractQuote()
    start = '2023-03-13 13:00:00'
    end = '2023-03-13 13:11:00'
    # opc.daily_info("10004405.XSHG", '2023-02-01 00:00:00','2023-02-03 00:00:00')
    code = "10005157.XSHG"
    # print(len(opc.collect_info(start=start, end=end, update=1)))

    c, f = opc.get(code=code, start=start, end=end)
    print(c)
