# -*- coding: utf-8 -*-
# @Name     : OpContractQuote.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime
import math
import time

import numpy as np
import pandas
from jqdatasdk import get_ticks, opt, query, get_price

from service.InfluxService import InfluxService
from utils.GreeksIV import Greeks, ImpliedVolatility
from utils.InfluxTime import InfluxTime
from utils.JoinQuant import Authentication


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

        self.adjust = None

    def get_adjust(self):
        q = query(opt.OPT_ADJUSTMENT.adj_date,
                  opt.OPT_ADJUSTMENT.code,
                  opt.OPT_ADJUSTMENT.ex_exercise_price,
                  opt.OPT_ADJUSTMENT.ex_contract_unit, )
        df = opt.run_query(q)
        df.dropna(how="any", inplace=True)
        df.drop_duplicates(keep="first", inplace=True)
        self.adjust = df
        return df

    def daily_info(self, code, start_date, end_date):
        start_date = start_date[:11] + "00:00:00"

        q = query(opt.OPT_CONTRACT_INFO.code,
                  opt.OPT_CONTRACT_INFO.underlying_symbol,
                  opt.OPT_CONTRACT_INFO.exercise_price,
                  opt.OPT_CONTRACT_INFO.contract_type,
                  opt.OPT_CONTRACT_INFO.contract_unit,
                  opt.OPT_CONTRACT_INFO.expire_date,
                  opt.OPT_CONTRACT_INFO.is_adjust).filter(opt.OPT_CONTRACT_INFO.code == code)

        # q = query(opt.OPT_DAILY_PREOPEN.date,
        #           opt.OPT_DAILY_PREOPEN.code,
        #           opt.OPT_DAILY_PREOPEN.underlying_symbol,
        #           opt.OPT_DAILY_PREOPEN.exercise_price,
        #           opt.OPT_DAILY_PREOPEN.expire_date,
        #           opt.OPT_DAILY_PREOPEN.contract_type, ).filter(
        #     opt.OPT_DAILY_PREOPEN.code == code,
        #     opt.OPT_DAILY_PREOPEN.date >= start_date,
        #     opt.OPT_DAILY_PREOPEN.date <= end_date)

        self.pre_open = opt.run_query(q)
        time_list = pandas.date_range(start_date, end_date)
        temp = self.pre_open.iloc[0]
        for i in range(len(time_list) - 1):
            self.pre_open.loc[self.pre_open.shape[0]] = temp

        year, month, day = time.strptime(start_date, InfluxTime.yearmd_hourms_format)[:3]
        temp_ad = self.adjust[self.adjust["adj_date"] >= datetime.date(year, month, day)]
        self.pre_open = pandas.merge(left=self.pre_open, right=temp_ad, on="code", how="left")
        self.pre_open.set_index(time_list, inplace=True)

        if not np.isnan(self.pre_open.iloc[0]["ex_contract_unit"]):
            for i in range(len(self.pre_open)):
                index = self.pre_open.index[i]
                if index < self.pre_open.loc[index, "adj_date"]:
                    self.pre_open.loc[index, "exercise_price"] = self.pre_open.loc[index, "ex_exercise_price"]
                    self.pre_open.loc[index, "contract_unit"] = self.pre_open.loc[index, "ex_contract_unit"]

        self.pre_open.drop(["is_adjust", "adj_date", "ex_exercise_price", "ex_contract_unit"], inplace=True, axis=1)
        opc.pre_open["expire_date"] = pandas.to_datetime(opc.pre_open["expire_date"])
        self.pre_open['days'] = (self.pre_open["expire_date"] - self.pre_open.index).apply(lambda x: x.days / 365)

        # self.pre_open.set_index("date", inplace=True)

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
        self.code_minute = get_price(code, frequency='minute', start_date=start, end_date=end,
                                     fields=['open', 'close', 'high', 'low', 'volume', 'money', 'pre_close'])
        self.code_minute.index -= pandas.Timedelta(minutes=1)

        # print(self.code_minute)
        temp = self.code_minute.index[0]
        for i in range(len(self.code_minute)):
            index = self.code_minute.index[i]
            if datetime.time(9, 30) == index.time():
                temp = self.code_minute.loc[index, "pre_close"]
            else:
                self.code_minute.iloc[i, -1] = temp
        # print(self.code_minute)
        self.code_minute["pct"] = (self.code_minute["close"] - self.code_minute["pre_close"]) / self.code_minute[
            "pre_close"]
        # self.code_minute["pct"] = np.log(self.code_minute["close"].div(self.code_minute["close"].shift()))
        # self.code_minute["pct"][0] = 0

    def get_tick(self, code, start_date, end_date):
        """
        获取tick行情
        :param code:
        :param start_date:
        :param end_date:
        :return:
        """
        # start_date = start_date[:11] + "00:00:00"

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

        for i in ["his_vol", "exercise_price", "contract_type", "days"]:
            for j in range(len(self.constant)):
                self.code_minute.loc[self.constant.index[j], i] = self.constant.iloc[j][i]

        # if len(self.constant) > 1:
        #     print(self.code_minute)
        #     print(self.constant)
        #     self.code_minute[["his_vol", "exercise_price", "contract_type", "days"]] = self.constant
        # else:
        #     for i in ["his_vol", "exercise_price", "contract_type", "days"]:
        #         self.code_minute[i] = self.constant.iloc[0][i]

        # print(self.code_minute)

        df = self.code_minute[["his_vol", "exercise_price", "contract_type", "days"]].copy()
        df.fillna(method='ffill', inplace=True)

        self.code_minute[["his_vol", "exercise_price", "contract_type", "days"]] = df

    def greekiv(self, start, end):

        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        self.code_minute = self.code_minute[(self.code_minute.index >= start) & (self.code_minute.index <= end)]

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
            lambda x: max(float(0), x["close"] - x["contract_type"] * (x["symbol_price"] - x["exercise_price"])),
            axis=1)

        self.code_minute.drop(["symbol_price", "exercise_price", "days", "his_vol", "contract_type"],
                              axis=1, inplace=True)
        # print("done!")

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

        self.adjust = self.get_adjust()
        self.daily_info(code, start, end)
        self.get_his_vol(start, end)
        self.process_constant()

        self.get_underlying_symbol_price(start, end)
        self.get_minute_price(code, start, end)

        self.get_tick(code, start, end)
        self.process_df()
        self.greekiv(start, end)

        self.code_minute.dropna(how="any", inplace=True)

        self.code_minute.rename(columns={'code': 'opcode', "underlying_symbol": "targetcode"}, inplace=True)
        tag_columns = ['opcode', 'targetcode']

        self.code_minute.index = pandas.DatetimeIndex(self.code_minute.index, tz='Asia/Shanghai')
        return self.code_minute, tag_columns

    def collect_info(self, **kwargs):
        db = InfluxService()
        # q = f"""
        #             from(bucket: "{db.INFLUX.bucket}")
        #                 |> range(start: {start}, stop: {end})
        #                 |> filter(fn: (r) => r["_measurement"] == "opcontractinfo")
        #                 |> filter(fn: (r) => r["_field"] == "days")
        #                 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        #                 |> keep(columns: ["_time", "days", "opcode"])
        #         """

        time_ = InfluxTime.utc(kwargs["start"], timestamp_=True)
        filter_ = f"""|> filter(fn: (r) => r["_field"] == "expire_date" and r["_value"]>{time_})"""

        df = db.query_influx(start=kwargs["start"], end=kwargs["end"], measurement="opcontractinfo", filter_=filter_,
                             keep=["_time", "opcode", "expire_date"], unique="opcode")

        if len(df) == 0:
            return None

        filter_2 = """|> filter(fn: (r) => r["_field"] == "open")"""
        df2 = db.query_influx(start=kwargs["start"], end=kwargs["end"], measurement="opcontractquote", filter_=filter_2,
                              keep=["opcode"], unique="opcode", df=True)
        lst = list(df2["opcode"])

        # lst = [i for i in lst if i <= "10003755.XSHG"]

        df["_time"] = pandas.DatetimeIndex(df["_time"], tz='Asia/Shanghai')
        df.drop_duplicates(subset=["opcode"], inplace=True)

        # df["days"] = df["days"].apply(lambda x: datetime.timedelta(days=int(x)))

        df["expire_date"] = df["expire_date"].apply(
            lambda x: time.strftime(InfluxTime.yearmd_hourms_format, time.localtime(float(x))))
        df["expire_date"] = pandas.DatetimeIndex(df["expire_date"], tz='Asia/Shanghai')

        # print(df)
        df = df[["opcode", "_time", "expire_date"]]

        result = df.values.tolist()
        result = sorted(result, key=lambda x: x[0], reverse=True)
        # result = [i for i in result if i[0] not in lst]

        for i in range(len(result)):
            for j in [1, 2]:
                result[i][j] = result[i][j].strftime("%Y-%m-%d %H:%M:%S")

        return result[::-1]


if __name__ == "__main__":
    # pandas.set_option('display.max_rows', None)
    opc = OpContractQuote()
    start = '2023-02-01 00:00:00'
    end = '2023-02-03 00:00:00'
    # opc.daily_info("10004405.XSHG", '2023-02-01 00:00:00','2023-02-03 00:00:00')
    code = "10004405.XSHG"

    # c, f = opc.get(code=code, start=start, end=end)
    # print(c)
    opc.collect_info(start='2023-01-01 00:00:00', end='2023-02-03 00:00:00')
