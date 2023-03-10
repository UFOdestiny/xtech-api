# -*- coding: utf-8 -*-
# @Name     : PutdMinusCalld.py
# @Date     : 2022/12/30 11:13
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
import datetime

import numpy as np
import pandas
import scipy.interpolate as spi
from jqdatasdk import opt, query, get_price
from sqlalchemy import or_

from service.InfluxService import InfluxService
from service.JoinQuant import JQData
from utils.InfluxTime import InfluxTime
from utils.InfluxTime import SplitTime


class PutdMinusCalld(JQData):
    def __init__(self):
        super().__init__()
        self.db = InfluxService()

        self.code = None

        self.daily = None

        self.month1 = None
        self.result = None

        self.iv_delta = dict()

    def pre_set(self, start, end):
        self.result = get_price(self.targetcodes, fields=['close'], frequency='1m', start_date=start, end_date=end)

        if len(self.result) == 0:
            return None

        self.result["time"] -= pandas.Timedelta(minutes=1)
        # self.result.set_index("time", inplace=True)
        # self.result.index -= pandas.Timedelta(minutes=1)

        # self.result["targetcode"] = code
        self.result["putd"] = 0
        self.result["calld"] = 0
        self.result["putd_calld"] = 0

        # del self.result["close"]

    def daily_info(self, start, end):
        q = query(opt.OPT_CONTRACT_INFO.code,
                  opt.OPT_CONTRACT_INFO.underlying_symbol,
                  opt.OPT_CONTRACT_INFO.exercise_price,
                  opt.OPT_CONTRACT_INFO.contract_type,
                  opt.OPT_CONTRACT_INFO.contract_unit,
                  opt.OPT_CONTRACT_INFO.expire_date,
                  opt.OPT_CONTRACT_INFO.is_adjust).filter(
            or_(opt.OPT_CONTRACT_INFO.underlying_symbol == "510050.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "510500.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "510300.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159901.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159919.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159915.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159922.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000852.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000300.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000016.XSHG",
                ),
            opt.OPT_CONTRACT_INFO.list_date <= start,
            opt.OPT_CONTRACT_INFO.expire_date >= start, )

        self.daily = opt.run_query(q)
        # print(self.daily)

        if len(self.daily) == 0:
            return None

        temp_adjust = self.adjust[self.adjust["adj_date"] >= InfluxTime.to_date(start)]
        self.daily = pandas.merge(left=self.daily, right=temp_adjust, on="code", how="left")
        for i in range(len(self.daily)):
            if self.daily.loc[i, "is_adjust"] == 1:
                self.daily.loc[i, "exercise_price"] = self.daily.loc[i, "ex_exercise_price"]
                self.daily.loc[i, "contract_unit"] = self.daily.loc[i, "ex_contract_unit"]

        self.daily.drop(["is_adjust", "adj_date", "ex_exercise_price", "ex_contract_unit"], inplace=True, axis=1)

        today = datetime.date.today()
        today_month = today.month
        if today_month == 12:
            month_00 = today.replace(year=today.year + 1, month=1, day=1)
            month_01 = today.replace(year=today.year + 1, month=2, day=1)
        else:
            month_00 = today.replace(month=today_month + 1, day=1)
            month_01 = today.replace(month=today_month + 2, day=1)

        df_01 = self.daily[(month_00 <= self.daily["expire_date"]) & (self.daily["expire_date"] <= month_01)]

        # print(df_01)
        self.code = df_01["code"].unique().tolist()

    def get_all_iv_delta(self, start, end):
        # self.code = ['MO2309-C-7000.CCFX', 'MO2309-C-6800.CCFX', 'MO2309-C-6000.CCFX',
        #              "MO2306-P-5200.CCFX", "MO2306-P-5400.CCFX", "MO2306-P-5600.CCFX", ]
        # print(len(self.code))
        co = [f"r[\"opcode\"] == \"{i}\"" for i in self.code]
        co = " or ".join(co)

        filter_ = f"""|> filter(fn: (r) => {co})"""
        filter_ += f"""|> filter(fn: (r) => r["type"] == "CO" or r["type"] == "PO")
                       |> filter(fn: (r) => r["_field"] == "delta" or r["_field"] == "iv")"""

        df = self.db.query_influx(start=start, end=end, measurement="opcontractquote", filter_=filter_,
                                  keep=["_time", "targetcode", "delta", "iv", "type"])

        if not df or len(df) == 0:
            return False

        df.set_index("_time", inplace=True)

        targetcode = df["targetcode"].unique().tolist()
        for tc in targetcode:
            df_temp = df[df["targetcode"] == tc]
            self.iv_delta[tc] = df_temp
        return True

    @staticmethod
    def group_f(df):
        # print(df)
        df.drop(df[(df.iv == 0) & (df.delta == 1) & (df.delta == 0)].index, inplace=True)
        df_co = df[df["type"] == "CO"]
        df_po = df[df["type"] == "PO"]

        df_co.sort_values(by='delta', inplace=True)
        df_co.drop_duplicates(subset=['delta'], keep='first', inplace=True)
        df_po.sort_values(by='delta', inplace=True)
        df_po.drop_duplicates(subset=['delta'], keep='first', inplace=True)

        co_delta, co_iv = df_co["delta"].to_list(), df_co["iv"].to_list()
        po_delta, po_iv = df_po["delta"].to_list(), df_po["iv"].to_list()
        # print("__________________")
        # print(co_delta)
        # print(co_iv)
        # print(po_delta)
        # print(po_iv)

        if len(co_delta) <= 1 or len(po_delta) <= 1:
            putd, calld, putd_calld = np.nan, np.nan, np.nan
        else:
            tck1 = spi.splrep(co_delta, co_iv, k=1)

            ivc0 = spi.splev([0.25, 0.5], tck1, ext=0)
            # print(ivc0)

            tck2 = spi.splrep(po_delta, po_iv, k=1)

            ivp0 = spi.splev([-0.25, -0.5], tck2, ext=0)
            # print(ivp0)

            putd = ivp0[0] - ivp0[1]
            calld = ivc0[0] - ivc0[1]
            putd_calld = putd - calld

            # print(putd)
            # print(calld)
            # print(putd_calld)

        return pandas.DataFrame({"putd": [putd], "calld": [calld], "putd_calld": [putd_calld]})

    def aggregate(self):
        for tg in self.iv_delta:
            df = self.iv_delta[tg]
            g = df.groupby(df.index)
            df_ = g.apply(self.group_f)
            df_.reset_index("_time", inplace=True)
            df_.set_index("_time", inplace=True)

            # print(df)
            df_time = df_.index
            index = self.result.loc[(self.result["code"] == tg) &
                                    (self.result["time"] >= df_time[0]) &
                                    (self.result["time"] <= df_time[-1])].index

            self.result.loc[index, "calld"] = df_["calld"].values
            self.result.loc[index, "putd"] = df_["putd"].values
            self.result.loc[index, "putd_calld"] = df_["putd_calld"].values

    def get(self, **kwargs):
        start_ = kwargs["start"]
        end_ = kwargs["end"]
        times = SplitTime.split(start_, end_, interval_day=1)
        self.get_adjust()

        for t in times:
            self.pre_set(t[0], t[1])
            if self.result is None:
                continue
            self.daily_info(t[0], t[1])
            if self.daily is None:
                continue
            index = self.get_all_iv_delta(t[0], t[1])
            if not index:
                return None, None
            self.aggregate()

            # self.result["time"] = pandas.to_datetime(self.result.index).values.astype(object)
            # self.result.reset_index(drop=True, inplace=True)

        if self.result is None:
            return None, None

        self.result.set_index("time", inplace=True)
        self.result.index = pandas.DatetimeIndex(self.result.index, tz='Asia/Shanghai')
        self.result.rename(columns={"code": "targetcode", "close": "price"}, inplace=True)
        self.result.dropna(how="any", inplace=True)
        tag_columns = ['targetcode']

        return self.result, tag_columns


if __name__ == "__main__":
    pandas.set_option("display.max_rows", None)

    opc = PutdMinusCalld()
    start = '2023-02-23 10:00:00'
    end = '2023-02-23 10:05:00'

    a, _ = opc.get(start=start, end=end)
    print(a)

    # opc.get_adjust()
    # opc.pre_set(start, end)
    # opc.daily_info(start, end)
    # opc.get_all_iv_delta(start, end)
    # opc.aggregate()

    # a = opc.iv_delta['000852.XSHG']
    # b = a.groupby(a.index)
    # c = b.apply(opc.group_f1)
    # c.reset_index("_time", inplace=True).set_index("_time", inplace=True)
    # print(c)
