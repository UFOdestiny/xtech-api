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

        del self.result["close"]

    def daily_info(self, start, end):
        q = query(opt.OPT_CONTRACT_INFO.code,
                  opt.OPT_CONTRACT_INFO.underlying_symbol,
                  opt.OPT_CONTRACT_INFO.exercise_price,
                  opt.OPT_CONTRACT_INFO.contract_type,
                  opt.OPT_CONTRACT_INFO.contract_unit,
                  opt.OPT_CONTRACT_INFO.expire_date,
                  opt.OPT_CONTRACT_INFO.is_adjust).filter(self.query_underlying_symbol,
                                                          opt.OPT_CONTRACT_INFO.list_date <= start,
                                                          opt.OPT_CONTRACT_INFO.expire_date >= end, )

        self.daily = opt.run_query(q)

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
        month_00 = today.replace(month=today_month + 1, day=1)
        month_01 = today.replace(month=today_month + 2, day=1)

        df_01 = self.daily[(month_00 <= self.daily["expire_date"]) & (self.daily["expire_date"] <= month_01)]
        self.code = df_01["code"].unique().tolist()

        # self.CO = df_01[df_01["contract_type"] == "CO"]
        # self.PO = df_01[df_01["contract_type"] == "PO"]
        #
        # self.CO_code = self.CO["code"].values
        # self.PO_code = self.PO["code"].values

        # print(len(self.CO_code), len(self.PO_code))

        # co = [f"r[\"opcode\"] == \"{i}\"" for i in self.CO_code]
        # self.CO_code_all = " or ".join(co)
        #
        # po = [f"r[\"opcode\"] == \"{i}\"" for i in self.PO_code]
        # self.PO_code_all = " or ".join(po)
        # print(self.CO)
        # print(self.PO)
        # print(self.CO_code)
        # print(self.PO_code)

    def get_all_iv_delta(self, start, end):
        self.code = ['MO2309-C-7000.CCFX', 'MO2309-C-6800.CCFX', 'MO2309-C-6000.CCFX',
                     "MO2306-P-5200.CCFX", "MO2306-P-5400.CCFX", "MO2306-P-5600.CCFX", ]

        co = [f"r[\"opcode\"] == \"{i}\"" for i in self.code]
        co = " or ".join(co)

        filter_ = f"""|> filter(fn: (r) => {co})"""
        filter_ += f"""|> filter(fn: (r) => r["type"] == "CO" or r["type"] == "PO")
                       |> filter(fn: (r) => r["_field"] == "delta" or r["_field"] == "iv")"""

        df = self.db.query_influx(start=start, end=end, measurement="opcontractquote", filter_=filter_,
                                  keep=["_time", "targetcode", "delta", "iv", "type"])

        if len(df) == 0:
            print("None...")
            return None, None

        df.set_index("_time", inplace=True)

        targetcode = df["targetcode"].unique().tolist()
        for tc in targetcode:
            df_temp = df[df["targetcode"] == tc]
            self.iv_delta[tc] = df_temp

            # df_co = df_temp[df_temp["type"] == "CO"]
            # df_po = df_temp[df_temp["type"] == "PO"]

            # df_co.drop(df[(df.iv == 0) & (df.delta == 1) & (df.delta == 0)].index, inplace=True)
            # df_po.drop(df[(df.iv == 0) & (df.delta == 1) & (df.delta == 0)].index, inplace=True)
            #
            # calld = df_co.sort_values(by='delta').drop_duplicates(subset=['delta'], keep='first')
            # putd = df_po.sort_values(by='delta').drop_duplicates(subset=['delta'], keep='first')
            #
            # dic_temp = {"CO": [calld["delta"].to_list(), calld["iv"].to_list()],
            #             "PO": [putd["delta"].to_list(), putd["iv"].to_list()]}
            #
            # self.dic[tc] = dic_temp
            # df.drop(df[(df.iv == 0) & (df.delta == 1) & (df.delta == 0)].index, inplace=True)
            # df.drop_duplicates(subset=['delta'], keep="first", inplace=True)
            # df.sort_values(by=['delta'], inplace=True)
            # self.iv_delta = df_temp  # df
            # return df["delta"].tolist(), df["iv"].tolist()

    @staticmethod
    def group_f(df):
        df.drop(df[(df.iv == 0) & (df.delta == 1) & (df.delta == 0)].index, inplace=True)
        df_co = df[df["type"] == "CO"]
        df_po = df[df["type"] == "PO"]

        calld = df_co.sort_values(by='delta').drop_duplicates(subset=['delta'], keep='first')
        putd = df_po.sort_values(by='delta').drop_duplicates(subset=['delta'], keep='first')

        co_delta, co_iv = calld["delta"], calld["iv"]
        po_delta, po_iv = putd["delta"], putd["iv"]

        tck1 = spi.splrep(co_delta, co_iv, k=1)
        ivc0 = spi.splev([0.25, 0.5], tck1, ext=0)

        if len(co_delta) <= 1 or len(po_delta) <= 1:
            putd, calld, putd_calld = np.nan, np.nan, np.nan
        else:
            tck2 = spi.splrep(po_delta, po_iv, k=1)
            ivp0 = spi.splev([-0.25, -0.5], tck2, ext=0)

            putd = ivp0[0] - ivp0[1]
            calld = ivc0[0] - ivc0[1]
            putd_calld = putd - calld

        return pandas.DataFrame({"putd": [putd], "calld": [calld], "putd_calld": [putd_calld]})

    def aggregate(self):
        for tg in self.iv_delta:
            df = self.iv_delta[tg]
            g = df.groupby(df.index)
            df_ = g.apply(self.group_f)
            # df_.reset_index("_time", inplace=True)
            # df_.set_index("_time", inplace=True)
            index = self.result.loc[self.result["code"] == tg].index
            self.result.loc[index, "calld"] = df_["calld"].values
            self.result.loc[index, "putd"] = df_["putd"].values
            self.result.loc[index, "putd_calld"] = df_["putd_calld"].values
        #
        # indexes = self.result.index[(self.result.index >= start) & (self.result.index <= end)]
        # pairs = [(str(indexes[i]), str(indexes[i + 1])) for i in range(len(indexes) - 1)]
        # pairs.append()
        #
        # for s, e in pairs:
        #     # print(s)
        #     CO_delta, CO_iv = self.get_all_iv_delta(start, end)
        #     PO_delta, PO_iv = self.get_all_iv_delta(start, end)
        #
        #     tck1 = spi.splrep(CO_delta, CO_iv, k=1)
        #     ivc0 = spi.splev([0.25, 0.5], tck1, ext=0)
        #
        #     if len(CO_delta) <= 1 or len(PO_delta) <= 1:
        #         putd, calld, putd_calld = np.nan, np.nan, np.nan
        #     else:
        #         tck2 = spi.splrep(PO_delta, PO_iv, k=1)
        #         ivp0 = spi.splev([-0.25, -0.5], tck2, ext=0)
        #         putd = ivp0[0] - ivp0[1]
        #         calld = ivc0[0] - ivc0[1]
        #         putd_calld = putd - calld
        #
        #     self.result.loc[s, "putd"] = putd
        #     self.result.loc[s, "calld"] = calld
        #     self.result.loc[s, "putd_calld"] = putd_calld

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
            self.get_all_iv_delta(t[0], t[1])
            self.aggregate()

            # self.result["time"] = pandas.to_datetime(self.result.index).values.astype(object)
            # self.result.reset_index(drop=True, inplace=True)

        if self.result is None:
            return None, None

        self.result.set_index("time", inplace=True)
        self.result.index = pandas.DatetimeIndex(self.result.index, tz='Asia/Shanghai')
        self.result.rename(columns={"code": "targetcode"}, inplace=True)
        self.result.dropna(how="any", inplace=True)
        tag_columns = ['targetcode']

        return self.result, tag_columns


if __name__ == "__main__":
    opc = PutdMinusCalld()
    start = '2023-02-06 00:00:00'
    end = '2023-02-07 00:00:00'

    a, _ = opc.get(start=start, end=end)
    print(a)

    # opc.get_adjust()
    # opc.pre_set(start, end)
    # opc.daily_info(start, end)
    # opc.get_all_iv_delta(start, end)
    # opc.aggregate()

    # a = opc.iv_delta['000852.XSHG']
    # b = a.groupby(a.index)
    # c = b.apply(opc.group_f)
    # c.reset_index("_time", inplace=True).set_index("_time", inplace=True)
    # print(c)
