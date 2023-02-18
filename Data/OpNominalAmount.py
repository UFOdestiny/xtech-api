# -*- coding: utf-8 -*-
# @Name     : OpNominalAmount.py
# @Date     : 2022/11/29 11:35
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
import datetime
import time

import numpy as np
import pandas
from jqdatasdk import opt, query, get_price
from sqlalchemy import or_

from service.InfluxService import InfluxService
from utils.InfluxTime import SplitTime, InfluxTime
from utils.JoinQuant import Authentication


class OpNominalAmount(metaclass=Authentication):
    def __init__(self):
        self.targetcode = ['510050.XSHG', '510300.XSHG', '159919.XSHE', '510500.XSHG', '159915.XSHE', '159901.XSHE',
                           '159922.XSHE', '000852.XSHG', '000300.XSHG', "000016.XSHG"]
        self.db = InfluxService()

        self.daily = None
        self.code = None
        self.daily_00 = None
        self.code_00 = None
        self.daily_01 = None
        self.code_01 = None

        self.df = None
        self.result = None

        self.figured = []

        self.final_result = None
        self.adjust = None

    def get_adjust(self):
        q = query(opt.OPT_ADJUSTMENT.adj_date,
                  opt.OPT_ADJUSTMENT.code,
                  opt.OPT_ADJUSTMENT.ex_exercise_price,
                  opt.OPT_ADJUSTMENT.ex_contract_unit, )
        df = opt.run_query(q)
        df.dropna(how="any", inplace=True)
        self.adjust = df
        return df

    def pre_set(self, start, end):
        self.result = get_price(self.targetcode, fields=['close'], frequency='60m', start_date=start, end_date=end, )
        # self.result["targetcode"] = code

        self.result["vol_c"] = 0
        self.result["vol_p"] = 0
        self.result["vol"] = 0

        self.result["vol_c_00"] = 0
        self.result["vol_p_00"] = 0
        self.result["vol_00"] = 0

        self.result["vol_c_01"] = 0
        self.result["vol_p_01"] = 0
        self.result["vol_01"] = 0

        del self.result["close"]

    def daily_info(self, start, end):
        q = query(opt.OPT_CONTRACT_INFO.code,
                  opt.OPT_CONTRACT_INFO.underlying_symbol,
                  opt.OPT_CONTRACT_INFO.exercise_price,
                  opt.OPT_CONTRACT_INFO.contract_type,
                  opt.OPT_CONTRACT_INFO.contract_unit,
                  opt.OPT_CONTRACT_INFO.expire_date,
                  opt.OPT_CONTRACT_INFO.is_adjust).filter(

            or_(
                opt.OPT_CONTRACT_INFO.underlying_symbol == "510050.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "510300.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159919.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159915.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159901.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159922.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000852.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000300.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000016.XSHG",
            ),
            opt.OPT_CONTRACT_INFO.list_date <= start,
            opt.OPT_CONTRACT_INFO.expire_date >= end, )

        self.daily = opt.run_query(q)

        year, month, day = time.strptime(start, InfluxTime.yearmd_hourms_format)[:3]
        temp_adjust = self.adjust[self.adjust["adj_date"] >= datetime.date(year, month, day)]
        self.daily = pandas.merge(left=self.daily, right=temp_adjust, on="code", how="left")

        for i in range(len(self.daily)):
            if self.daily.loc[i, "is_adjust"] == 1:
                self.daily.loc[i, "exercise_price"] = self.daily.loc[i, "ex_exercise_price"]
                self.daily.loc[i, "contract_unit"] = self.daily.loc[i, "ex_contract_unit"]

        self.daily.drop(["is_adjust", "adj_date", "ex_exercise_price", "ex_contract_unit"], inplace=True, axis=1)

        if len(self.daily) == 0:
            self.code = None
            return None

        today = datetime.date.today()
        today_month = today.month
        month_00 = today.replace(month=today_month + 1, day=1)
        month_01 = today.replace(month=today_month + 2, day=1)

        self.daily_00 = self.daily[self.daily["expire_date"] <= month_00]
        self.daily_01 = self.daily[(month_00 <= self.daily["expire_date"]) & (self.daily["expire_date"] <= month_01)]

        self.code = self.daily["code"].values
        self.code_00 = self.daily_00["code"].values
        self.code_01 = self.daily_01["code"].values

    def vol(self, code, start, end, types):
        df = get_price(code, start, end, frequency='60m', fields=['close', 'volume'])
        if len(df) == 0:
            return

        temp = self.daily[self.daily["code"] == code].iloc[0]
        unit = temp["contract_unit"]
        type_ = temp["contract_type"]

        df["unit"] = unit
        # df.fillna(method='ffill', inplace=True)

        # print(df["close"], df["unit"], df["volume"])
        df["close"] = df["close"] * df["unit"] * df["volume"]
        del df["unit"]

        if types == 0:
            res_seg = ""
        elif types == 1:
            res_seg = "_00"
        else:
            res_seg = "_01"
            # print(df)

        index_c = f"money_c{res_seg}"
        index_p = f"money_p{res_seg}"

        # print(code)
        # print(df)

        if type_:
            self.result[index_c] = self.result[index_c].add(df["close"], fill_value=0)
        else:
            self.result[index_p] = self.result[index_p].add(df["close"], fill_value=0)

    def vol_aggregate(self, start, end):
        if self.code is None:
            self.result = None
            return

        for i in self.code:
            self.vol(i, start, end, 0)

        for i in self.code_00:
            self.vol(i, start, end, 1)

        for i in self.code_01:
            self.vol(i, start, end, 2)

        if self.result is None or len(self.result) == 0:
            return

        self.result["money"] = self.result["money_c"] + self.result["money_p"]
        self.result["money_00"] = self.result["money_c_00"] + self.result["money_p_00"]
        self.result["money_01"] = self.result["money_c_01"] + self.result["money_p_01"]
        # self.result["money"]=self.result["vol"]*1

        # self.result["time"] = pandas.to_datetime(self.result.index).values.astype(object)
        # self.result = self.result[['time', "targetcode", 'vol_c', 'vol_p', 'vol', 'vol_c_00', 'vol_p_00',
        #                            "vol_00", 'vol_c_01', 'vol_p_01', "vol_01"]]

        if self.final_result is None:
            self.final_result = self.result
        else:
            self.final_result = pandas.concat([self.final_result, self.result])

    def get(self, **kwargs):
        start = kwargs["start"]
        end = kwargs["end"]
        times = SplitTime.split(start, end, interval_day=1)
        self.get_adjust()

        for t in times:
            self.pre_set(t[0], t[1])
            self.daily_info(t[0], t[1])
            self.vol_aggregate(t[0], t[1])
            length = len(self.result) if self.result is not None else 0
            print(t[0], t[1], length)

        if self.final_result is None:
            return None, None
        self.final_result.dropna(inplace=True)

        tag_columns = ['targetcode']
        self.final_result.index = pandas.DatetimeIndex(self.final_result.index, tz='Asia/Shanghai')

        return self.final_result, tag_columns


if __name__ == "__main__":
    opc = OpNominalAmount()
    start = '2023-01-02 00:00:00'
    end = '2023-01-03 00:00:00'

    opc.get_adjust()
    opc.pre_set(start, end)
    opc.daily_info(start, end)
    # print(opc.result)
    #
    # start_tm = time.mktime(time.strptime(start, InfluxTime.yearmd_hourms_format))
    #
    # f_ = f"""|> filter(fn: (r) => r["_field"] == "expire_date" and r["_value"] >= {start_tm} )"""
    # t = opc.db.query_influx(start='2023-01-01 00:00:00', end=end, measurement="opcontractinfo", )
    # print(t)

    # a, b = opc.get(start='2023-02-01 00:00:00', end='2023-02-17 00:00:00')
    # print(a["vol"].to_list())
    # print(a["vol"].values())
