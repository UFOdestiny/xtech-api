# -*- coding: utf-8 -*-
# @Name     : OpNominalAmount.py
# @Date     : 2022/11/29 11:35
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
import datetime

import pandas
from jqdatasdk import opt, query, get_price

from service.InfluxService import InfluxService
from service.JoinQuant import JQData
from utils.InfluxTime import SplitTime, InfluxTime


class OpNominalAmount(JQData):
    def __init__(self):
        super().__init__()

        self.db = InfluxService()
        self.daily = None
        self.code = None
        self.daily_00 = None
        self.code_00 = None
        self.daily_01 = None
        self.code_01 = None

        self.result = None

    def pre_set(self, start, end):
        self.result = get_price(self.targetcodes, fields=['close'], frequency='60m', start_date=start, end_date=end, )

        if len(self.result) == 0:
            self.result = None

        self.result["money_c"] = 0
        self.result["money_p"] = 0
        self.result["money"] = 0

        self.result["money_c_00"] = 0
        self.result["money_p_00"] = 0
        self.result["money_00"] = 0

        self.result["money_c_01"] = 0
        self.result["money_p_01"] = 0
        self.result["money_01"] = 0

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

        temp_adjust = self.adjust[self.adjust["adj_date"] >= InfluxTime.to_date(start)]
        self.daily = pandas.merge(left=self.daily, right=temp_adjust, on="code", how="left")

        for i in range(len(self.daily)):
            if self.daily.loc[i, "is_adjust"] == 1:
                self.daily.loc[i, "exercise_price"] = self.daily.loc[i, "ex_exercise_price"]
                self.daily.loc[i, "contract_unit"] = self.daily.loc[i, "ex_contract_unit"]

        self.daily.drop(["is_adjust", "adj_date", "ex_exercise_price", "ex_contract_unit"], inplace=True, axis=1)
        self.daily.dropna(how="any", inplace=True)

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

    def vol(self, code, start, end):
        df = get_price(code, start, end, frequency='60m', fields=['close', 'volume'])

        if len(df) == 0:
            return

        temp = self.daily[self.daily["code"] == code].iloc[0]

        unit = temp["contract_unit"]
        type_ = temp["contract_type"]
        targetcode = temp["underlying_symbol"]

        df["unit"] = unit
        df["close"] = df["close"] * df["unit"] * df["volume"]
        del df["unit"]

        if code in self.code_00:
            suffix = "_00"
        elif code in self.code_01:
            suffix = "_01"
        else:
            suffix = ""

        if type_ == "CO":
            type_ = "_c"
        else:
            type_ = "_p"

        index = f"money{type_}{suffix}"

        # print('_________________________')
        # print(targetcode)
        # print(index)
        # print(self.result[self.result["code"] == targetcode][index])
        # print(df["close"])
        # print('_________________________')

        ind = self.result[self.result["code"] == targetcode].index

        # print(df)
        # print(self.result.iloc[0].code)
        # print(targetcode)
        # print(self.result.iloc[0].code==targetcode)
        # print(self.result[self.result["code"] == targetcode])
        df.set_index(ind, inplace=True)
        # print(index)
        # print(self.result[self.result["code"] == targetcode])
        # print(df["close"])
        # df_=df.reset_index()
        # print(self.result)
        t = self.result[self.result["code"] == targetcode][index].add(df["close"], fill_value=0)
        self.result.loc[ind, index] = t
        # print(self.result)

        if suffix:
            index = f"money{type_}"
            t = self.result[self.result["code"] == targetcode][index].add(df["close"], fill_value=0)
            ind = self.result[self.result["code"] == targetcode].index
            self.result.loc[ind, index] = t

        # df.fillna(method='ffill', inplace=True)

    def vol_aggregate(self, start, end):
        if self.code is None:
            self.result = None
            return
        print(len(self.code))
        for i in self.code:
            self.vol(i, start, end)

        if self.result is None or len(self.result) == 0:
            return

        self.result["money"] = self.result["money_c"] + self.result["money_p"]
        self.result["money_00"] = self.result["money_c_00"] + self.result["money_p_00"]
        self.result["money_01"] = self.result["money_c_01"] + self.result["money_p_01"]

    def get(self, **kwargs):
        start = kwargs["start"]
        end = kwargs["end"]
        times = SplitTime.split(start, end, interval_day=1)
        self.get_adjust()

        for t in times:
            self.pre_set(t[0], t[1])
            if self.result is None:
                print(t[0], t[1], "pass")
                continue
            self.daily_info(t[0], t[1])
            self.vol_aggregate(t[0], t[1])

        if self.result is None:
            return None, None
        self.result.dropna(inplace=True)

        self.result.set_index("time", inplace=True)
        self.result.index = pandas.DatetimeIndex(self.result.index, tz='Asia/Shanghai')

        self.result.rename(columns={"code": "targetcode"}, inplace=True)
        tag_columns = ['targetcode']
        return self.result, tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_columns', None)
    opc = OpNominalAmount()
    start = '2023-02-17 00:00:00'
    end = '2023-02-18 00:00:00'

    a, _ = opc.get(start=start, end=end)
    print(a)
