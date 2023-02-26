# -*- coding: utf-8 -*-
# @Name     : OpDiscount.py
# @Date     : 2023/2/25 11:04
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime
from bisect import bisect_left
import pandas
from jqdatasdk import opt, query, get_price
from service.JoinQuant import JQData
from utils.InfluxTime import SplitTime, InfluxTime
from sqlalchemy import or_


class OpDiscount(JQData):
    def __init__(self):
        super().__init__()

        self.daily = None
        self.daily_00 = None
        self.daily_01 = None
        self.daily_02 = None
        self.dic = dict()

        self.result = None

    def pre_set(self, start, end):
        self.result = get_price(self.targetcodes, fields=['close'], frequency='1m', start_date=start, end_date=end, )
        if len(self.result) == 0:
            self.result = None
            return
        self.result["time"] -= pandas.Timedelta(minutes=1)

        # self.result.set_index("time", inplace=True)

        self.result["discount_l_00"] = 0.0
        self.result["discount_l_01"] = 0.0
        self.result["discount_l_02"] = 0.0

        self.result["discount_s_00"] = 0.0
        self.result["discount_s_01"] = 0.0
        self.result["discount_s_02"] = 0.0

        # del self.result["close"]

    def daily_info(self, start, end):
        q = query(opt.OPT_CONTRACT_INFO.code,
                  opt.OPT_CONTRACT_INFO.underlying_symbol,
                  opt.OPT_CONTRACT_INFO.exercise_price,
                  opt.OPT_CONTRACT_INFO.contract_type,
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
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000016.XSHG", ),
            opt.OPT_CONTRACT_INFO.list_date <= start,
            opt.OPT_CONTRACT_INFO.expire_date >= start, )

        self.daily = opt.run_query(q)

        temp_adjust = self.adjust[self.adjust["adj_date"] >= InfluxTime.to_date(start)]
        self.daily = pandas.merge(left=self.daily, right=temp_adjust, on="code", how="left")

        for i in range(len(self.daily)):
            if self.daily.loc[i, "is_adjust"] == 1:
                self.daily.loc[i, "exercise_price"] = self.daily.loc[i, "ex_exercise_price"]
                self.daily.loc[i, "contract_unit"] = self.daily.loc[i, "ex_contract_unit"]

        self.daily.drop(columns=["is_adjust", "adj_date", "ex_exercise_price", "ex_contract_unit", "contract_unit"],
                        inplace=True, axis=1)

        self.daily.dropna(how="any", inplace=True)

        if len(self.daily) == 0:
            return None

        today = datetime.date.today()
        today_month = today.month

        month_l1 = today.replace(day=1)
        month_00 = today.replace(month=today_month + 1, day=1)
        month_01 = today.replace(month=today_month + 2, day=1)
        month_02 = today.replace(month=today_month + 3, day=1)

        self.daily_00 = self.daily[(month_l1 <= self.daily["expire_date"]) & (self.daily["expire_date"] <= month_00)]
        self.daily_01 = self.daily[(month_00 <= self.daily["expire_date"]) & (self.daily["expire_date"] <= month_01)]
        self.daily_02 = self.daily[(month_01 <= self.daily["expire_date"]) & (self.daily["expire_date"] <= month_02)]

        code = self.daily["underlying_symbol"].unique().tolist()

        for c in code:
            self.dic[c] = dict()

            df_temp_00 = self.daily_00[self.daily_00["underlying_symbol"] == c]
            df_co_00 = df_temp_00[df_temp_00["contract_type"] == "CO"]["exercise_price"].unique().tolist()
            df_po_00 = df_temp_00[df_temp_00["contract_type"] == "PO"]["exercise_price"].unique().tolist()
            df_co_00.sort()
            df_po_00.sort()
            self.dic[c]["00"] = {"CO": df_co_00, "PO": df_po_00}

            df_temp_01 = self.daily_01[self.daily_01["underlying_symbol"] == c]
            df_co_01 = df_temp_01[df_temp_01["contract_type"] == "CO"]["exercise_price"].unique().tolist()
            df_po_01 = df_temp_01[df_temp_01["contract_type"] == "PO"]["exercise_price"].unique().tolist()
            df_co_01.sort()
            df_po_01.sort()
            self.dic[c]["01"] = {"CO": df_co_01, "PO": df_po_01}

            df_temp_02 = self.daily_02[self.daily_02["underlying_symbol"] == c]
            df_co_02 = df_temp_02[df_temp_02["contract_type"] == "CO"]["exercise_price"].unique().tolist()
            df_po_02 = df_temp_02[df_temp_02["contract_type"] == "PO"]["exercise_price"].unique().tolist()
            df_co_02.sort()
            df_po_02.sort()
            self.dic[c]["02"] = {"CO": df_co_02, "PO": df_po_02}

    @staticmethod
    def takeClosest(lst, target):
        if not lst:
            return None
        if target > lst[-1]:
            return lst[-1]
        if target < lst[0]:
            return lst[0]

        pos = bisect_left(lst, target)
        if pos == 0:
            return lst[0]
        if pos == len(lst):
            return lst[-1]

        before = lst[pos - 1]
        after = lst[pos]
        if after - target < target - before:
            return after
        else:
            return before

    def get_pp_pc(self, code, minute):
        # time_ = datetime.datetime.strptime(minute, "%Y-%m-%d %H:%M:%S")
        minute += pandas.Timedelta(minutes=1)
        df = get_price(code, fields=['close'], frequency='1m', start_date=minute, end_date=minute, )
        df.index -= pandas.Timedelta(minutes=1)
        return df.iloc[0]["close"]

    def vol_aggregate(self):
        for i in range(len(self.result)):
            # if i % 10 == 0:
            #     print(f"{i}/{len(self.result)}")
            temp = self.result.iloc[i]

            time_ = temp["time"]
            code = temp["code"]
            close = temp["close"]

            strike_00 = self.takeClosest(self.dic[code]["00"]["CO"], close)
            if strike_00:
                codes_00 = self.daily_00[(self.daily_00["exercise_price"] == strike_00) &
                                         (self.daily_00["underlying_symbol"] == code)]

                code_co_00 = codes_00[codes_00["contract_type"] == "CO"].iloc[0]["code"]
                code_po_00 = codes_00[codes_00["contract_type"] == "PO"].iloc[0]["code"]
                p_co_00 = self.get_pp_pc(code_co_00, time_)
                p_po_00 = self.get_pp_pc(code_po_00, time_)

                discount_l_00 = (strike_00 + p_po_00 - p_co_00 - close) / close
                discount_s_00 = (strike_00 - p_po_00 + p_co_00 - close) / close
                self.result.loc[i, "discount_l_00"] = discount_l_00
                self.result.loc[i, "discount_s_00"] = discount_s_00

            strike_01 = self.takeClosest(self.dic[code]["01"]["CO"], close)
            if strike_01:
                codes_01 = self.daily_01[(self.daily_01["exercise_price"] == strike_01) &
                                         (self.daily_01["underlying_symbol"] == code)]

                code_co_01 = codes_01[codes_01["contract_type"] == "CO"].iloc[0]["code"]
                code_po_01 = codes_01[codes_01["contract_type"] == "PO"].iloc[0]["code"]
                p_co_01 = self.get_pp_pc(code_co_01, time_)
                p_po_01 = self.get_pp_pc(code_po_01, time_)

                discount_l_01 = (strike_01 + p_po_01 - p_co_01 - close) / close
                discount_s_01 = (strike_01 - p_po_01 + p_co_01 - close) / close
                self.result.loc[i, "discount_l_01"] = discount_l_01
                self.result.loc[i, "discount_s_01"] = discount_s_01

            strike_02 = self.takeClosest(self.dic[code]["02"]["CO"], close)

            if strike_02:
                codes_02 = self.daily_02[(self.daily_02["exercise_price"] == strike_02) &
                                         (self.daily_02["underlying_symbol"] == code)]
                code_co_02 = codes_02[codes_02["contract_type"] == "CO"].iloc[0]["code"]
                code_po_02 = codes_02[codes_02["contract_type"] == "PO"].iloc[0]["code"]
                p_co_02 = self.get_pp_pc(code_co_02, time_)
                p_po_02 = self.get_pp_pc(code_po_02, time_)

                discount_l_02 = (strike_02 + p_po_02 - p_co_02 - close) / close
                discount_s_02 = (strike_02 - p_po_02 + p_co_02 - close) / close
                self.result.loc[i, "discount_l_02"] = discount_l_02
                self.result.loc[i, "discount_s_02"] = discount_s_02

    def get(self, **kwargs):
        # print(kwargs)
        start = kwargs["start"]
        end = kwargs["end"]
        times = SplitTime.split(start, end, interval_day=1)
        self.get_adjust()

        for t in times:
            self.pre_set(t[0], t[1])
            if self.result is None:
                print(t[0], t[1], "pass")
                return None, None

            self.daily_info(t[0], t[1])
            self.vol_aggregate()

        if self.result is None:
            print("no..")
            return None, None
        self.result.dropna(inplace=True)

        self.result.set_index("time", inplace=True)
        self.result.index = pandas.DatetimeIndex(self.result.index, tz='Asia/Shanghai')
        self.result.drop(columns=["close"], inplace=True, axis=1)
        self.result.rename(columns={"code": "targetcode"}, inplace=True)
        tag_columns = ['targetcode']
        return self.result, tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_columns', None)
    opc = OpDiscount()
    start = '2023-02-25 09:35:00'
    end = '2023-02-26 09:36:00'

    a, _ = opc.get(start=start, end=end)
    print(a)
