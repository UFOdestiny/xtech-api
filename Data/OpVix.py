# -*- coding: utf-8 -*-
# @Name     : OpVix.py
# @Date     : 2023/3/17 14:55
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import math

import pandas
from jqdatasdk import opt, query
from sqlalchemy import or_

from utils.InfluxTime import SplitTime, InfluxTime
from service.JoinQuant import JQData
import datetime


class OpVix(JQData):
    def __init__(self):
        super().__init__()
        self.daily = None
        self.daily_00 = None
        self.daily_01 = None
        self.daily_02 = None

        self.dic = dict()

        self.df = None
        # self.targetcodes = ["510050.XSHG", "510500.XSHG"]

        self.indicator = True
        self.scroll = dict()
        self.r = 0.015

        self.df = None
        self.res = None

    def daily_info(self, start, end):

        q1 = query(opt.OPT_CONTRACT_INFO.code,
                   opt.OPT_CONTRACT_INFO.underlying_symbol,
                   opt.OPT_CONTRACT_INFO.exercise_price,
                   opt.OPT_CONTRACT_INFO.contract_type,
                   opt.OPT_CONTRACT_INFO.expire_date,
                   opt.OPT_CONTRACT_INFO.is_adjust).filter(
            or_(
                opt.OPT_CONTRACT_INFO.underlying_symbol == "510050.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "510500.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "510300.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159901.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159919.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159915.XSHE",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "159922.XSHE",
            ),
            opt.OPT_CONTRACT_INFO.list_date <= start,
            opt.OPT_CONTRACT_INFO.expire_date >= start, )

        q2 = query(opt.OPT_CONTRACT_INFO.code,
                   opt.OPT_CONTRACT_INFO.underlying_symbol,
                   opt.OPT_CONTRACT_INFO.exercise_price,
                   opt.OPT_CONTRACT_INFO.contract_type,
                   opt.OPT_CONTRACT_INFO.expire_date,
                   opt.OPT_CONTRACT_INFO.is_adjust).filter(
            or_(
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000852.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000300.XSHG",
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000016.XSHG",
            ),
            opt.OPT_CONTRACT_INFO.list_date <= start,
            opt.OPT_CONTRACT_INFO.expire_date >= start, )

        df1 = self.run_query(q1)
        d1 = sorted(df1["expire_date"].unique())

        df2 = self.run_query(q2)
        d2 = sorted(df2["expire_date"].unique())

        self.daily = pandas.concat([df1, df2])
        self.daily.reset_index(drop=True, inplace=True)

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

        self.daily_00 = self.daily[(self.daily["expire_date"] == d1[0]) | (self.daily["expire_date"] == d2[0])].copy()
        self.daily_01 = self.daily[(self.daily["expire_date"] == d1[1]) | (self.daily["expire_date"] == d2[1])].copy()
        self.daily_02 = self.daily[(self.daily["expire_date"] == d1[2]) | (self.daily["expire_date"] == d2[2])].copy()

        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S").date()

        self.daily_00["days"] = self.daily_00["expire_date"].apply(lambda x: (x - start).days / 365)
        self.daily_01["days"] = self.daily_01["expire_date"].apply(lambda x: (x - start).days / 365)
        self.daily_02["days"] = self.daily_02["expire_date"].apply(lambda x: (x - start).days / 365)

        code = self.daily["underlying_symbol"].unique().tolist()

        for c in code:
            self.dic[c] = dict()

            df_temp_00 = self.daily_00[self.daily_00["underlying_symbol"] == c]
            if df_temp_00["days"].min() > 8 * 1440:
                df_co_00 = df_temp_00[df_temp_00["contract_type"] == "CO"]["exercise_price"].unique().tolist()
                df_po_00 = df_temp_00[df_temp_00["contract_type"] == "PO"]["exercise_price"].unique().tolist()
                df_co_00.sort()
                df_po_00.sort()
                self.dic[c]["00"] = {"CO": df_co_00, "PO": df_po_00, "df": df_temp_00}

                df_temp_01 = self.daily_01[self.daily_01["underlying_symbol"] == c]
                df_co_01 = df_temp_01[df_temp_01["contract_type"] == "CO"]["exercise_price"].unique().tolist()
                df_po_01 = df_temp_01[df_temp_01["contract_type"] == "PO"]["exercise_price"].unique().tolist()
                df_co_01.sort()
                df_po_01.sort()
                self.dic[c]["01"] = {"CO": df_co_01, "PO": df_po_01, "df": df_temp_01}
            else:
                df_temp_01 = self.daily_01[self.daily_01["underlying_symbol"] == c]
                df_co_01 = df_temp_01[df_temp_01["contract_type"] == "CO"]["exercise_price"].unique().tolist()
                df_po_01 = df_temp_01[df_temp_01["contract_type"] == "PO"]["exercise_price"].unique().tolist()
                df_co_01.sort()
                df_po_01.sort()
                self.dic[c]["00"] = {"CO": df_co_01, "PO": df_po_01, "df": df_temp_01}

                df_temp_02 = self.daily_02[self.daily_02["underlying_symbol"] == c]
                df_co_02 = df_temp_02[df_temp_02["contract_type"] == "CO"]["exercise_price"].unique().tolist()
                df_po_02 = df_temp_02[df_temp_02["contract_type"] == "PO"]["exercise_price"].unique().tolist()
                df_co_02.sort()
                df_po_02.sort()
                self.dic[c]["01"] = {"CO": df_co_02, "PO": df_po_02, "df": df_temp_02}

    def get_preset(self, start, end):
        self.df = self.get_price(security=self.targetcodes, fields=['close'], frequency='1m',
                                 start_date=start, end_date=end)
        self.df["vix"] = 0.0
        # self.df.set_index("time", inplace=True)

    def figure_sigma(self, start, end):
        for i in self.dic:
            df_temp_00 = self.dic[i]["00"]["df"]
            df_temp_01 = self.dic[i]["01"]["df"]
            # df_temp_02 = self.dic[i]["02"]["df"]

            sigma00t00 = self.process_df(df_temp_00, start, end)
            sigma00t00.reset_index(inplace=True)
            # sigma00t00.set_index("time", inplace=True)
            del sigma00t00["level_1"]

            sigma01t01 = self.process_df(df_temp_01, start, end)
            sigma01t01.reset_index(inplace=True)
            # sigma01t01.set_index("time", inplace=True)
            del sigma01t01["level_1"]

            df_sigma = sigma00t00.merge(sigma01t01, how="inner", on="time")
            # df_sigma.set_index("time", inplace=True)
            df_sigma["vix"] = 0.0

            n365_30 = 365 / 30
            n30 = 30 / 365

            for j in df_sigma.index:
                sigma00, sigma01, t00, t01 = df_sigma.loc[j][["sigma_x", "sigma_y", "days_x", "days_y"]]

                part1 = t00 * sigma00 * ((t01 - n30) / (t01 - t00))
                part2 = t01 * sigma01 * ((n30 - t00) / (t01 - t00))

                vix = 100 * (n365_30 * (part1 + part2)) ** 0.5
                df_sigma.loc[j, "vix"] = vix

            # df = df_sigma[["vix"]]
            # df["targetcode"] = i
            index = self.df[self.df["code"] == i].index

            self.df.loc[index, "vix"] = df_sigma["vix"].tolist()

            print(self.df)

    def group_f1(self, df, start, end):
        df.dropna(inplace=True)

        co_code = df[df["contract_type"] == "CO"].iloc[0]["code"]
        po_code = df[df["contract_type"] == "PO"].iloc[0]["code"]

        co = self.get_price(security=co_code, fields=['close'], frequency='1m', start_date=start, end_date=end)
        po = self.get_price(security=po_code, fields=['close'], frequency='1m', start_date=start, end_date=end)

        co_price = co["close"]
        po_price = po["close"]
        difference = abs(co_price - po_price)

        return pandas.DataFrame({"diff": difference, "CO": co_price, "PO": po_price, "days": df.iloc[0]["days"]})

    def group_f2(self, df):
        # df.set_index("exercise_price",inplace=True)
        least = df["diff"].min()

        min_ = df[df["diff"] == least].iloc[0]
        diff = min_["diff"]
        exe_price = min_["exercise_price"]

        f = exe_price + diff * math.exp(self.r * min_["days"])
        # print(f)
        df["avg"] = (df["CO"] + df["PO"]) / 2
        days = df.iloc[0]['days']

        # df_time = df.set_index("time")
        # g2 = df_time.groupby(df_time.index)
        # df_final = g2.apply(self.group_f2)

        price_list = df[["exercise_price", "avg"]].values.tolist()

        # print(df_t)

        df.set_index("exercise_price", inplace=True)
        df.sort_index(inplace=True)

        exercise_price_list = df.index[::-1]
        k = exercise_price_list[-1]
        for i in exercise_price_list:
            if i < f:
                k = i
                break

        # print(df)
        # print(k)

        sigma = -((f / k - 1) ** 2) / days

        n_co = len(price_list)
        for i in range(n_co):
            t = 2 / days

            k_, q = price_list[i]

            if i == 0:
                delta_k = price_list[1][0] - k_
            elif i == n_co - 1:
                delta_k = k_ - price_list[i - 1][0]
            else:
                delta_k = (price_list[i + 1][0] - price_list[i - 1][0]) / 2

            sigma += t * (delta_k / k_ ** 2) * math.exp(self.r * days) * q

        return pandas.DataFrame({"sigma": [sigma], "days": [days]})

    def process_df(self, df, start, end):
        g = df.groupby("exercise_price")
        df = g.apply(self.group_f1, start=start, end=end)
        df.reset_index(inplace=True)
        df.rename(columns={"level_1": "time"}, inplace=True)
        df.set_index("time", inplace=True)

        g2 = df.groupby(df.index)
        df = g2.apply(self.group_f2)
        return df

    def get(self, **kwargs):
        start = kwargs["start"]
        end = kwargs["end"]

        # times = SplitTime.split(start, end, interval_day=1)

        self.get_adjust()
        self.get_preset(start=start, end=end)
        self.daily_info(start=start, end=end)
        self.figure_sigma(start=start, end=end)

        if self.df is None:
            return None, None

        self.df["time"] = pandas.DatetimeIndex(self.df["time"], tz='Asia/Shanghai')
        self.df.set_index("time", inplace=True)
        self.df.rename(columns={'code': 'targetcode', "close": 'price'}, inplace=True)
        tag_columns = ['targetcode']

        return self.df, tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_rows', None)
    op = OpVix()
    start = "2023-03-15 10:00:00"
    end = "2023-03-15 10:01:00"
    a, b = op.get(start=start, end=end)
    print(a)
