# -*- coding: utf-8 -*-
# @Name     : OpSkew.py
# @Date     : 2023/3/17 13:31
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


class OpSkew(JQData):
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
            if df_temp_00["days"].min() > 8 / 365:
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
        if len(self.df) == 0:
            return None

        self.df["skew"] = 0.0
        return True

    def figure_sigma(self, start, end):
        for i in self.dic:
            df_temp_00 = self.dic[i]["00"]["df"]
            df_temp_01 = self.dic[i]["01"]["df"]

            s00t00 = self.process_df(df_temp_00, start, end)
            s00t00.reset_index(inplace=True)
            del s00t00["level_1"]

            s01t01 = self.process_df(df_temp_01, start, end)
            s01t01.reset_index(inplace=True)
            del s01t01["level_1"]

            df_skew = s00t00.merge(s01t01, how="inner", on="time")
            df_skew["skew"] = 0.0

            n30 = 30 / 365

            for j in df_skew.index:
                s0, s1, t00, t01 = df_skew.loc[j][["s_x", "s_y", "days_x", "days_y"]]
                time_3060 = n30
                # 如果本月合约到期日大于30天（即取下月合约的情况），则到期日按照60天计算
                if t00 > 30 / 365:
                    time_3060 *= 2

                w = (t01 - time_3060) / (t01 - t00)
                skew = 100 - 10 * (w * s0 + (1 - w) * s1)

                df_skew.loc[j, "skew"] = skew

            index = self.df[self.df["code"] == i].index
            self.df.loc[index, "skew"] = df_skew["skew"].tolist()

    def process_df(self, df, start, end):
        g = df.groupby("exercise_price")
        df = g.apply(self.group_f1, start=start, end=end)
        df.reset_index(inplace=True)
        df.rename(columns={"level_1": "time"}, inplace=True)
        df.set_index("time", inplace=True)

        g2 = df.groupby(df.index)
        df = g2.apply(self.group_f2)

        return df

    def group_f1(self, df, start, end):
        df.dropna(inplace=True)

        co_code = df[df["contract_type"] == "CO"].iloc[0]["code"]
        po_code = df[df["contract_type"] == "PO"].iloc[0]["code"]

        co = self.get_price(security=co_code, fields=['close'], frequency='1m', start_date=start, end_date=end)
        po = self.get_price(security=po_code, fields=['close'], frequency='1m', start_date=start, end_date=end)

        co_price = co["close"]
        po_price = po["close"]

        # 认沽认购差的绝对值
        difference = abs(co_price - po_price)
        # 认沽认购的差
        diff2 = co_price - po_price

        return pandas.DataFrame({"diff": difference, "days": df.iloc[0]["days"], "diff2": diff2, "co": co_price,
                                 "po": po_price})

    def group_f2(self, df):
        df["mix"] = 0.0
        least = df["diff"].min()

        min_ = df[df["diff"] == least].iloc[0]
        diff = min_["diff2"]
        exe_price = min_["exercise_price"]

        f = exe_price + diff * math.exp(self.r * min_["days"])

        df["avg"] = (df["CO"] + df["PO"]) / 2
        days = df.iloc[0]['days']

        df.set_index("exercise_price", inplace=True)
        df.sort_index(inplace=True)

        exercise_price_list = df.index[::-1]
        k = exercise_price_list[-1]
        for i in exercise_price_list:
            if i < f:
                k = i
                break

        for i in df.index:
            if i < k:
                df.loc[i, "mix"] = df.loc[i, "po"]
            elif i == k:
                df.loc[i, "mix"] = (df.loc[i, "po"] + df.loc[i, "co"]) / 2
            else:
                df.loc[i, "mix"] = df.loc[i, "co"]

        df.reset_index(inplace=True)
        price_list = df[["exercise_price", "mix"]].values.tolist()

        k_f = k / f
        f_k = f / k

        p1 = f_k - 1 - math.log(f_k)
        p2 = 2 * math.log(k_f) * (f_k - 1) + math.log(k_f) ** 2 / 2
        p3 = 3 * math.log(k_f) ** 2 * (f_k - 1 + math.log(f_k) / 3)
        ert = math.exp(self.r * days)

        n_co = len(price_list)
        for i in range(n_co):
            k_, q = price_list[i]
            k_f = k_ / f

            if i == 0:
                delta_k = price_list[1][0] - k_
            elif i == n_co - 1:
                delta_k = k_ - price_list[i - 1][0]
            else:
                delta_k = (price_list[i + 1][0] - price_list[i - 1][0]) / 2

            p1 += ert * (-delta_k * q / k_ ** 2)
            p2 += ert * (2 / k_ ** 2 * (1 - math.log(k_f) * delta_k * q))
            p3 += ert * (3 / k_ ** 2 * (2 * math.log(k_f) - math.log(k_f) ** 2 * delta_k * q))

        s = (p3 - 3 * p1 * p2 + 2 * p1 ** 3) / (p2 - p1 ** 2) ** (3 / 2)

        return pandas.DataFrame({"s": [s], "days": [days]})

    def get(self, **kwargs):
        start = kwargs["start"]
        end = kwargs["end"]

        # times = SplitTime.split(start, end, interval_day=1)

        self.get_adjust()
        ind = self.get_preset(start=start, end=end)
        if not ind:
            return None, None
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
    op = OpSkew()
    start = "2023-03-15 10:00:00"
    end = "2023-03-15 10:01:00"
    a, b = op.get(start=start, end=end)
    print(a)
