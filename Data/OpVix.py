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
        self.targetcodes = ["510050.XSHG", "510500.XSHG"]

        self.indicator = True
        self.scroll = False

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
                # opt.OPT_CONTRACT_INFO.underlying_symbol == "510300.XSHG",
                # opt.OPT_CONTRACT_INFO.underlying_symbol == "159901.XSHE",
                # opt.OPT_CONTRACT_INFO.underlying_symbol == "159919.XSHE",
                # opt.OPT_CONTRACT_INFO.underlying_symbol == "159915.XSHE",
                # opt.OPT_CONTRACT_INFO.underlying_symbol == "159922.XSHE",
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
                # opt.OPT_CONTRACT_INFO.underlying_symbol == "000300.XSHG",
                # opt.OPT_CONTRACT_INFO.underlying_symbol == "000016.XSHG",
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

        if self.daily_00["days"].min() < 8 / 365:
            self.scroll = True

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

            # print(df_temp_02)
            self.dic[c]["02"] = {"CO": df_co_02, "PO": df_po_02}

    def get_data(self, start, end):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        df_pre = None

        if start.time() > datetime.time(9, 30):
            start_temp = start.replace(hour=9, minute=30, second=0, microsecond=0)
            end_temp = end.replace(hour=9, minute=31, second=0, microsecond=0)
            df_pre = self.get_price(security=self.targetcodes, start_date=start_temp, end_date=end_temp, fq='pre',
                                    frequency='minute', fields=['pre_close'], panel=False)

            df_pre = df_pre[["code", "pre_close"]].values.tolist()

        df = self.get_price(security=self.targetcodes, start_date=start, end_date=end, fq='pre', frequency='minute',
                            fields=['close', 'pre_close'], panel=False)
        # print(df)
        if len(df) == 0:
            return

        df["time"] -= pandas.Timedelta(minutes=1)

        if df_pre:
            for i, j in df_pre:
                indexes = df[df["code"] == i].index
                df.loc[indexes, "pre_close"] = j
        else:
            temp = df.iloc[0]["time"]
            for i in range(len(df)):
                if datetime.time(9, 30) == df.iloc[i]["time"].time():
                    temp = df.iloc[i]["pre_close"]
                else:
                    df.iloc[i, 3] = temp

        df = df[(df["time"] >= start) & (df["time"] <= end)]

        return df

    def process_df(self, df):
        df["pct"] = (df["close"] - df["pre_close"]) / df["pre_close"]
        del df["pre_close"]

        print(len(df))
        if df is not None and len(df) > 0:
            self.df = pandas.concat([self.df, df])

    def get(self, **kwargs):
        times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=30)
        for t in times:
            print(t)
            df = self.get_data(t[0], t[1])
            if df is not None:
                self.process_df(df)

        if self.df is None:
            return None, None

        self.df["time"] = pandas.DatetimeIndex(self.df["time"], tz='Asia/Shanghai')
        self.df.set_index("time", inplace=True)
        self.df.rename(columns={'code': 'targetcode', "close": 'price'}, inplace=True)
        tag_columns = ['targetcode']

        # print(self.df)

        return self.df, tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_rows', None)
    op = OpVix()
    start = "2023-03-15 00:00:00"
    end = "2023-03-16 00:00:00"
    op.get_adjust()
    op.daily_info(start=start, end=end)

    d = op.dic
    a0, a1, a2 = op.daily_00, op.daily_01, op.daily_02
    print(a0)
