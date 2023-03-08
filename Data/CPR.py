# -*- coding: utf-8 -*-
# @Name     : CPR.py
# @Date     : 2023/3/8 10:33
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pandas
from jqdatasdk import get_price, query, opt
from sqlalchemy import or_

from utils.InfluxTime import SplitTime, InfluxTime
from service.JoinQuant import JQData
import datetime


class CPR(JQData):
    def __init__(self):
        super().__init__()
        self.df = None
        self.targetcodes = ["510050.XSHG", "510500.XSHG"]
        self.indicator = True

        self.dic1 = dict()
        self.dic2 = dict()

        self.df_result = None

    @staticmethod
    def group_f1(df):
        co = df[df["contract_type"] == "CO"]["code"].to_list()
        po = df[df["contract_type"] == "PO"]["code"].to_list()
        return [co, po]

    @staticmethod
    def group_f2(df):
        vol = df["volume"].sum()
        money = df["money"].sum()
        open_interest = df["open_interest"].sum()
        return pandas.DataFrame({"volume": [vol], "money": [money], "open_interest": [open_interest]})

    def get_pre_data(self):
        start, end = InfluxTime.this_day()
        mid, _ = InfluxTime.today()

        q1 = query(opt.OPT_CONTRACT_INFO.list_date,
                   opt.OPT_CONTRACT_INFO.code,
                   opt.OPT_CONTRACT_INFO.underlying_symbol,
                   opt.OPT_CONTRACT_INFO.contract_type).filter(
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
            opt.OPT_CONTRACT_INFO.expire_date >= mid, )

        q2 = query(opt.OPT_CONTRACT_INFO.code,
                   opt.OPT_CONTRACT_INFO.underlying_symbol,
                   opt.OPT_CONTRACT_INFO.contract_type).filter(
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
            opt.OPT_CONTRACT_INFO.list_date <= mid,
            opt.OPT_CONTRACT_INFO.expire_date >= end, )

        df1 = opt.run_query(q1)
        if len(df1) == 0:
            return

        g1 = df1.groupby("underlying_symbol")
        df_1 = g1.apply(self.group_f1)
        dic1 = {index: {"CO": df_1[index][0], "PO": df_1[index][1]} for index in df_1.index}
        self.dic1 = dic1

        df2 = opt.run_query(q2)
        if len(df2) == 0:
            return

        g2 = df2.groupby("underlying_symbol")
        df_2 = g2.apply(self.group_f1)
        dic2 = {index: {"CO": df_2[index][0], "PO": df_2[index][1]} for index in df_2.index}
        self.dic2 = dic2

    def get_data(self, start, end):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        for target_code in self.dic2:
            df = get_price(security=self.dic2[target_code]["CO"], start_date=start, end_date=end, fq='pre',
                           frequency='60m', fields=['volume', 'money', 'open_interest'], panel=False)

            df = df.groupby("time").apply(self.group_f2)
            df.reset_index("time", inplace=True)
            df.set_index("time", inplace=True)

            df_po = get_price(security=self.dic2[target_code]["PO"], start_date=start, end_date=end, fq='pre',
                              frequency='60m', fields=['volume', 'money', 'open_interest'], panel=False)

            df_po = df_po.groupby("time").apply(self.group_f2)
            df_po.reset_index("time", inplace=True)
            df_po.set_index("time", inplace=True)

            df["money"] = df["money"] / df_po["money"]
            df["volume"] = df["volume"] / df_po["volume"]
            df["open_interest"] = df["open_interest"] / df_po["open_interest"]

            df["targetcode"] = target_code
            self.df = pandas.concat([df, self.df])
        return self.df

    def get(self, **kwargs):
        times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=30)
        for t in times:
            print(t)
            self.get_pre_data()
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
    op = CPR()
    start = "2023-03-07 00:00:00"
    end = "2023-03-07 18:00:00"
    op.get_pre_data()
    a = op.get_data(start=start, end=end)
    print(a)

    # df = get_bars(security="510050.XSHG", unit='1m', count=10, fields=['close'])
    # print(df)
