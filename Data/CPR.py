# -*- coding: utf-8 -*-
# @Name     : CPR.py
# @Date     : 2023/3/8 10:33
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
import time

import pandas
from jqdatasdk import get_price, query, opt
from sqlalchemy import or_
from thriftpy2.transport import TTransportException

from utils.InfluxTime import SplitTime, InfluxTime
from service.JoinQuant import JQData
import datetime


class CPR(JQData):
    def __init__(self):
        super().__init__()
        self.df = None
        self.indicator = True

        self.dic1 = dict()
        self.dic2 = dict()

    @staticmethod
    def group_f1(df):
        df.dropna(inplace=True)
        co = df[df["contract_type"] == "CO"]["code"].to_list()
        po = df[df["contract_type"] == "PO"]["code"].to_list()
        return [co, po]

    @staticmethod
    def group_f2(df):
        df.dropna(inplace=True)
        vol = df["volume"].sum()
        money = df["money"].sum()
        open_interest = df["open_interest"].sum()
        close = df["close"].iloc[0]
        return pandas.DataFrame({"volume": [vol], "money": [money], "open_interest": [open_interest],
                                 "close": [close]})

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
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000016.XSHG",
                ),
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
                opt.OPT_CONTRACT_INFO.underlying_symbol == "000016.XSHG",
                ),
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

    def get_data(self, yesterday, start, end):
        # start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        # end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        for target_code in self.dic2:
            lst = []
            for s, e, d in [(start, end, self.dic2), (yesterday, end, self.dic1), ]:
                for type_ in ["CO", "PO"]:
                    # print(target_code, s, e, type_)

                    df1 = self.get_price(security=d[target_code][type_], start_date=s, end_date=e, fq='pre',
                                         frequency='60m', fields=['volume', 'money', 'open_interest'],
                                         panel=False)

                    df2 = self.get_price(security=target_code, start_date=s, end_date=e, fq='pre',
                                         frequency='60m', fields=['close'],
                                         panel=False)

                    df1.set_index("time", inplace=True)
                    df1["close"] = df2

                    if len(df1) == 0:
                        self.df = None
                        return
                    df1 = df1.groupby("time").apply(self.group_f2)
                    df1.reset_index("time", inplace=True)
                    df1.set_index("time", inplace=True)
                    lst.append(df1)

            # today CO today PO yes CO yes PO

            df_t_c, df_t_p, df_y_c, df_y_p = lst
            close_t = df_t_c["close"]
            del df_t_c["close"], df_t_p["close"], df_y_c["close"], df_y_p["close"]

            # print(1, df_t_c)
            # print(2, df_t_p)
            # print(3, df_y_c)
            # print(4, df_y_p)

            # lst = ['volume', 'money', 'open_interest']
            length_t = len(df_t_c)
            for i in range(length_t - 1, -1, -1):
                df_t_c.iloc[i] = df_t_c.iloc[:i + 1].sum() / df_t_p.iloc[:i + 1].sum()

            # print(1, df_t_c)
            length_y = len(df_y_c)
            for i in range(length_y - 1, 3, -1):
                df_y_c.iloc[i] = df_y_c.iloc[i - 3:i + 1].sum() / df_y_p.iloc[i - 3:i + 1].sum()

            # print(2, df_y_c)
            # df_y_c.drop(columns=["close"], axis=1, inplace=True)

            df_t_c["close"] = close_t

            df_temp = df_t_c.merge(df_y_c, how='inner', on='time')
            # print(3, df_temp)
            df_temp["targetcode"] = target_code

            # df_t_c.drop(columns=["close_y"], axis=1, inplace=True)

            self.df = pandas.concat([df_temp, self.df])

    def find_valid_yesterday(self, yesterday):
        while True:
            df1 = self.get_price(security="510050.XSHG", start_date=yesterday, end_date=yesterday,
                                 fq='pre', frequency='1d', fields=['volume'], panel=False)
            if len(df1) != 0:
                return yesterday
            else:
                yesterday -= datetime.timedelta(days=1)

    def attach_targetprice(self, df):
        pass

    def get(self, **kwargs):
        times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=1)
        for t in times:
            # print(t)
            time_ = datetime.datetime.strptime(t[0], "%Y-%m-%d %H:%M:%S")
            yesterday = time_ - datetime.timedelta(days=1)
            yesterday = self.find_valid_yesterday(yesterday)
            today = time_.replace(hour=0, minute=0, second=0, microsecond=0)
            tmr = today + datetime.timedelta(days=1)

            self.get_pre_data()
            self.get_data(yesterday.__str__(), today.__str__(), tmr.__str__())

        if self.df is None:
            return None, None

        self.df.index = pandas.DatetimeIndex(self.df.index, tz='Asia/Shanghai')
        # self.df.set_index("time", inplace=True)
        self.df.rename(columns={'volume_x': 'volume', "money_x": 'money', "open_interest_x": 'oi',
                                'volume_y': 'volume_scroll', "money_y": 'money_scroll',
                                "open_interest_y": 'oi_scroll', "close": "price"}, inplace=True)
        tag_columns = ['targetcode']
        # print(self.df)
        return self.df, tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_rows', None)
    pandas.set_option('display.max_columns', None)
    op = CPR()
    start = "2023-03-01 00:00:00"
    end = "2023-03-02 00:00:00"
    # op.get_pre_data()
    # a = op.get_data(start=start, end=end)
    # print(a)
    a, b = op.get(start=start, end=end)
    print(a)

    # df = get_bars(security="510050.XSHG", unit='1m', count=10, fields=['close'])
    # print(df)
