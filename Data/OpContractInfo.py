# -*- coding: utf-8 -*-
# @Name     : OpContractInfo.py
# @Date     : 2022/9/14 9:09
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 合约信息

import pandas
from jqdatasdk import opt, query
from sqlalchemy import or_

from utils.InfluxTime import SplitTime, InfluxTime
from utils.JoinQuant import Authentication


class OpContractInfo(metaclass=Authentication):
    def __init__(self):
        self.df = None
        self.adjust = None

    def get_adjust(self, start, end):
        q = query(opt.OPT_ADJUSTMENT.adj_date,
                  opt.OPT_ADJUSTMENT.code,
                  opt.OPT_ADJUSTMENT.ex_exercise_price,
                  opt.OPT_ADJUSTMENT.ex_contract_unit,
                  # opt.OPT_ADJUSTMENT.new_exercise_price,
                  # opt.OPT_ADJUSTMENT.new_contract_unit,
                  )
        df = opt.run_query(q)
        self.adjust = df
        return df

    @staticmethod
    def get_data(start, end):
        q = query(opt.OPT_CONTRACT_INFO.list_date,
                  opt.OPT_CONTRACT_INFO.code,
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

            opt.OPT_CONTRACT_INFO.list_date >= start,
            opt.OPT_CONTRACT_INFO.list_date <= end, )

        df = opt.run_query(q)
        return df

    def process_df(self, df):
        # df['days'] = (df["expire_date"] - df["date"]).apply(lambda x: x.days)

        if df is not None and len(df) > 0:
            self.df = pandas.concat([self.df, df])
        print(len(df))

    def get(self, **kwargs):
        times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=30)
        self.get_adjust(kwargs["start"], kwargs["end"])
        for t in times:
            print(t)
            df = self.get_data(t[0], t[1])
            self.process_df(df)

        # print(self.df, self.adjust)

        self.df = pandas.merge(left=self.df, right=self.adjust, on="code", how="left")

        self.df["adj_date"].fillna("1980-01-01", inplace=True)
        self.df["list_date"] = pandas.DatetimeIndex(self.df["list_date"], tz='Asia/Shanghai')

        # self.df["expire_date"] = pandas.to_datetime(self.df["expire_date"], utc=True)
        # self.df["expire_date"] = self.df["expire_date"].dt.tz_convert('Asia/Shanghai')

        # self.df["adj_date"] = pandas.to_datetime(self.df["adj_date"], utc=True)
        # self.df["adj_date"] = self.df["adj_date"].dt.tz_convert('Asia/Shanghai')

        self.df.fillna(0, inplace=True)
        self.df.set_index(["list_date"], inplace=True)

        # ['code', 'underlying_symbol', 'exercise_price', 'contract_type','contract_unit', 'days']
        self.df.drop_duplicates(inplace=True)

        self.df.rename(columns={'code': 'opcode', "underlying_symbol": "targetcode", "contract_type": "type",
                                "contract_unit": "multiplier", "exercise_price": "strikeprice",
                                "ex_exercise_price": "ex_strikeprice", "ex_contract_unit": "ex_multiplier"},
                       inplace=True)

        tag_columns = ['opcode', 'targetcode', 'type']

        # print(self.df["expire_date"][0])
        self.df["expire_date"] = InfluxTime.utc(*self.df["expire_date"].astype(str).values, timestamp_=True)
        self.df["adj_date"] = InfluxTime.utc(*self.df["adj_date"].astype(str).values, timestamp_=True)
        # self.df[["expire_date", "adj_date"]] = self.df[["expire_date", "adj_date"]].values

        return self.df, tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_rows', None)
    pandas.set_option('display.max_columns', None)
    opc = OpContractInfo()
    # opc.get(start='2023-01-01 00:00:00', end='2023-02-14 00:00:00')
    # a, b = opc.get(start='2020-01-01 00:00:00', end='2020-10-01 00:00:00')
    # print(a.iloc[0])
    # print(opc.get_adjust(start='2020-01-01 00:00:00', end='2020-10-01 00:00:00'))
    # a = opc.get_adjust(start='2020-01-01 00:00:00', end='2020-10-01 00:00:00')
    # print(a)
    # print(opc.get_data(1, 1))
    # a = opc.get_data(start='2020-01-01 00:00:00', end='2020-02-01 00:00:00')
    a, _ = opc.get(start='2022-02-10 00:00:00', end='2023-02-20 00:00:00')
    # print(a[a["is_adjust"] == 1])
    # print(type(a.iloc[0]["expire_date"]))
    # print(list(a["opcode"].unique()))
    # print(a.duplicated())
    print(a)
