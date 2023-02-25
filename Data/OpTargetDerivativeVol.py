# -*- coding: utf-8 -*-
# @Name     : OpTargetDerivativeVol.py
# @Date     : 2023/2/25 14:52
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pandas
from jqdatasdk import get_price
from utils.InfluxTime import SplitTime
from service.JoinQuant import JQData
import datetime


class OpTargetDerivativeVol(JQData):
    def __init__(self):
        super().__init__()
        self.dic = dict()

        self.df_1d = None
        self.df_2h = None
        self.df_1h = None

    def get_data(self, start, end):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        start_ = start - datetime.timedelta(days=300)
        df = get_price(security=["510050.XSHG", "510500.XSHG"], start_date=start_, end_date=end, fq='pre',
                       frequency='minute', fields=['close'], panel=False)
        if len(df) == 0:
            return
        df["time"] -= pandas.Timedelta(minutes=1)

        for code in ["510050.XSHG"]:  # self.targetcodes["510050.XSHG"]
            self.dic[code] = None

            df_temp = df[df["code"] == code]
            df_temp.set_index("time", inplace=True)

            df_1d = df_temp.resample("1d").first()
            df_2h = df_temp.resample("2h").first()
            df_1h = df_temp.resample("1h").first()

            df_1d["close"] = (df_1d["close"].shift() - df_1d["close"]).div(df_1d["close"].shift())
            df_2h["close"] = (df_2h["close"].shift() - df_2h["close"]).div(df_2h["close"].shift())
            df_1h["close"] = (df_1h["close"].shift() - df_1h["close"]).div(df_1h["close"].shift())

            df_1d.dropna(inplace=True)
            df_1d.reset_index(inplace=True)
            df_2h.dropna(inplace=True)
            df_2h.reset_index(inplace=True)
            df_1h.dropna(inplace=True)
            df_1h.reset_index(inplace=True)

            # print(df_1d)
            for df_, multiplier, prefix in [(df_1d, 250 ** 0.5, "1d"), (df_2h, 500, "2h"), (df_1h, 1000, "1h")]:  #
                for interval in [20, 40, 60, 120]:  #
                    df = df_.copy()
                    df[f"{prefix}_{interval}_max"] = 0
                    df["min"] = 0
                    df["mean"] = 0
                    df["80"] = 0
                    df["20"] = 0
                    df["now"] = 0

                    for index in range(interval, len(df)):
                        df.loc[index, "now"] = df.loc[index - interval:index, "close"].std() * multiplier
                        if index >= 2 * interval:
                            df.loc[index, "max"] = df.loc[index - interval:index, "now"].max()
                            df.loc[index, "min"] = df.loc[index - interval:index, "now"].min()
                            df.loc[index, "mean"] = df.loc[index - interval:index, "now"].mean()
                            df.loc[index, "20"] = df.loc[index - interval:index, "now"].quantile(0.2)
                            df.loc[index, "80"] = df.loc[index - interval:index, "now"].quantile(0.8)

                    df.drop(index=list(range(2 * interval)), inplace=True, axis=0)
                    df.drop(columns=["close"], inplace=True, axis=1)

    # def process_df(self, df):
    #     df["pct"] = (df["close"] - df["pre_close"]) / df["pre_close"]
    #     del df["pre_close"]
    #
    #     print(len(df))
    #     if df is not None and len(df) > 0:
    #         self.df = pandas.concat([self.df, df])

    def get(self, **kwargs):
        times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=30)
        for t in times:
            print(t)
            self.get_data(t[0], t[1])
            self.process_df(df)

        if self.df is None:
            return None, None

        self.df["time"] = pandas.DatetimeIndex(self.df["time"], tz='Asia/Shanghai')
        self.df.set_index("time", inplace=True)
        self.df.rename(columns={'code': 'targetcode'}, inplace=True)
        tag_columns = ['targetcode']

        # print(self.df)

        return self.df, tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_rows', None)
    pandas.set_option('display.max_columns', None)
    op = OpTargetDerivativeVol()
    start = "2023-02-15 00:00:00"
    end = "2023-02-22 00:00:00"
    # a = op.get(start=start, end=end)
    a = op.get_data(start, end)
    # df = get_bars(security="510050.XSHG", unit='1m', count=10, fields=['close'])
    # print(df)
