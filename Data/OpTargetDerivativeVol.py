# -*- coding: utf-8 -*-
# @Name     : OpTargetDerivativeVol.py
# @Date     : 2023/2/25 14:52
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime

import pandas
from jqdatasdk import get_price

from service.JoinQuant import JQData


class OpTargetDerivativeVol(JQData):
    def __init__(self):
        super().__init__()

        self.pre_dic = dict()

        # self.targetcodes = ["510050.XSHG", "510500.XSHG"]

        self.result_dic = {i: {"1d_volatility": None, "2h_volatility": None, "1h_volatility": None}
                           for i in self.targetcodes}

    def get_data(self, start, end):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        start_ = start - datetime.timedelta(days=500)

        df = get_price(security=self.targetcodes, start_date=start_, end_date=end, fq='pre',
                       frequency='minute', fields=['close'], panel=False)
        if len(df) == 0:
            return
        df["time"] -= pandas.Timedelta(minutes=1)

        for code in self.targetcodes:
            df_temp = df[df["code"] == code]
            df_temp.set_index("time", inplace=True)

            df_1h = df_temp.resample("1H").first().dropna(how="any")
            df_2h = df_temp.resample("2H").first().dropna(how="any")
            df_1d = df_temp.resample("1d").first().dropna(how="any")

            df_1d["price"] = df_1d["close"]
            df_2h["price"] = df_2h["close"]
            df_1h["price"] = df_1h["close"]

            df_1d["close"] = (df_1d["close"] - df_1d["close"].shift()).div(df_1d["close"].shift())
            df_2h["close"] = (df_2h["close"] - df_2h["close"].shift()).div(df_2h["close"].shift())
            df_1h["close"] = (df_1h["close"] - df_1h["close"].shift()).div(df_1h["close"].shift())

            df_1d.dropna(inplace=True)
            df_1d.reset_index(inplace=True)
            df_2h.dropna(inplace=True)
            df_2h.reset_index(inplace=True)
            df_1h.dropna(inplace=True)
            df_1h.reset_index(inplace=True)

            self.pre_dic[code] = [[df_1d, 250 ** 0.5, "1d_volatility"], [df_2h, 500 ** 0.5, "2h_volatility"],
                                  [df_1h, 1000 ** 0.5, "1h_volatility"]]

    def process_df(self):
        for code in self.targetcodes:
            for interval in [20, 40, 60, 120]:
                for df_, multiplier, prefix in self.pre_dic[code]:
                    print(code, interval, prefix)
                    if len(df_) < 1:
                        continue

                    df = df_.copy()

                    columns = [f"{prefix}_{interval}_now",
                               f"{prefix}_{interval}_{interval}_max",
                               f"{prefix}_{interval}_{interval}_min",
                               f"{prefix}_{interval}_{interval}_mean",
                               f"{prefix}_{interval}_{interval}_80",
                               f"{prefix}_{interval}_{interval}_20"]

                    for column in columns:
                        df[column] = 0.0

                    # df.to_excel(f"{code}{interval}{prefix}_1.xlsx")

                    for index in range(interval, len(df)):
                        df.loc[index, columns[0]] = df.loc[index + 1 - interval:index, "close"].std() * multiplier
                        if index >= 2 * interval:
                            df.loc[index, columns[1]] = df.loc[index + 1 - interval:index, columns[0]].max()
                            df.loc[index, columns[2]] = df.loc[index + 1 - interval:index, columns[0]].min()
                            df.loc[index, columns[3]] = df.loc[index + 1 - interval:index, columns[0]].mean()
                            df.loc[index, columns[4]] = df.loc[index + 1 - interval:index, columns[0]].quantile(0.8)
                            df.loc[index, columns[5]] = df.loc[index + 1 - interval:index, columns[0]].quantile(0.2)

                    # df.to_excel(f"{code} {interval} {prefix}.xlsx")
                    # break
                    # print(df)

                    df.drop(index=list(range(2 * interval)), inplace=True, axis=0)
                    df.drop(columns=["close"], inplace=True, axis=1)
                    # print(df.head())

                    retrieve = self.result_dic[code][prefix]
                    if retrieve is None:
                        self.result_dic[code][prefix] = df
                    else:
                        df.drop(columns=["code", "price"], inplace=True, axis=1)
                        self.result_dic[code][prefix] = pandas.merge(retrieve, df, how="inner", on="time")

    def get(self, **kwargs):
        # times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=30)
        # for t in times:
        #     print(t)
        #     self.get_data(t[0], t[1])
        #     self.process_df()

        self.get_data(kwargs["start"], kwargs["end"])
        self.process_df()

        vol_1d = [self.result_dic[i]["1d_volatility"] for i in self.targetcodes]
        vol_2h = [self.result_dic[i]["2h_volatility"] for i in self.targetcodes]
        vol_1h = [self.result_dic[i]["1h_volatility"] for i in self.targetcodes]

        vol_1d = pandas.concat(vol_1d)
        vol_2h = pandas.concat(vol_2h)
        vol_1h = pandas.concat(vol_1h)

        # vol_1d["time"] = pandas.DatetimeIndex(vol_1d["time"], tz='Asia/Shanghai')
        # vol_1d.set_index("time", inplace=True)
        # vol_1d.rename(columns={'code': 'targetcode'}, inplace=True)
        # vol_1d.dropna(inplace=True)
        #
        # vol_2h["time"] = pandas.DatetimeIndex(vol_2h["time"], tz='Asia/Shanghai')
        # vol_2h.set_index("time", inplace=True)
        # vol_2h.rename(columns={'code': 'targetcode'}, inplace=True)
        # vol_2h.dropna(inplace=True)
        #
        # vol_1h["time"] = pandas.DatetimeIndex(vol_1h["time"], tz='Asia/Shanghai')
        # vol_1h.set_index("time", inplace=True)
        # vol_1h.rename(columns={'code': 'targetcode'}, inplace=True)
        # vol_1h.dropna(inplace=True)

        result_df = []
        for v in [vol_1d, vol_2h, vol_1h]:
            v["time"] = pandas.DatetimeIndex(v["time"], tz='Asia/Shanghai')
            v.set_index("time", inplace=True)
            v.rename(columns={'code': 'targetcode'}, inplace=True)
            v.dropna(inplace=True)
            result_df.append(v)

        name_list = ["optargetderivativevol_1d", "optargetderivativevol_2h", "optargetderivativevol_1h"]
        tag_columns = ['targetcode']
        return list(zip(result_df, name_list)), tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_rows', None)
    pandas.set_option('display.max_columns', None)
    op = OpTargetDerivativeVol()
    start = "2023-03-15 00:00:00"
    end = "2023-03-16 00:00:00"

    a, b = op.get(start=start, end=end)
    x, y, z = a
    # print(x[0].columns)
    # print(y[0].columns)
    # print(z[0].columns)
    #
    # print(x[0])
