# -*- coding: utf-8 -*-
# @Name     : OpTargetDerivativePrice.py
# @Date     : 2023/2/25 14:52
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime

import pandas
from jqdatasdk import get_price

from service.JoinQuant import JQData


class OpTargetDerivativePrice(JQData):
    def __init__(self):
        super().__init__()

        self.pre_dic = dict()

        # self.targetcodes = ["510050.XSHG", "510500.XSHG"]

        self.result_dic = {i: {"1d_price": None, "2h_price": None, "1h_price": None,
                               "5m_price": None, "15m_price": None, "30m_price": None}
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

            df_1h = df_temp.resample("1H").first()
            df_2h = df_temp.resample("2H").first()
            df_1d = df_temp.resample("1d").first()

            df_5m = df_temp.resample("5min").first()
            df_15m = df_temp.resample("15min").first()
            df_30m = df_temp.resample("30min").first()

            df_1d.dropna(inplace=True)
            df_1d.reset_index(inplace=True)
            df_2h.dropna(inplace=True)
            df_2h.reset_index(inplace=True)
            df_1h.dropna(inplace=True)
            df_1h.reset_index(inplace=True)

            df_5m.dropna(inplace=True)
            df_5m.reset_index(inplace=True)
            df_15m.dropna(inplace=True)
            df_15m.reset_index(inplace=True)
            df_30m.dropna(inplace=True)
            df_30m.reset_index(inplace=True)

            self.pre_dic[code] = [[df_1d, "1d_price"],
                                  [df_2h, "2h_price"],
                                  [df_1h, "1h_price"],

                                  [df_5m, "5m_price"],
                                  [df_15m, "15m_price"],
                                  [df_30m, "30m_price"]]

    def process_df(self):
        for code in self.targetcodes:
            for interval in [20, 40, 60, 120]:
                for df_, prefix in self.pre_dic[code]:
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

                    for index in range(interval, len(df)):
                        df.loc[index, columns[0]] = df.loc[index + 1 - interval:index, "close"].iloc[0]
                        if index >= 2 * interval:
                            df.loc[index, columns[1]] = df.loc[index + 1 - interval:index, columns[0]].max()
                            df.loc[index, columns[2]] = df.loc[index + 1 - interval:index, columns[0]].min()
                            df.loc[index, columns[3]] = df.loc[index + 1 - interval:index, columns[0]].mean()
                            df.loc[index, columns[4]] = df.loc[index + 1 - interval:index, columns[0]].quantile(0.8)
                            df.loc[index, columns[5]] = df.loc[index + 1 - interval:index, columns[0]].quantile(0.2)

                    # df.to_excel(f"{code} {interval} {prefix}.xlsx")

                    df.drop(index=list(range(2 * interval)), inplace=True, axis=0)
                    # df.drop(columns=["close"], inplace=True, axis=1)
                    # print(df.head().iloc[0])
                    # print(df)
                    retrieve = self.result_dic[code][prefix]
                    if retrieve is None:
                        self.result_dic[code][prefix] = df
                    else:
                        df.drop(columns=["code", "close"], inplace=True, axis=1)
                        self.result_dic[code][prefix] = pandas.merge(retrieve, df, how="inner", on="time")

    def get(self, **kwargs):
        self.get_data(kwargs["start"], kwargs["end"])
        self.process_df()

        vol_1d = [self.result_dic[i]["1d_price"] for i in self.targetcodes]
        vol_2h = [self.result_dic[i]["2h_price"] for i in self.targetcodes]
        vol_1h = [self.result_dic[i]["1h_price"] for i in self.targetcodes]

        vol_5m = [self.result_dic[i]["5m_price"] for i in self.targetcodes]
        vol_15m = [self.result_dic[i]["15m_price"] for i in self.targetcodes]
        vol_30m = [self.result_dic[i]["30m_price"] for i in self.targetcodes]

        vol_1d = pandas.concat(vol_1d)
        vol_2h = pandas.concat(vol_2h)
        vol_1h = pandas.concat(vol_1h)

        vol_5m = pandas.concat(vol_5m)
        vol_15m = pandas.concat(vol_15m)
        vol_30m = pandas.concat(vol_30m)

        result_df = []
        for v in [vol_1d, vol_2h, vol_1h, vol_5m, vol_15m, vol_30m]:
            v["time"] = pandas.DatetimeIndex(v["time"], tz='Asia/Shanghai')
            v.set_index("time", inplace=True)
            v.rename(columns={'code': 'targetcode', "close": "price"}, inplace=True)
            v.dropna(inplace=True)

            result_df.append(v)

        tag_columns = ['targetcode']
        name_list = ["optargetderivativeprice_1d", "optargetderivativeprice_2h", "optargetderivativeprice_1h",
                     "optargetderivativeprice_5m", "optargetderivativeprice_15m", "optargetderivativeprice_30m"]

        return list(zip(result_df, name_list)), tag_columns


if __name__ == "__main__":
    pandas.set_option('display.max_rows', None)
    # pandas.set_option('display.max_columns', None)
    op = OpTargetDerivativePrice()
    start = "2023-03-01 00:00:00"
    end = "2023-03-08 00:00:00"

    a, b = op.get(start=start, end=end)
    x, y, z, a1, b1, c1 = a
    print(x[0].columns)
    print(x[0])
    # a = op.get_data(start, end)
    # df = get_bars(security="510050.XSHG", unit='1m', count=10, fields=['close'])
    # print(df)
