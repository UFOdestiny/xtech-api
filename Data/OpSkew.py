# -*- coding: utf-8 -*-
# @Name     : OpSkew.py
# @Date     : 2023/3/17 13:31
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
import math

import pandas

from utils.InfluxTime import SplitTime
from service.JoinQuant import JQData
import datetime


class OpSkew(JQData):
    def __init__(self):
        # self.code = normalize_code(self.code_pre)
        super().__init__()
        self.df = None
        # self.targetcodes = ["510050.XSHG"]
        self.indicator = True

    def get_his_vol(self, code_s, start_date, end_date):
        b = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=90)
        start_date = str(b)

        """
        # 1、获取制定时间段内的日频价格
        """
        df = self.get_price(security=code_s,
                            fields=['close'],
                            frequency='daily',
                            start_date=start_date,
                            end_date=end_date, )

        if len(df) == 0:
            self.indicator = None
            return

        close = df["close"].to_list()

        """
        # 2、计算收益率，收益率=ln(后一天的价格/前一天的价格)
        """
        length = len(close)
        rts = [0]
        for i in range(1, length):
            rts.append(math.log(close[i] / close[i - 1]))

        df["pct"] = rts
        """
        # 3、计算某一天的前30天的平均收益率
        """
        # sum(rts[1:]) / (length - 1)

        avg = [0.0] * 31
        avg0 = sum(rts[1:31]) / 30
        avg.append(avg0)

        for i in range(32, length):
            avg_i = avg[-1] + (rts[i - 1] - rts[i - 31]) / 30
            avg.append(avg_i)

        df["pct_avg"] = avg

        """
        # 4、计算某一天的前30天的历史收益率
        """

        variance = [0.0] * 31
        for i in range(31, length):
            v = 0
            # 方差
            for j in range(i - 30, i):
                v += (rts[j] - avg[i]) ** 2
            # 年化，乘根号252
            # v = (v * 252 / 30) ** 0.5

            variance.append(v)

        df["std"] = variance

        df.drop(df[df["pct_avg"] == 0.0].index, inplace=True)
        df["S"] = 100 - 10 * ((df["pct"] - df["pct_avg"]) / df["std"])

        return df

    def get_data(self, start, end):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        df_pre = None
        #############
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
    op = OpSkew()
    start = "2023-03-15 00:00:00"
    end = "2023-03-16 00:00:00"
    a = op.get_his_vol(start_date=start, end_date=end, code_s="510050.XSHG")
    print(a)
