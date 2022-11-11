# -*- coding: utf-8 -*-
# @Name     : Greeks.py
# @Date     : 2022/11/5 12:27
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
import datetime

import numpy as np
import pandas as pd
import scipy.stats as si
from jqdatasdk import opt, query, auth

df = pd.read_excel("D:\\COURSE\\X-tech\\api\\Data\\10004496XSHG.xlsx")

start_time = df["time"][0].replace(hour=15, minute=0, second=0, microsecond=0)

time_series = pd.date_range(start=start_time, end=start_time)
days_offset = {(i.replace(hour=15, minute=0, second=0, microsecond=0) - start_time).days for i in df["time"]}

for i in days_offset:
    if i != 0:
        d = start_time + datetime.timedelta(days=i)
        temp = pd.date_range(start=d, end=d)
        time_series = time_series.append(temp)

df2 = df[df["time"].isin(time_series)]
df2 = df2.reset_index()
df3 = df2.copy()
df3["rt"] = np.log(df2["close"].div(df2["close"].shift()))
rt_series = df3["rt"][1:]

avg = sum(rt_series[1:20]) / 20

df3["avg"] = 0
df3["sigma"] = 0
for t in range(21, len(df2)):
    df3.loc[t, "avg"] = avg

    deviation = 0
    for j in range(t - 19, t + 1):
        deviation += (avg - rt_series[j]) ** 2

    df3.loc[t, "sigma"] = (deviation / 19) ** 0.5 * (250 ** 0.5)

    avg = avg - rt_series[t - 20] / 20 + rt_series[t] / 20

# last_one = float(df[df["time"] == time_series[0]]["close"])
# for t in range(1, len(time_series)):
#     this_one = float(df[df["time"] == time_series[t]]["close"])
#     df.loc[df["time"] == time_series[t], "rt"] = math.log(this_one / last_one)
#     last_one = this_one


# df["rt"]=np.log(df["close"].div(df["close"].shift()))
# print(df)


auth("15210597532", "jin00904173")

q = query(opt.OPT_DAILY_PREOPEN.date,
          opt.OPT_DAILY_PREOPEN.code,
          opt.OPT_DAILY_PREOPEN.exercise_price,
          opt.OPT_DAILY_PREOPEN.expire_date, ).filter(
    opt.OPT_DAILY_PREOPEN.code == "10004496.XSHG",
    opt.OPT_DAILY_PREOPEN.date >= '2022-08-01 00:00:00',
    opt.OPT_DAILY_PREOPEN.date <= '2022-11-08 23:00:00')

df666 = opt.run_query(q)
df666['days'] = df666["expire_date"] - df666["date"]
df666['days'] = df666['days'].apply(lambda x: x.days / 365)


class Greeks:
    def __init__(self):
        self.r = 0.015

    # s 股票价格 k行权价 r无风险利率 T年化期限 sigma历史波动率
    def d(self, s, k, T, sigma):
        d1 = (np.log(s / k) + (self.r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        return d1, d2

    def delta(self, s, k, T, sigma, n=1):
        d1 = self.d(s, k, T, sigma)[0]
        delta = n * si.norm.cdf(n * d1)
        return delta

    def gamma(self, s, k, T, sigma):
        d1 = self.d(s, k, T, sigma)[0]
        gamma = si.norm.pdf(d1) / (s * sigma * np.sqrt(T))
        return gamma

    def vega(self, s, k, T, sigma):
        d1 = self.d(s, k, T, sigma)[0]
        vega = (s * si.norm.pdf(d1) * np.sqrt(T)) / 100
        return vega

    def theta(self, s, k, T, sigma, n=1):
        d1 = self.d(s, k, T, sigma)[0]
        d2 = self.d(s, k, T, sigma)[1]

        theta = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * self.r * k * np.exp(
            -self.r * T) * si.norm.cdf(
            n * d2)) / 365
        return theta


df["exercise_price"] = 0
df["sigma"] = 0
df["span"] = 0
df["delta"] = 0
df["gamma"] = 0
df["vega"] = 0
df["theta"] = 0

g = Greeks()

df666["date"] = pd.to_datetime(df666["date"])

for i in range(len(df666)):
    dt = df666["date"][i]
    df.loc[(df["time"] < dt + datetime.timedelta(days=1)) & (df["time"] > dt), "span"] = df666["days"][i]
    df.loc[(df["time"] < dt + datetime.timedelta(days=1)) & (df["time"] > dt), "exercise_price"] = \
        df666["exercise_price"][i]

for i in range(len(df3)):
    dt = df3["time"][i].replace(hour=0)
    df.loc[(df["time"] < dt + datetime.timedelta(days=1)) & (df["time"] > dt), "sigma"] = df3["sigma"][i]

# for time1 in df666["date"]:
#     for time2 in df["time"]:
#         if time1.year == time2.year and time1.month == time2.month and time1.day == time2.day:
#             df[df["time"] == time2]["span"] = df666[df666["date"] == time1]["days"]


df["delta"] = g.delta(df["close"], df["exercise_price"], df["span"], df["sigma"])
df["gamma"] = g.gamma(df["close"], df["exercise_price"], df["span"], df["sigma"])
df["vega"] = g.vega(df["close"], df["exercise_price"], df["span"], df["sigma"])
df["theta"] = g.theta(df["close"], df["exercise_price"], df["span"], df["sigma"])

#pd.set_option('display.max_rows', None)
df.to_excel('test.xlsx',index=False)

# df["gamma"] = 0
# df["theta"] = 0
#
# writer = pd.ExcelWriter("greeks.xlsx")
# df3.to_excel(writer, sheet_name='minute', index=False)
