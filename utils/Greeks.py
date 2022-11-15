# -*- coding: utf-8 -*-
# @Name     : Greeks.py
# @Date     : 2022/11/5 12:27
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import numpy as np
import scipy.stats as si


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


if __name__ == "__main__":
    import pandas as pd

    # df = pd.read_excel("D:\\COURSE\\X-tech\\api\\Data\\10004496XSHG.xlsx")
    g = Greeks()
    # a = df.iloc[0]
    # print(a["close"], a["exercise_price"], a["days"], a["his_vol"])
    print(g.delta(4.6, 4.6, 0.167123, 0.1637))
