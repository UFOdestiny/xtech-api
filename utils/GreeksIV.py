# -*- coding: utf-8 -*-
# @Name     : GreeksIV.py
# @Date     : 2022/11/5 12:27
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import numpy as np
import scipy.stats as si


class Greeks:
    def __init__(self):
        self.r = 0.015

    # s价格 k行权价 r无风险利率 T年化期限 sigma历史波动率
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


class ImpliedVolatility:
    def __init__(self):
        self.n = si.norm.pdf
        self.N = si.norm.cdf

        self.MAX_ITERATIONS = 100
        self.PRECISION = 1.0e-5

        self.r = 0.015

    def bs_price(self, cp_flag, S, K, T, v, q=0.0):
        d1 = (np.log(S / K) + (self.r + v * v / 2.) * T) / (v * np.sqrt(T))
        d2 = d1 - v * np.sqrt(T)
        if cp_flag == 'c':
            price = S * np.exp(-q * T) * self.N(d1) - K * np.exp(-self.r * T) * self.N(d2)
        else:
            price = K * np.exp(-self.r * T) * self.N(-d2) - S * np.exp(-q * T) * self.N(-d1)
        return price

    def bs_vega(self, S, K, T, v):
        d1 = (np.log(S / K) + (self.r + v * v / 2.) * T) / (v * np.sqrt(T))
        return S * np.sqrt(T) * self.n(d1)

    def find_vol(self, target_value, call_put, S, K, T):
        sigma = 0.5
        for i in range(0, self.MAX_ITERATIONS):
            price = self.bs_price(call_put, S, K, T, sigma)
            vega = self.bs_vega(S, K, T, sigma)
            diff = target_value - price  # 我们的根

            if abs(diff) < self.PRECISION:
                return sigma

            sigma = sigma + diff / vega  # f(x) / f'(x)
            print(sigma)


        return sigma


if __name__ == "__main__":
    import pandas as pd
    import datetime

    # df = pd.read_excel("D:\\COURSE\\X-tech\\api\\Data\\10004496XSHG.xlsx")
    g = Greeks()
    # a = df.iloc[0]
    # print(a["close"], a["exercise_price"], a["days"], a["his_vol"])
    print(g.delta(4.12, 4.6, 0.167123, 0.1637))

    # V_market = 0.8079
    # K = 4.6
    # T = 0.039
    #
    # S = 3.786


    # cp = 'c'  # 看涨期权
    # for i in range(1):
    #     implied_vol = ImpliedVolatility().find_vol(V_market, cp, S, K, T)
    #     print('Implied vol: %.2f%%' % (implied_vol * 100))
