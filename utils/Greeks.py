# -*- coding: utf-8 -*-
# @Name     : Greeks.py
# @Date     : 2022/11/5 12:27
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import numpy as np
import scipy.stats as si


def d(s, k, r, T, sigma):
    d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return d1, d2


def delta(s, k, r, T, sigma, n):
    d1 = d(s, k, r, T, sigma)[0]
    delta = n * si.norm.cdf(n * d1)
    return delta


def gamma(s, k, r, T, sigma):
    d1 = d(s, k, r, T, sigma)[0]
    gamma = si.norm.pdf(d1) / (s * sigma * np.sqrt(T))
    return gamma


def vega(s, k, r, T, sigma):
    d1 = d(s, k, r, T, sigma)[0]
    vega = (s * si.norm.pdf(d1) * np.sqrt(T)) / 100
    return vega


def theta(s, k, r, T, sigma, n):
    """
    认购期权的n为1
    认沽期权的n为-1
    """
    d1 = d(s, k, r, T, sigma)[0]
    d2 = d(s, k, r, T, sigma)[1]

    theta = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(
        n * d2)) / 365
    return theta
