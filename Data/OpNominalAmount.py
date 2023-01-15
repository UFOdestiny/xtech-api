# -*- coding: utf-8 -*-
# @Name     : OpNominalAmount.py
# @Date     : 2022/11/29 11:35
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pandas
from jqdatasdk import opt, query, get_price

from Data.JoinQuant import Authentication
from datetime import datetime, timedelta


class OpNominalAmount(metaclass=Authentication):
    def __init__(self):
        self.underlying_symbol = None

        self.daily = None
        self.dic = dict()
        self.code = None
        self.daily_00 = None
        self.dic_00 = dict()
        self.code_00 = None
        self.daily_01 = None
        self.dic_01 = dict()
        self.code_01 = None

        self.df = None
        self.month0 = None
        self.month0_expire = None
        self.month1 = None
        self.month1_expire = None
        self.result = None

        self.final_result = None

    def aggravate(self, start, end):
        start_date = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        res = []
        while start_date != end_date:
            res.append([])
            res[-1].append(start_date.strftime("%Y-%m-%d %H:%M:%S"))
            start_date += timedelta(days=1)
            res[-1].append(start_date.strftime("%Y-%m-%d %H:%M:%S"))
        return res

    def pre_set(self, code, start, end):
        self.result = get_price(code, fields=['close'], frequency='60m', start_date=start, end_date=end, )
        self.result["targetcode"] = code

        self.result["vol_c"] = 0
        self.result["vol_p"] = 0
        self.result["vol"] = 0

        self.result["vol_c_00"] = 0
        self.result["vol_p_00"] = 0
        self.result["vol_00"] = 0

        self.result["vol_c_01"] = 0
        self.result["vol_p_01"] = 0
        self.result["vol_01"] = 0

        del self.result["close"]
        # print(self.result)

    def daily_info(self, code, start, end):
        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.contract_unit,
                  opt.OPT_DAILY_PREOPEN.expire_date,
                  opt.OPT_DAILY_PREOPEN.contract_type, ).filter(
            opt.OPT_DAILY_PREOPEN.underlying_symbol == code,
            opt.OPT_DAILY_PREOPEN.date >= start,
            opt.OPT_DAILY_PREOPEN.date <= end)

        self.daily = opt.run_query(q)

        if len(self.daily) == 0:
            self.code = None
            return None

        self.daily["date"] = pandas.to_datetime(self.daily["date"])
        self.daily["date"] += pandas.Timedelta(hours=10, minutes=30)
        self.daily.set_index('date', inplace=True)

        date = sorted(self.daily["expire_date"].unique())
        # print(date)

        # time0 = self.daily.index[0]
        # date = [i for i in date if i.month != time0.month]

        # print(date)
        self.month0 = date[0]
        # self.month0_expire = self.month0 + timedelta(days=1)
        # self.month0_expire = (self.month0 + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        self.month1 = date[1]
        # self.month1_expire = (self.month1 + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        # self.month1_expire = self.month1 + timedelta(days=1)

        self.daily_00 = self.daily[self.daily["expire_date"] == self.month0]
        self.daily_01 = self.daily[self.daily["expire_date"] == self.month1]

        self.dic = {i[0]: i[1] for i in self.daily[["code", "contract_type"]].values}
        self.code = list(set((self.dic.keys())))

        self.dic_00 = {i[0]: i[1] for i in self.daily_00[["code", "contract_type"]].values}
        self.code_00 = list(set((self.dic_00.keys())))

        self.dic_01 = {i[0]: i[1] for i in self.daily_01[["code", "contract_type"]].values}
        self.code_01 = list(set((self.dic_01.keys())))

        print(self.code)
        # print(self.code_00)
        # print(self.code_01)

    def vol(self, code, start, end, types):
        df = get_price(code, start, end, frequency='60m', fields=['close', 'volume'])
        unit = self.daily[self.daily["code"] == code]["contract_unit"]

        df["unit"] = unit
        df.fillna(method='ffill', inplace=True)
        df["close"] = df["close"] * df["unit"] * df["volume"]
        del df["unit"]

        if types == 0:
            res_seg = ""
        elif types == 1:
            res_seg = "_00"
        else:
            res_seg = "_01"
            # print(df)

        index_c = f"vol_c{res_seg}"
        index_p = f"vol_p{res_seg}"

        print(code)
        print(df)

        if self.dic[code] == "CO":
            self.result[index_c] = self.result[index_c].add(df["close"], fill_value=0)
        else:
            self.result[index_p] = self.result[index_p].add(df["close"], fill_value=0)

    def vol_aggregate(self, start, end):
        if self.code is None:
            return

        for i in self.code:
            self.vol(i, start, end, 0)

        for i in self.code_00:
            self.vol(i, start, end, 1)

        for i in self.code_01:
            self.vol(i, start, end, 2)

        self.result["vol"] = self.result["vol_c"] + self.result["vol_p"]
        self.result["vol_00"] = self.result["vol_c_00"] + self.result["vol_p_00"]
        self.result["vol_01"] = self.result["vol_c_01"] + self.result["vol_p_01"]

        if self.final_result is None:
            self.final_result = self.result
        else:
            self.final_result = pandas.concat([self.final_result, self.result])

    def process_df(self):
        self.final_result.dropna(inplace=True)
        # self.final_result.to_excel("sep.xlsx")

        self.final_result["time"] = pandas.to_datetime(self.final_result.index).values.astype(object)
        self.final_result = self.final_result[['time', "targetcode", 'vol_c', 'vol_p', 'vol', 'vol_c_00', 'vol_p_00',
                                               "vol_00", 'vol_c_01', 'vol_p_01', "vol_01"]]

    def get(self, **kwargs):
        code = kwargs["code"]
        start = kwargs["start"]
        end = kwargs["end"]

        codes = ['510050.XSHG', '510300.XSHG', '159919.XSHE', '510500.XSHG', '159915.XSHE', '159901.XSHE',
                 '159922.XSHE', '000852.XSHE', '000016.XSHE', '000300.XSHG', ]
        for c in codes:
            times = self.aggravate(start, end)
            for t in times:
                print(c, t)
                self.pre_set(c, t[0], t[1])
                self.daily_info(c, t[0], t[1])
                self.vol_aggregate(t[0], t[1])

        self.process_df()
        if not self.result.isnull().values.any():
            return self.result.values.tolist()
        else:
            print("error")


if __name__ == "__main__":
    opc = OpNominalAmount()
    opc.get(code="510050.XSHG", start='2022-12-20 00:00:00', end='2023-01-10 00:00:00')
