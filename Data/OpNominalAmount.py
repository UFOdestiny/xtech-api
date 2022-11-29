# -*- coding: utf-8 -*-
# @Name     : OpNominalAmount.py
# @Date     : 2022/11/29 11:35
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pandas
from jqdatasdk import opt, query, get_price

from Data.JoinQuant import Authentication


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
        self.daily["date"] = pandas.to_datetime(self.daily["date"])
        self.daily["date"] += pandas.Timedelta(hours=10, minutes=30)
        self.daily.set_index('date', inplace=True)

        date = sorted(self.daily["expire_date"].unique())
        time0 = self.daily.index[0]
        date = [i for i in date if i.month != time0.month]

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

    def vol(self, code, start, end, types):
        df = get_price(code, start, end, frequency='60m', fields=['close'])

        unit = self.daily[self.daily["code"] == code]["contract_unit"]

        df["unit"] = unit
        df.fillna(method='ffill', inplace=True)
        df["close"] = df["close"] * df["unit"]
        del df["unit"]

        if types == 0:
            res_seg = ""
        elif types == 1:
            res_seg = "_00"
        else:
            res_seg = "_01"

        index_c = f"vol_c{res_seg}"
        index_p = f"vol_p{res_seg}"

        if self.dic[code] == "CO":
            self.result[index_c] = self.result[index_c].add(df["close"], fill_value=0)
        else:
            self.result[index_p] = self.result[index_p].add(df["close"], fill_value=0)

    def vol_aggregate(self, start, end):

        for i in self.code:
            self.vol(i, start, end, 0)

        for i in self.code_00:
            self.vol(i, start, end, 1)

        for i in self.code_01:
            self.vol(i, start, end, 2)

        self.result["vol"] = self.result["vol_c"] + self.result["vol_p"]
        self.result["vol_00"] = self.result["vol_c_00"] + self.result["vol_p_00"]
        self.result["vol_01"] = self.result["vol_c_01"] + self.result["vol_p_01"]

        # self.result.to_excel("Nov.xlsx")

    def process_df(self):
        self.result["time"] = pandas.to_datetime(self.result.index).values.astype(object)
        self.result = self.result[['time', "targetcode", 'vol_c', 'vol_p', 'vol', 'vol_c_00', 'vol_p_00',
                                   "vol_00", 'vol_c_01', 'vol_p_01', "vol_01"]]

    def get(self, **kwargs):

        code = kwargs["code"]
        start = kwargs["start"]
        end = kwargs["end"]

        self.pre_set(code, start, end)
        self.daily_info(code, start, end)
        self.vol_aggregate(start, end)
        self.process_df()
        if not self.result.isnull().values.any():
            return self.result.values.tolist()
        else:
            print("error")


if __name__ == "__main__":
    opc = OpNominalAmount()
    opc.get(code="510050.XSHG", start='2022-09-01 00:00:00', end='2022-10-01 00:00:00')
