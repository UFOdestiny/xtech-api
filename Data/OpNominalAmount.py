# -*- coding: utf-8 -*-
# @Name     : OpNominalAmount.py
# @Date     : 2022/11/29 11:35
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pandas
from jqdatasdk import opt, query, get_price

from service.InfluxService import InfluxService
from utils.InfluxTime import SplitTime
from utils.JoinQuant import Authentication


class OpNominalAmount(metaclass=Authentication):
    def __init__(self):
        self.underlying_symbol = None
        self.db = InfluxService()
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

    def daily_info(self, code, start, end):
        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.contract_unit,
                  opt.OPT_DAILY_PREOPEN.expire_date,
                  opt.OPT_DAILY_PREOPEN.contract_type,
                  opt.OPT_DAILY_PREOPEN.exercise_price, ).filter(
            opt.OPT_DAILY_PREOPEN.underlying_symbol == code,
            opt.OPT_DAILY_PREOPEN.date >= start,
            opt.OPT_DAILY_PREOPEN.date <= end)

        self.daily = opt.run_query(q)

        print(self.daily)

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

        # print(self.code)
        # print(self.code_00)
        # print(self.code_01)

    def vol(self, code, start, end, types):
        df = get_price(code, start, end, frequency='60m', fields=['close', 'volume'])
        if len(df) == 0:
            return

        unit = self.daily[self.daily["code"] == code]["contract_unit"]

        df["unit"] = unit
        df.fillna(method='ffill', inplace=True)
        # print(df["close"], df["unit"], df["volume"])
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

        # print(code)
        # print(df)

        if self.dic[code] == "CO":
            self.result[index_c] = self.result[index_c].add(df["close"], fill_value=0)
        else:
            self.result[index_p] = self.result[index_p].add(df["close"], fill_value=0)

    def vol_aggregate(self, start, end):
        if self.code is None:
            self.result = None
            return

        for i in self.code:
            self.vol(i, start, end, 0)

        for i in self.code_00:
            self.vol(i, start, end, 1)

        for i in self.code_01:
            self.vol(i, start, end, 2)

        if self.result is None or len(self.result) == 0:
            return

        self.result["vol"] = self.result["vol_c"] + self.result["vol_p"]
        self.result["vol_00"] = self.result["vol_c_00"] + self.result["vol_p_00"]
        self.result["vol_01"] = self.result["vol_c_01"] + self.result["vol_p_01"]
        # self.result["money"]=self.result["vol"]*1

        # self.result["time"] = pandas.to_datetime(self.result.index).values.astype(object)
        # self.result = self.result[['time', "targetcode", 'vol_c', 'vol_p', 'vol', 'vol_c_00', 'vol_p_00',
        #                            "vol_00", 'vol_c_01', 'vol_p_01', "vol_01"]]

        if self.final_result is None:
            self.final_result = self.result
        else:
            self.final_result = pandas.concat([self.final_result, self.result])

    def get_all(self, **kwargs):
        start = kwargs["start"]
        end = kwargs["end"]
        codes = ['510050.XSHG', '510300.XSHG', '159919.XSHE', '510500.XSHG', '159915.XSHE', '159901.XSHE',
                 '159922.XSHE', '000852.XSHG', '000016.XSHE', '000300.XSHG', '000852.XSHE', "000016.XSHG"]

        times = SplitTime.split(start, end, interval_day=1)
        for t in times:
            for c in codes:
                self.pre_set(c, t[0], t[1])
                self.daily_info(c, t[0], t[1])
                self.vol_aggregate(t[0], t[1])

                length = len(self.result) if self.result is not None else 0
                print(c, t, length)

        if self.final_result is None:
            print("ZERO")
            return

        self.final_result.dropna(inplace=True)
        # print(self.final_result)

        # if not self.final_result.isnull().values.any():
        #     return self.final_result.values.tolist()
        # else:
        #     print("error")
        tag_columns = ['targetcode']
        self.final_result.index = pandas.DatetimeIndex(self.final_result.index, tz='Asia/Shanghai')

        # print(self.final_result)
        # print(self.final_result.columns)
        return self.final_result, tag_columns

    def get(self, **kwargs):
        start = kwargs["start"]
        end = kwargs["end"]
        codes = ['510050.XSHG', '510300.XSHG', '159919.XSHE', '510500.XSHG', '159915.XSHE', '159901.XSHE',
                 '159922.XSHE', '000852.XSHG', '000016.XSHG', '000300.XSHG', '000852.XSHE', "000016.XSHE"]

        for c in codes:
            self.pre_set(c, start, end)
            self.daily_info(c, start, end)
            self.vol_aggregate(start, end)
            length = len(self.result) if self.result is not None else 0
            print(c, start, end, length)

        if self.final_result is None:
            return None, None
        self.final_result.dropna(inplace=True)

        tag_columns = ['targetcode']
        self.final_result.index = pandas.DatetimeIndex(self.final_result.index, tz='Asia/Shanghai')

        print(123)
        print(self.result)
        return self.final_result, tag_columns


if __name__ == "__main__":
    opc = OpNominalAmount()
    # opc.daily_info("000300.XSHG", '2022-02-01 00:00:00', '2023-02-05 00:00:00')

    opc.db.query_data()
    # a, b = opc.get(start='2023-02-01 00:00:00', end='2023-02-17 00:00:00')
    # print(a["vol"].to_list())
    # print(a["vol"].values())
