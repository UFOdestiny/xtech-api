# -*- coding: utf-8 -*-
# @Name     : PutdMinusCalld.py
# @Date     : 2022/12/30 11:13
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import numpy as np
import pandas
import scipy.interpolate as spi
from jqdatasdk import opt, query, get_price

from service.InfluxService import InfluxdbService
from utils.InfluxTime import InfluxTime
from utils.InfluxTime import SplitTime
from utils.JoinQuant import Authentication


class PutdMinusCalld(metaclass=Authentication):
    def __init__(self):
        self.db = InfluxdbService()

        self.underlying_symbol = None

        self.CO = None
        self.PO = None
        self.CO_code = None
        self.CO_code_all = None
        self.PO_code = None
        self.PO_code_all = None

        self.daily = None

        self.df = None
        self.month0 = None
        self.month1 = None
        self.result = None

        self.final_result = None

    def pre_set(self, code, start, end):
        self.result = get_price(code, fields=['close'], frequency='1m', start_date=start, end_date=end, )

        self.result.index -= pandas.Timedelta(minutes=1)
        self.result["targetcode"] = code
        self.result["putd"] = 0
        self.result["calld"] = 0
        self.result["putd_calld"] = 0

        del self.result["close"]

    def daily_info(self, code, start, end):
        q = query(opt.OPT_DAILY_PREOPEN.date,
                  opt.OPT_DAILY_PREOPEN.code,
                  opt.OPT_DAILY_PREOPEN.underlying_symbol,
                  opt.OPT_DAILY_PREOPEN.contract_unit,
                  opt.OPT_DAILY_PREOPEN.expire_date,
                  opt.OPT_DAILY_PREOPEN.contract_type,

                  opt.OPT_DAILY_PREOPEN.name,

                  ).filter(
            opt.OPT_DAILY_PREOPEN.underlying_symbol == code,
            opt.OPT_DAILY_PREOPEN.date >= start,
            opt.OPT_DAILY_PREOPEN.date <= end)

        self.daily = opt.run_query(q)

        if len(self.daily) == 0:
            self.CO = None
            return None

        self.daily["date"] = pandas.to_datetime(self.daily["date"])
        self.daily["date"] += pandas.Timedelta(hours=10, minutes=30)

        self.daily.set_index('date', inplace=True)

        # print(self.daily["name"].values.tolist())

        date = sorted(self.daily["expire_date"].unique())
        # print(date)
        self.month1 = date[1]
        self.CO = self.daily[(self.daily["expire_date"] == self.month1) & (self.daily["contract_type"] == "CO")]
        self.PO = self.daily[(self.daily["expire_date"] == self.month1) & (self.daily["contract_type"] == "PO")]

        self.CO_code = self.CO["code"].values
        self.PO_code = self.PO["code"].values

        # print(len(self.CO_code), len(self.PO_code))

        CO = [f"r[\"opcode\"] == \"{i}\"" for i in self.CO_code]
        self.CO_code_all = " or ".join(CO)

        PO = [f"r[\"opcode\"] == \"{i}\"" for i in self.PO_code]
        self.PO_code_all = " or ".join(PO)
        # print(self.CO)
        # print(self.PO)
        # print(self.CO_code)
        # print(self.PO_code)

    def vol(self, start, end, targetcode, mode="CO"):
        # delta = f"""
        #         from(bucket: "xtech")
        #         |> range(start: {start}, stop: {end})
        #         |> filter(fn: (r) => r["targetcode"] == "{targetcode}")
        #         |> filter(fn: (r) => {self.CO_code_all})
        #         |> filter(fn: (r) => r["_field"] == "delta" or r["_field"] == "iv")
        #         """
        # res = self.db.query_data_raw(delta)
        # res1 = [i.get_value() for i in res]
        # length = len(res1)
        # delta_set = res1[:length // 2]
        # iv_set = res1[length // 2:]
        # zips = list(zip(delta_set, iv_set))
        # zips.sort(key=lambda x: x[0])
        # final_res = [i for i in zips if i[0] != 0 and i[0] != 1 and i[1] != 0]
        # print(len(final_res))

        if mode == "CO":
            data = self.CO_code_all
        else:
            data = self.PO_code_all

        delta2 = f"""
                    from(bucket: "{self.db.INFLUX.bucket}")
                      |> range(start: {start}, stop: {end})
                      |> filter(fn: (r) => r["targetcode"] == "{targetcode}")
                      |> filter(fn: (r) => {data})
                      |> filter(fn: (r) => r["_field"] == "delta" or r["_field"] == "iv")
                      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                      |> keep(columns: ["delta", "iv"])
                """

        df = self.db.query_api.query_data_frame(delta2)

        if len(df) == 0:
            return None, None

        df.drop(["result", "table", ], axis=1, inplace=True)
        df.drop(df[(df.iv == 0) & (df.delta == 1) & (df.delta == 0)].index, inplace=True)
        df.drop_duplicates(subset=['delta'], keep="first", inplace=True)
        df.sort_values(by=['delta'], inplace=True)

        return df["delta"].tolist(), df["iv"].tolist()

    def vol_aggregate(self, start, end, code):
        if self.CO is None:
            return None
        # print(self.result)
        indexs = self.result.index[(self.result.index > start) & (self.result.index < end)]
        pairs = [(str(indexs[i]), str(indexs[i + 1])) for i in range(len(indexs) - 1)]

        for s, e in pairs:
            # print(s)
            start_, end_ = InfluxTime.utc(s), InfluxTime.utc(e)
            CO_delta, CO_iv = self.vol(start_, end_, targetcode=code, mode="CO")
            PO_delta, PO_iv = self.vol(start_, end_, targetcode=code, mode="PO")

            tck1 = spi.splrep(CO_delta, CO_iv, k=1)
            ivc0 = spi.splev([0.25, 0.5], tck1, ext=0)

            # print(len(CO_delta), len(PO_delta))
            if len(CO_delta) <= 1 or len(PO_delta) <= 1:
                putd, calld, putd_calld = np.nan, np.nan, np.nan
            else:
                tck2 = spi.splrep(PO_delta, PO_iv, k=1)
                ivp0 = spi.splev([-0.25, -0.5], tck2, ext=0)
                putd = ivp0[0] - ivp0[1]
                calld = ivc0[0] - ivc0[1]
                putd_calld = putd - calld

            self.result.loc[s, "putd"] = putd
            self.result.loc[s, "calld"] = calld
            self.result.loc[s, "putd_calld"] = putd_calld

            # print(putd, calld, putd_calld)

    def process_df(self):
        self.result.dropna(inplace=True)
        # print(self.result)
        self.result.to_excel("sep2.xlsx")

        # self.result["time"] = pandas.to_datetime(self.result.index).values.astype(object)
        # self.result = self.result[['time', "targetcode", 'vol_c', 'vol_p', 'vol', 'vol_c_00', 'vol_p_00',
        #                            "vol_00", 'vol_c_01', 'vol_p_01', "vol_01"]]

    def get(self, **kwargs):
        codes = [kwargs["code"]] if "code" in kwargs else ['510050.XSHG', '510300.XSHG', '159919.XSHE', '510500.XSHG',
                                                           '159915.XSHE', '159901.XSHE', '159922.XSHE', '000852.XSHE',
                                                           '000016.XSHE', '000300.XSHG', ]

        start = kwargs["start"]
        end = kwargs["end"]

        times = SplitTime.split(start, end, interval_day=1)

        for code in codes:
            self.pre_set(code, start, end)
            for t in times:
                print(code, t)
                self.daily_info(code, t[0], t[1])
                self.vol_aggregate(t[0], t[1], code)

            self.result["time"] = pandas.to_datetime(self.result.index).values.astype(object)
            self.result.reset_index(drop=True, inplace=True)
            if self.final_result is None:
                self.final_result = self.result
            else:
                self.final_result = pandas.concat([self.final_result, self.result])

        self.final_result = self.final_result[["time", "targetcode", "putd", "calld", "putd_calld"]]
        if not self.final_result.isnull().values.any():
            return self.final_result.values.tolist()
        else:
            print("error")


if __name__ == "__main__":
    opc = PutdMinusCalld()
    opc.get(start='2022-01-05 00:00:00', end='2022-01-06 00:00:00')
