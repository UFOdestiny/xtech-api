# -*- coding: utf-8 -*-
# @Name     : PutdMinusCalld.py
# @Date     : 2022/12/30 11:13
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :


# -*- coding: utf-8 -*-
"""
Created on Mon Dec 26 21:27:35 2022

@author: xinyue
"""
from datetime import datetime, timedelta
from utils.InfluxTime import InfluxTime
import pandas
from jqdatasdk import opt, query, get_price

from Data.JoinQuant import Authentication
from service.InfluxService import InfluxdbService
import scipy.interpolate as spi


#
# ts = sorted(list(set(df_new['Time'].to_list())))
#
# l = []
# for t in ts:
#     # 取次月合约
#     df_t = df_new[df_new['Time'] == t]
#     Tset = sorted(list(set(df_t['T'].to_list())))
#     T_active = Tset[1]
#     df_active = df_t[df_t['T'] == T_active]
#     # 取有效次月认沽、认购合约
#     put = sqldf(
#         '''select * from df_active where option_name not like '%A%' and option_name like '%沽%' and delta not in (0,1) and IV<>0''').sort_values(
#         by='delta').drop_duplicates(subset=['delta'], keep='first', inplace=False)
#     call = sqldf(
#         '''select * from df_active where option_name not like '%A%' and option_name like '%购%' and delta not in (0,1) and IV<>0''').sort_values(
#         by='delta').drop_duplicates(subset=['delta'], keep='first', inplace=False)
#
#     if len(put) == 0 or len(call) == 0:
#         put = sqldf(
#             '''select * from df_active where option_name like '%沽%' and delta not in (0,1) and IV<>0''').sort_values(
#             by='delta').drop_duplicates(subset=['delta'], keep='first', inplace=False)
#         call = sqldf(
#             '''select * from df_active where option_name like '%购%' and delta not in (0,1) and IV<>0''').sort_values(
#             by='delta').drop_duplicates(subset=['delta'], keep='first', inplace=False)
#
#     # delta-iv插值，得到正负0.25、正负0.5 delta对应IV
#     tck1 = spi.splrep(call['delta'].to_list(), call['iv'].to_list(), k=1)
#     ivc0 = spi.splev([0.25, 0.5], tck1, ext=0)
#     tck2 = spi.splrep(put['delta'].to_list(), put['iv'].to_list(), k=1)
#     ivp0 = spi.splev([-0.25, -0.5], tck2, ext=0)
#     # 计算putd-calld
#     v = ivp0[0] - ivp0[1] - (ivc0[0] - ivc0[1])
#
#     l.append([ivp0[0] - ivp0[1], ivc0[0] - ivc0[1], v])
#
# df_PutdMinusCalld = pd.DataFrame(l, index=ts)


class OpNominalAmount(metaclass=Authentication):
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
        self.result = get_price(code, fields=['close'], frequency='1d', start_date=start, end_date=end, )
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
                  opt.OPT_DAILY_PREOPEN.contract_type, ).filter(
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

        date = sorted(self.daily["expire_date"].unique())
        # print(date)
        self.month1 = date[1]
        self.CO = self.daily[(self.daily["expire_date"] == self.month1) & (self.daily["contract_type"] == "CO")]
        self.PO = self.daily[(self.daily["expire_date"] == self.month1) & (self.daily["contract_type"] == "PO")]

        self.CO_code = self.CO["code"].values
        self.PO_code = self.PO["code"].values

        CO = [f"r[\"opcode\"] == \"{i}\"" for i in self.CO_code]
        self.CO_code_all = " or ".join(CO)

        PO = [f"r[\"opcode\"] == \"{i}\"" for i in self.PO_code]
        self.PO_code_all = " or ".join(PO)
        # print(self.CO)
        # print(self.PO)
        # print(self.CO_code)
        # print(self.PO_code)

    def vol(self, start, end, targetcode="510050.XSHG", mode="CO"):
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
                    from(bucket: "xtech")
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

        # print(df)
        return df["delta"].tolist(), df["iv"].tolist()

    def vol_aggregate(self, start, end):
        if self.CO is None:
            return None
        start_, end_ = InfluxTime.to_influx_time(start), InfluxTime.to_influx_time(end)
        CO_delta, CO_iv = self.vol(start_, end_, mode="CO")
        if CO_delta is None:
            return None
        PO_delta, PO_iv = self.vol(start_, end_, mode="PO")
        if PO_delta is None:
            return None

        tck1 = spi.splrep(CO_delta, CO_iv, k=1)

        ivc0 = spi.splev([0.25, 0.5], tck1, ext=0)

        tck2 = spi.splrep(PO_delta, PO_iv, k=1)

        ivp0 = spi.splev([-0.25, -0.5], tck2, ext=0)

        putd = ivp0[0] - ivp0[1]
        calld = ivc0[0] - ivc0[1]
        putd_calld = putd - calld

        self.result.loc[start, "putd"] = putd
        self.result.loc[start, "calld"] = calld
        self.result.loc[start, "putd_calld"] = putd_calld
        # print(self.result)

    def process_df(self):
        self.result.dropna(inplace=True)
        print(self.result)
        self.result.to_excel("sep2.xlsx")

        # self.result["time"] = pandas.to_datetime(self.result.index).values.astype(object)
        # self.result = self.result[['time', "targetcode", 'vol_c', 'vol_p', 'vol', 'vol_c_00', 'vol_p_00',
        #                            "vol_00", 'vol_c_01', 'vol_p_01', "vol_01"]]

    def get(self, **kwargs):
        code = kwargs["code"]
        start = kwargs["start"]
        end = kwargs["end"]

        times = self.aggravate(start, end)

        self.pre_set(code, start, end)
        for t in times:
            print(t)
            self.daily_info(code, t[0], t[1])
            self.vol_aggregate(t[0], t[1])

        self.process_df()
        # if not self.result.isnull().values.any():
        #     return self.result.values.tolist()
        # else:
        #     print("error")


if __name__ == "__main__":
    opc = OpNominalAmount()
    opc.get(code="510050.XSHG", start='2022-09-01 00:00:00', end='2022-09-27 00:00:00')
