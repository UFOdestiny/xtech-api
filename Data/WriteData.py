# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/9/9 13:29
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from functools import partial
import argparse

import os
import sys

root_path = os.path.abspath(__file__)
root_path = '/'.join(root_path.split('/')[:-2])
sys.path.append(root_path)

from service.InfluxService import InfluxService
from utils.Logger import Logger
from Data.OpTargetQuote import OpTargetQuote
from Data.OpContractInfo import OpContractInfo
from Data.OpContractQuote import OpContractQuote
from Data.OpNominalAmount import OpNominalAmount
from Data.PutdMinusCalld import PutdMinusCalld
from Data.OpDiscount import OpDiscount
from Data.CPR import CPR
from Data.OpTargetDerivativeVol import OpTargetDerivativeVol
from Data.OpTargetDerivativePrice import OpTargetDerivativePrice
from utils.InfluxTime import SplitTime, InfluxTime


class Write:
    def __init__(self, source):
        self.db = InfluxService()
        self.source = source
        self.measurement = self.source.__name__.lower()
        self.log = Logger()
        self.count = 0
        self.lock = Lock()

    def callback(self, future, kw):
        msg = " ".join(list(map(str, kw.values())))
        msg = f"{self.measurement} {msg}"

        t = future.exception()
        self.lock.acquire()
        self.count += 1
        msg = f"{msg} {self.count} "

        if t is not None:
            # self.log.exception(t)
            self.log.exception(msg)
        else:
            future_result = future.result()
            result_msg, s_time = future_result
            end_time = f" {round((time.time() - s_time) / 60, 3)}min"
            self.log.info(msg + result_msg + end_time)

        self.lock.release()

    def get_data(self, **kwargs):
        s = self.source()
        if self.source == OpContractQuote and "code" not in kwargs:
            lst = s.collect_info(**kwargs)
            return lst
        df, tag_columns = s.get(**kwargs)
        return df, tag_columns

    def submit(self, **kwargs):
        df, tag_columns = self.get_data(**kwargs)
        msg = ""
        if df is None:
            return False

        if type(df) != list:
            df = [i for i in [df] if i is not None]

        if len(df) == 0:
            return False

        if type(df[0]) == tuple or type(df[0]) == list:
            for df_, m in df:
                msg += self.db.write_pandas(df=df_, tag_columns=tag_columns, measurement=m, method="syn")
                msg += " "
        else:
            if self.measurement in ["opcontractquote", "opnominalamount", "putdminuscalld", "opdiscount", "cpr"]:
                method = "syn"
            else:
                method = "batch"
            msg = self.db.write_pandas(df=df[0], tag_columns=tag_columns, measurement=self.measurement, method=method)
        return msg

    def thread(self, **kw):
        start_time = time.time()
        indicator = self.submit(**kw)

        if not indicator:
            return "PASS", start_time
        else:
            return indicator, start_time

    def multitask(self, kw_lst):
        if type(kw_lst) != list:
            kw_lst = [kw_lst]

        length = len(kw_lst)

        with ThreadPoolExecutor(max_workers=min(10, length)) as e:
            for kw in kw_lst:
                future = e.submit(self.thread, **kw)
                future.add_done_callback(partial(self.callback, kw=kw))

    def __call__(self, **kwargs):
        kw = kwargs
        if self.source == OpContractQuote:
            if "code" not in kwargs:
                lst = self.get_data(**kwargs)
                length = len(lst)
                kw = [{"code": c, "start": s, "end": e, "length": length} for c, s, e in lst]
            else:
                if type(kwargs["code"]) == str:
                    kwargs["code"] = [kwargs["code"]]
                lst = kwargs["code"]
                length = len(lst)
                kw = [{"code": c, "start": kwargs["start"], "end": kwargs["end"], "length": length} for c in lst]

        elif self.source in [OpNominalAmount, PutdMinusCalld, OpDiscount, CPR]:
            times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=1, reverse=True)
            length = len(times)
            kw = [{"start": t[0], "end": t[1], "length": length} for t in times]

        self.multitask(kw)


start_ = time.time()
parser = argparse.ArgumentParser()
parser.add_argument('--source', type=str, default=None, help='measurement')
parser.add_argument('--update', type=str, default='True', help='update?')
parser.add_argument('--time', type=str, default='minute', help='type of time')
args = parser.parse_args()

source = args.source
if source:
    time_ = args.time
    if time_ == "today":
        start, end = InfluxTime.today()
    elif time_ == "minute":
        start, end = InfluxTime.last_minute()
    else:
        start, end = InfluxTime.last_minute(5)

    Write(source=eval(source))(start=start, end=end, update=args.update)
    if source == "OpContractQuote":
        Write(source=PutdMinusCalld)(start=start, end=end, update=args.update)

if __name__ == '__main__':
    if not source:
        start = "2023-03-14 00:00:00"
        end = '2023-03-14 12:00:00'

        # Write(source=OpContractInfo)(start=start, end=end)
        # Write(source=OpTargetQuote)(start=start, end=end, update='1')
        # Write(source=OpNominalAmount)(start=start, end=end)
        # Write(source=OpContractQuote)(start=start, end=end, update=1)
        Write(source=PutdMinusCalld)(start=start, end=end)
        # Write(source=OpDiscount)(start=start, end=end)
        # Write(source=CPR)(start=start, end=end)
        # Write(source=OpTargetDerivativeVol)(start=start, end=end)
        # Write(source=OpTargetDerivativePrice)(start=start, end=end)
