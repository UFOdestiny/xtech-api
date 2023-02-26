# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/9/9 13:29
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :


from service.InfluxService import InfluxService
from utils.Logger import Logger
from Data.OpTargetQuote import OpTargetQuote
from Data.OpContractInfo import OpContractInfo
from Data.OpContractQuote import OpContractQuote
from Data.OpNominalAmount import OpNominalAmount
from Data.PutdMinusCalld import PutdMinusCalld
from Data.OpDiscount import OpDiscount
from Data.OpTargetDerivativeVol import OpTargetDerivativeVol

from utils.InfluxTime import SplitTime
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from threading import Lock


class Write:
    def __init__(self, source):
        self.db = InfluxService()
        self.source = source
        self.measurement = self.source.__name__.lower()
        self.log = Logger()
        self.count = 0
        self.lock = Lock()

    def get_data(self, **kwargs):
        s = self.source()
        if self.source == OpContractQuote and "code" not in kwargs:
            lst = s.collect_info(**kwargs)
            return lst
        df, tag_columns = s.get(**kwargs)
        return df, tag_columns

    def submit(self, **kwargs):
        df, tag_columns = self.get_data(**kwargs)
        if df is None:
            return False
        if type(df) != list:
            df = [i for i in df if i is not None]
        if len(df) == 0:
            return False

        for df_, m in df:
            self.db.write_pandas(df=df_, tag_columns=tag_columns, measurement=m, )
        return True

    def thread(self, **kw):
        indicator = self.submit(**kw)
        if indicator:
            self.lock.acquire()
            self.count += 1
            self.lock.release()
            self.log.info(f"{list(kw.values())} {self.count}")

        else:  # pass
            self.lock.acquire()
            self.count += 1
            self.lock.release()
            self.log.info(f"{list(kw.values())} {self.count} PASS")

    def __call__(self, **kwargs):
        if self.source == OpContractQuote:
            if "code" not in kwargs:
                lst = self.get_data(**kwargs)
                length = len(lst)

                # lst_kw = [{"code": c, "start": s, "end": e, "length": length} for c, s, e in lst]
                l_ = [{"code": c, "start": s, "end": e, "length": length} for c, s, e in lst]

            else:
                if type(kwargs["code"]) == str:
                    kwargs["code"] = [kwargs["code"]]

                lst = kwargs["code"]
                length = len(lst)
                l_ = [{"code": c, "start": kwargs["start"], "end": kwargs["end"], "length": length} for c in lst]

            print(length)
            with ThreadPoolExecutor(max_workers=min(10, length)) as e:
                all_task = [e.submit(self.thread, **kw) for kw in l_]
                wait(all_task, return_when=ALL_COMPLETED)

        elif self.source in [OpNominalAmount, PutdMinusCalld, OpDiscount]:
            times = SplitTime.split(kwargs["start"], kwargs["end"], interval_day=1, reverse=True)
            length = len(times)
            l_ = [{"start": t[0], "end": t[1], "length": length} for t in times]
            with ThreadPoolExecutor(max_workers=min(10, length)) as e:
                all_task = [e.submit(self.thread, **kw) for kw in l_]
                wait(all_task, return_when=ALL_COMPLETED)

        else:
            self.log.info(" ".join(kwargs.values()))
            self.submit(**kwargs)


if __name__ == '__main__':
    start = "2023-02-01 00:00:00"
    end = '2023-02-25 00:00:00'

    # Write(source=OpContractInfo)(start=start, end=end)
    # Write(source=OpTargetQuote)(start=start, end=end)
    # Write(source=OpNominalAmount)(start=start, end=end)
    # Write(source=OpContractQuote)(start=start, end=end, update=1)  # , updata=1
    # Write(source=PutdMinusCalld)(start=start, end=end)
    # Write(source=OpDiscount)(start=start, end=end)
    Write(source=OpTargetDerivativeVol)(start=start, end=end)
