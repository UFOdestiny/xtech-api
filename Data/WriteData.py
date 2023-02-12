# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/9/9 13:29
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from service.InfluxService import InfluxdbService
from utils.Logger import Logger
from utils.JoinQuant import Authentication
from Data.OpTargetQuote import OpTargetQuote
from Data.OpContractInfo import OpContractInfo
from Data.OpContractQuote import OpContractQuote
from Data.OpNominalAmount import OpNominalAmount
from Data.PutdMinusCalld import PutdMinusCalld

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from threading import Lock


class WriteData:
    format_dict = {
        "optargetquote": "optargetquote,targetcode={1} price={2},pct={3} {0}",
        "opcontractinfo": "opcontractinfo,opcode={1},targetcode={2},type={4} multiplier={5},strikeprice={3},days={6} {0}",
        "opcontractquote": "opcontractquote,opcode={1},targetcode={2} open={3},close={4},high={5},low={6},amount={7},"
                           "vol={8},pct={9},a1_p={10},a1_v={11},b1_p={12},b1_v={13},delta={14},"
                           "gamma={15},vega={16},theta={17},iv={18},timevalue={19} {0}",
        "opnominalamount": "opnominalamount,targetcode={1} vol_c={2},vol_p={3},vol={4},"
                           "vol_c_00={5},vol_p_00={6},vol_00={7},vol_c_01={8},vol_p_01={9},vol_01={10} {0}",
        "putdminuscalld": "putdminuscalld,targetcode={1} putd={2},calld={3},putd_calld={4} {0}",
    }

    def __init__(self, source):
        self.db = InfluxdbService()
        self.source = source
        self.log = Logger()
        self.count = 0
        self.lock = Lock()

    def get_data(self, **kwargs):
        s = self.source()
        if self.source == OpContractQuote and "code" not in kwargs:
            lst = s.collect_info(**kwargs)
            return lst
        data = s.get(**kwargs)
        return data

    def generate(self, **kwargs):
        data = self.get_data(**kwargs)
        fmt = self.format_dict[self.source.__name__.lower()]
        sequence = [fmt.format(*i) for i in data]
        return sequence

    def send(self, **kwargs):
        q = self.generate(**kwargs)
        self.db.write_data_execute(q)


class Write(WriteData):
    def thread(self, **kw):
        self.send(**kw)
        self.lock.acquire()
        self.count += 1
        self.log.info(f"{kw['code']} {kw['start']} {kw['end']} {self.count}/{kw['length']}")
        self.lock.release()

    def __call__(self, **kwargs):
        if self.source == OpContractQuote:
            if "code" not in kwargs:
                lst = self.get_data(**kwargs)
                length = len(lst)

                # lst_kw = [{"code": c, "start": s, "end": e, "length": length} for c, s, e in lst]
                l_ = [{"code": c, "start": kwargs["start"], "end": kwargs["end"], "length": length} for c, s, e in lst]

            else:
                if type(kwargs["code"]) == str:
                    kwargs["code"] = [kwargs["code"]]

                lst = kwargs["code"]
                length = len(lst)
                l_ = [{"code": c, "start": kwargs["start"], "end": kwargs["end"], "length": length} for c in lst]

            with ThreadPoolExecutor(max_workers=max(10, length)) as e:
                all_task = [e.submit(self.thread, **kw) for kw in l_]
                wait(all_task, return_when=ALL_COMPLETED)

        else:
            self.log.info(" ".join(kwargs.values()))
            self.send(**kwargs)


if __name__ == '__main__':
    start = "2019-01-01 00:00:00"
    end = "2023-02-12 00:00:00"

    # Write(source=OpContractInfo)(start=start, end=end)
    # Write(source=OpTargetQuote)(start=start, end=end)

    Write(source=OpNominalAmount)(start=start, end=end)
    # Write(source=OpContractQuote)(start=start, end=end, code="10004405.XSHG")
    # Write(source=PutdMinusCalld)(start=start, end=end)
