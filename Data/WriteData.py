# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/9/9 13:29
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from service.InfluxService import InfluxdbService

# 不能删除下面的导入！因为用到了eval！
from OpTargetQuote import OpTargetQuote
from OpContractInfo import OpContractInfo
from OpContractQuote import OpContractQuote


class WriteData:
    format_dict = {
        "optargetquote": "optargetquote,targetcode={1} price={2},pct={3} {0}",
        "opcontractinfo": "opcontractinfo,opcode={1},targetcode={2},type={4} multiplier={5},strikeprice={3},days={6} {0}",
        "opcontractquote": "opcontractquote,opcode={1},targetcode={2} open={3},close={4},high={5},low={6},amount={7},"
                           "vol={8},oi={9},a1_v={10},a1_p={11},b1_v={12},b1_p={13},pct={14},delta+={15},delta-={16},"
                           "gamma={17},vega={18},theta+={19},theta-={20},iv={21},timevalue={22} {0}",
    }

    def __init__(self):
        self.db = InfluxdbService()
        self.q = []
        self.source = None
        self.S = None
        self.data = None

    def set_source(self, source):
        self.source = source
        self.S = eval(f"{source}()")

    def set_time(self, **kwargs):
        d = self.S.get(**kwargs)
        self.data = {self.source.lower(): d}

    def generate(self):
        data = self.data

        q = []
        keys = data.keys()
        for k in keys:
            fmt = self.format_dict[k]
            data = data[k]
            sequence = [fmt.format(*i) for i in data]
            q.extend(sequence)
        self.q = q

    def send(self):
        self.db.write_data_execute(self.q)


class Write(WriteData):
    def __call__(self, **kwargs):
        self.set_source(kwargs["source"])
        self.set_time(**kwargs)
        self.generate()
        self.send()


if __name__ == '__main__':
    start = "2022-11-01 00:00:00"
    end = "2022-11-30 23:00:00"

    # Write()(source="OpContractInfo", start=start, end=end)
    # Write()(source="OpTargetQuote", start=start, end=end)
    Write()(source="OpContractQuote", start=start, end=end, code="10004237.XSHG")
