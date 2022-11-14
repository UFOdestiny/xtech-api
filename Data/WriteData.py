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
        "optargetquote": "optargetquote,targetcode={1} price={2},pct={4} {0}",
        "opcontractinfo": "opcontractinfo,opcode={1},targetcode={2},type={5} multiplier={6},strikeprice={4},days={9} {0}",
        "opcontractquote": "opcontractquote,targetcode={1},opcode={2},type={5} price={4},days={9},multiplier={6} {0}",
        "opcontractgreek": "opcontractquote,targetcode={1},opcode={2},type={5} price={4},days={9},multiplier={6} {0}",
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

    def set_time(self, start, end):
        d = self.S.get(start, end)
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
    def __call__(self,
                 source="OpContractInfo",
                 start='2022-09-01 00:00:00',
                 end='2022-09-30 00:00:00'):
        self.set_source(source)
        self.set_time(start=start, end=end)
        self.generate()
        self.send()


if __name__ == '__main__':
    Write()("OpContractInfo", "2022-01-01 00:00:00", "2022-10-01 00:00:00")
