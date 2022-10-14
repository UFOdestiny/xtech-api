# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/9/9 13:29
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from service.InfluxService import InfluxdbService


class WriteData:
    format_dict = {
        "optargetquote": "optargetquote,targetcode={1} price={2},pct={4} {0}",
        "opcontractinfo": "opcontractinfo,targetcode={1},opcode={2},type={5} price={4},days={8} {0}",
    }

    def __init__(self):
        self.db = InfluxdbService()
        self.q = []

    def generate(self, data):
        q = []
        keys = data.keys()
        for k in keys:
            fmt = self.format_dict[k]
            data = data[k]
            sequence = [fmt.format(*i) for i in data]
            q.extend(sequence)
        self.q = q

        # print(self.q)

    def send(self):
        self.db.write_data_execute(self.q)


class TargetQuote:
    from OpTargetQuote import OpTargetQuote
    op = OpTargetQuote()
    a = op.get(start='2021-09-01 00:00:00', end='2021-09-30 00:00:00')

    w = WriteData()
    w.generate({"optargetquote": a})
    w.send()


class ContractInfo:
    from OpContractInfo import OpContractInfo
    op = OpContractInfo()
    a = op.get(start='2021-09-01 00:00:00', end='2021-09-30 00:00:00')

    w = WriteData()
    w.generate({"opcontractinfo": a})
    w.send()


if __name__ == '__main__':
    ContractInfo()
