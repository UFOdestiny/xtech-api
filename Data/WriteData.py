# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/9/9 13:29
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from service.InfluxService import InfluxdbService


class WriteData:
    format_dict = {
        "optargetquote": "optargetquote,targetcode={1} price={2},pct={4} {0}"
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


if __name__ == '__main__':
    from OpTargetQuote import OpTargetQuote

    op = OpTargetQuote()
    a = op.get()

    w = WriteData()
    w.generate({"optargetquote": a})
    w.send()
