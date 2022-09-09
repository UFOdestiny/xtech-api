# -*- coding: utf-8 -*-
# @Name     : OpTargetQuote.py
# @Date     : 2022/9/9 9:51
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from jqdatasdk import get_price, normalize_code

from JoinQuant import Authentication

index = ["000300", "000852"]


class OpTargetQuote(metaclass=Authentication):
    code_pre = [510050, 510300, 159919]

    def __init__(self):
        self.code = normalize_code(self.code_pre)
        self.df = None
        # datetime targetcode price pct

        self.result = []

    def get_data(self):
        # 'open', 'close', 'low', 'high', 'volume', 'money', 'factor','high_limit', 'low_limit', 'avg', 'pre_close'
        self.df = get_price(security=self.code,
                            start_date='2015-01-28 09:00:00',
                            end_date='2015-01-30 14:00:00',
                            fq='pre',
                            frequency='minute',
                            fields=['close', 'pre_close'],
                            panel=False)

    def process_df(self):
        self.result = self.df.values.tolist()

        for i in range(len(self.result)):
            close = self.result[i][-2]
            pre_close = self.result[i][-1]
            pct = (close - pre_close) / pre_close
            self.result[i].append(pct)

    def get(self):
        self.get_data()
        self.process_df()
        return self.result


if __name__ == "__main__":
    op = OpTargetQuote()
    a = op.get()
    for j in a[:100]:
        print(j)
