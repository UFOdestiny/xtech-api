# -*- coding: utf-8 -*-
# @Name     : OpTargetQuote.py
# @Date     : 2022/9/9 9:51
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
from datetime import datetime

from jqdatasdk import get_price, normalize_code

from JoinQuant import Authentication

index = ["000300", "000852"]


class OpTargetQuote(metaclass=Authentication):
    code_pre = [510050, 510300, 159919, ]

    def __init__(self):
        self.code = normalize_code(self.code_pre)
        self.df = None
        # datetime targetcode price pct

        self.result = []

    def get_data(self, start='2020-09-14 00:00:00', end='2021-09-14 00:00:00'):
        # 'open', 'close', 'low', 'high', 'volume', 'money', 'factor','high_limit', 'low_limit', 'avg', 'pre_close'
        self.df = get_price(security=self.code,
                            start_date=start,
                            end_date=end,
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

            origin_time = datetime.timestamp(self.result[i][0]) - 60
            print(origin_time)
            # time_ = InfluxTime.to_influx_time(origin_time)
            self.result[i][0] = f"{origin_time * 1e9:.0f}"

    def get(self, start='2021-09-09 00:00:00', end='2021-09-14 00:00:00'):
        self.get_data(start, end)
        self.process_df()
        return self.result


if __name__ == "__main__":
    op = OpTargetQuote()
    a = op.get()
    print(len(a))
