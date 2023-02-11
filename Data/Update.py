# -*- coding: utf-8 -*-
# @Name     : Update.py
# @Date     : 2023/1/9 18:38
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import json
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from datetime import timedelta, datetime

from Data.OpContractInfo import OpContractInfo
from Data.OpContractQuote import OpContractQuote
from Data.OpNominalAmount import OpNominalAmount
from Data.OpTargetQuote import OpTargetQuote
from Data.PutdMinusCalld import PutdMinusCalld
from Data.WriteData import Write
from utils.Logger import Logger


class Update:
    def __init__(self):
        self.logger = Logger()

        with open("../static/subscribe.json", 'r') as load_f:
            self.subscribe = json.load(load_f)["code_list"]
            if not self.subscribe:
                self.subscribe = None

        self.subscribe = "10004405.XSHG"

    def update(self, source, **kwargs):
        Write(source=source)(**kwargs)
        self.logger.info(f"UPDATE {source.__name__} {list(kwargs.values())}")

    def execute(self, start, end):
        task = [OpTargetQuote, OpContractQuote, OpNominalAmount, PutdMinusCalld]
        with ThreadPoolExecutor(max_workers=4) as e:
            all_task = [e.submit(self.update(source=t, start=start, end=end, code=self.subscribe)) for t in task]
            wait(all_task, return_when=ALL_COMPLETED)

    def run(self):
        now = datetime.now().replace(second=0, microsecond=0)
        next = now + timedelta(minutes=1)

        day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day1 = now.replace(hour=23, minute=0, second=0, microsecond=0)

        day930 = now.replace(hour=9, minute=30, second=0, microsecond=0)
        day1130 = now.replace(hour=11, minute=30, second=0, microsecond=0)
        day1230 = now.replace(hour=12, minute=30, second=0, microsecond=0)
        day1730 = now.replace(hour=17, minute=30, second=0, microsecond=0)

        start = str(now)
        end = str(next)

        if now.hour == 7 and now.minute == 0:
            self.update(source=OpContractInfo, start=str(day0), end=str(day1))
            return
        elif (now.hour == 11 and now.minute == 0) or (now.hour == 18 and now.minute == 0):
            self.subscribe = None
            start = str(now.replace(hour=1, second=0, microsecond=0))
            end = str(next.replace(hour=23, second=0, microsecond=0))
            self.execute(start, end)
        elif day930 < now < day1130 or day1230 < now < day1730:
            self.execute(start, end)
        else:
            self.logger.info("UPDATE NOT NOW")

        # start = '2023-02-10 09:30:00'
        # end = '2023-02-10 09:40:00'

        # task = [OpTargetQuote, OpContractQuote, OpNominalAmount, PutdMinusCalld]
        # task = [OpContractQuote]


if __name__ == "__main__":
    u = Update()
    u.run()
