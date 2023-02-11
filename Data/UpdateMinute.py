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


class UpdateMinute:
    def __init__(self):
        self.logger = Logger()

        with open("../static/subscribe.json", 'r') as load_f:
            self.subscribe = json.load(load_f)["code_list"]
            if not self.subscribe:
                self.subscribe = None

    def update(self, source, **kwargs):
        self.logger.info(f"{source} 更新")
        Write(source=source)(**kwargs)
        self.logger.info(f"{source} {kwargs}")

        # self.W(source="OpContractQuote", start=start, end=end, code="10004237.XSHG")
        # self.W(source="OpNominalAmount", start=start, end=end, code="510050.XSHG")
        # self.W(source="PutdMinusCalld", start=start, end=end, code="510050.XSHG")

    def run(self):
        now = datetime.now().replace(second=0, microsecond=0)
        next = now + timedelta(minutes=1)

        start = str(now)
        end = str(next)

        task = [OpContractInfo, OpTargetQuote, OpContractQuote, OpNominalAmount, PutdMinusCalld]

        with ThreadPoolExecutor(max_workers=5) as e:
            all_task = [e.submit(self.update(source=t, start=start, end=end, code=self.subscribe)) for t in task]
            wait(all_task, return_when=ALL_COMPLETED)


if __name__ == "__main__":
    # u = UpdateMinute()
    now = datetime.now().replace(second=0, microsecond=0)
    print(str(now))
    next = now + timedelta(minutes=1)
    print(next)
