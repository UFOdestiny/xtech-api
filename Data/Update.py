# -*- coding: utf-8 -*-
# @Name     : Update.py
# @Date     : 2023/1/9 18:38
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from utils.Logger import Logger
from WriteData import Write
import time


class Update:
    def __init__(self):
        self.logger = Logger()
        self.W = Write()
        self.format = "%Y-%m-%d %H:%M:%S"

    def update(self):
        now = time.time()
        s = now - 30
        e = now + 30
        start = time.strftime(self.format, time.localtime(s))
        end = time.strftime(self.format, time.localtime(e))

        self.W(source="OpContractInfo", start=start, end=end)
        self.W(source="OpTargetQuote", start=start, end=end)
        self.W(source="OpContractQuote", start=start, end=end, code="10004237.XSHG")
        self.W(source="OpNominalAmount", start=start, end=end, code="510050.XSHG")
        self.W(source="PutdMinusCalld", start=start, end=end, code="510050.XSHG")

    def run(self):
        self.update()
        self.logger.info("数据更新")
