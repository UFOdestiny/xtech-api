# -*- coding: utf-8 -*-
# @Name     : Update.py
# @Date     : 2023/1/9 18:38
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
import os

from utils.Logger import Logger
from Data.WriteData import Write

from Data.OpTargetQuote import OpTargetQuote
from Data.OpContractInfo import OpContractInfo
from Data.OpContractQuote import OpContractQuote
from Data.OpNominalAmount import OpNominalAmount
from Data.PutdMinusCalld import PutdMinusCalld
import time
import threading


class Update:
    def __init__(self):
        self.logger = Logger()
        self.format = "%Y-%m-%d %H:%M:%S"

        self.subscribe = os.getenv("SUBSCRIBE", None)

    def update(self, arg, start, end, code):
        self.logger.info(f"{arg} 更新")
        Write(source=arg)(start=start, end=end, code=code)

        # self.W(source="OpContractQuote", start=start, end=end, code="10004237.XSHG")
        # self.W(source="OpNominalAmount", start=start, end=end, code="510050.XSHG")
        # self.W(source="PutdMinusCalld", start=start, end=end, code="510050.XSHG")

    def run(self):
        now = time.time()
        s = now - 15
        e = now + 15
        start = time.strftime(self.format, time.localtime(s))
        end = time.strftime(self.format, time.localtime(e))

        t1 = threading.Thread(target=self.update, args=(OpContractInfo, start, end))
        t2 = threading.Thread(target=self.update, args=(OpTargetQuote, start, end))
        t3 = threading.Thread(target=self.update, args=(OpContractQuote, start, end, self.subscribe))
        t4 = threading.Thread(target=self.update, args=(OpNominalAmount, start, end))
        t5 = threading.Thread(target=self.update, args=(PutdMinusCalld, start, end))

        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
