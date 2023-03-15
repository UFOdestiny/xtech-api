# -*- coding: utf-8 -*-
# @Name     : Data.py
# @Date     : 2022/9/9 9:52
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 聚宽API的验权，数据获取的父类。
import time
from threading import Lock

from jqdatasdk import JQDataClient, auth, query, opt, get_price, get_ticks
from thriftpy2.transport import TTransportException

from config import JoinQuantSetting


class Authentication(JoinQuantSetting, type):
    """
    聚宽接口验权，单例模式，但是无效，因为auth内部使用了local函数，每一个线程都会重新实例化。
    """
    _instance_lock = Lock()
    _auth = None

    def __call__(cls, *args, **kwargs):
        with cls._instance_lock:
            if cls._auth:
                # print(id(cls), cls.__name__, "跳过实例化")
                pass
            else:
                # print(id(cls), cls.__name__, cls._auth)
                auth(cls.username, cls.password)
                cls._auth = "already"

        return type.__call__(cls, *args, **kwargs)


class JQData(metaclass=Authentication):
    def __init__(self):
        self.adjust = None
        self.targetcodes = ['510050.XSHG', '510300.XSHG', '159919.XSHE', '510500.XSHG', '159915.XSHE', '159901.XSHE',
                            '159922.XSHE', '000852.XSHG', '000300.XSHG', "000016.XSHG"]

        # self.targetcodes = ['510500.XSHG']

    def run_query(self, q):
        max_retries = 10
        while max_retries:
            try:
                return opt.run_query(q)
            except TTransportException:
                print(f"run_query error: Retry {max_retries}")
                max_retries -= 1
                time.sleep(3)

    def get_adjust(self):
        """
        获取全部合约调整信息
        :return:
        """
        q = query(opt.OPT_ADJUSTMENT.adj_date,
                  opt.OPT_ADJUSTMENT.code,
                  opt.OPT_ADJUSTMENT.ex_exercise_price,
                  opt.OPT_ADJUSTMENT.ex_contract_unit)

        df = self.run_query(q)
        df.dropna(how="any", inplace=True)
        df.drop_duplicates(keep="first", inplace=True)

        self.adjust = df
        return df

    def get_price(self, **kwargs):
        max_retries = 10
        while max_retries:
            try:
                df = get_price(**kwargs)
                return df
            except TTransportException:
                print(f"get_price error: Retry {max_retries}")
                max_retries -= 1
                time.sleep(3)

    def get_ticks(self, **kwargs):
        max_retries = 10
        while max_retries:
            try:
                df = get_ticks(**kwargs)
                return df
            except TTransportException:
                print(f"get_ticks error: Retry {max_retries}")
                max_retries -= 1
                time.sleep(3)


if __name__ == "__main__":
    print(JQDataClient.instance())
    auth("15210597532", "jin00904173")
    print(JQDataClient.instance())
