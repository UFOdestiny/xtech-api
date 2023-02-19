# -*- coding: utf-8 -*-
# @Name     : Data.py
# @Date     : 2022/9/9 9:52
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 登陆聚宽API，以便后续的查找。

from jqdatasdk import JQDataClient, auth, query, opt
from sqlalchemy import or_

from config import JoinQuantSetting
from threading import Lock


class Authentication(JoinQuantSetting, type):
    _instance_lock = Lock()
    _auth = None

    def __call__(cls, *args, **kwargs):
        with cls._instance_lock:
            if cls._auth:
                print(id(cls), cls.__name__, "跳过实例化")
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

        self.query_underlying_symbol = or_(opt.OPT_CONTRACT_INFO.underlying_symbol == "510050.XSHG",
                                           opt.OPT_CONTRACT_INFO.underlying_symbol == "510300.XSHG",
                                           opt.OPT_CONTRACT_INFO.underlying_symbol == "159919.XSHE",
                                           opt.OPT_CONTRACT_INFO.underlying_symbol == "159915.XSHE",
                                           opt.OPT_CONTRACT_INFO.underlying_symbol == "159901.XSHE",
                                           opt.OPT_CONTRACT_INFO.underlying_symbol == "159922.XSHE",
                                           opt.OPT_CONTRACT_INFO.underlying_symbol == "000852.XSHG",
                                           opt.OPT_CONTRACT_INFO.underlying_symbol == "000300.XSHG",
                                           opt.OPT_CONTRACT_INFO.underlying_symbol == "000016.XSHG", )

    def get_adjust(self):
        q = query(opt.OPT_ADJUSTMENT.adj_date,
                  opt.OPT_ADJUSTMENT.code,
                  opt.OPT_ADJUSTMENT.ex_exercise_price,
                  opt.OPT_ADJUSTMENT.ex_contract_unit)

        df = opt.run_query(q)
        df.dropna(how="any", inplace=True)
        df.drop_duplicates(keep="first", inplace=True)
        self.adjust = df
        return df


if __name__ == "__main__":
    print(JQDataClient.instance())
    auth("15210597532", "jin00904173")
    print(JQDataClient.instance())
