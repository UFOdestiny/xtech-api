# -*- coding: utf-8 -*-
# @Name     : getdata.py
# @Date     : 2023/2/14 14:52
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from service.InfluxService import InfluxService


class GetData:
    def __init__(self):
        self.db = InfluxService()

    def get_data(self, start, end, measurement, targetcode=None, opcode=None, df=True, keep=None, filter_=None):
        return self.db.query_influx(start, end, measurement, targetcode=targetcode, opcode=opcode, df=df, keep=keep,
                                    filter_=filter_)
