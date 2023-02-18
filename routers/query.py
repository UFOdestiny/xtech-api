# -*- coding: utf-8 -*-
# @Name     : query.py
# @Date     : 2023/1/9 18:07
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from fastapi import APIRouter

from service.InfluxService import InfluxService
from service.ResponseService import check_exception
from utils.Model import QueryData

router = APIRouter()

# InfluxDB
influxdbService = InfluxService()


@router.post("/")
@check_exception
async def get_data(data: QueryData):
    name = data.name
    # "%Y-%m-%d %H:%M:%S"
    time_series = data.time
    targetcode = data.targetcode

    opcode = data.opcode
    if not opcode:
        opcode = None

    df = influxdbService.query_influx(start=time_series[0], end=time_series[1], measurement=name,
                                      targetcode=targetcode, opcode=opcode, df=False)

    res = df.values.tolist()

    for i in res[:10]:
        print(i)
    return res
