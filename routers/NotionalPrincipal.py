# -*- coding: utf-8 -*-
# @Name     : user.py
# @Date     : 2022/8/11 14:37
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 名义

import random

from fastapi import APIRouter, Depends

from routers.verify import verify_token
from service.InfluxService import InfluxdbService
from service.ResponseService import check_exception
from utils.InfluxTime import InfluxTime
from utils.Model import TimeRange, Day

router = APIRouter(dependencies=[Depends(verify_token)])

# InfluxDB
influxdbService = InfluxdbService()


@router.post("/all")
@check_exception
async def all_data(time: TimeRange):
    start = InfluxTime.utc(time.start)
    stop = InfluxTime.utc(time.stop)
    tables = influxdbService.query_data(start=start, stop=stop)

    # unpack = [[round(table.get_time().timestamp() * 1000), table.get_value(), random.random() * 15] for table in
    #           tables]
    fake = []
    for table in tables[:50]:
        temp = [round(table.get_time().timestamp() * 1000)]
        for _ in range(11):
            temp.append(random.random() * 100)
        fake.append(temp)
    # print(fake)
    return fake


@router.post("/right")
@check_exception
async def right(day: Day):
    fake = []
    for table in range(10):
        temp = [1]
        fake.append(temp)
    # print(fake)
    return fake
