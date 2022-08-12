# -*- coding: utf-8 -*-
# @Name     : user.py
# @Date     : 2022/8/11 14:37
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import random

from fastapi import APIRouter

from config import InfluxDBLocal as InfluxDB
from db_service import InfluxdbService
from model import TimeRange
from response_service import check_exception
from utils import InfluxTime

router = APIRouter()

# InfluxDB
influxdbService = InfluxdbService(influxdb=InfluxDB)


@router.post("/price")
@check_exception
async def targetquote_price(time: TimeRange):
    start = InfluxTime.to_influx_time(time.start)
    stop = InfluxTime.to_influx_time(time.stop)

    tables = influxdbService.query_data(start=start, stop=stop)

    if time.type == 1:
        unpack = [[round(table.get_time().timestamp() * 1000), table.get_value(), random.random() * 15] for table in
                  tables]
    elif time.type == 2:
        unpack = [[int(time.start) + i * 1000, random.random(), random.random()] for i in range(20)]

    # print(len(unpack))
    return unpack
