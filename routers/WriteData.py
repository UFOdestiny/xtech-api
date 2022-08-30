# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/8/30 10:45
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import random

from fastapi import APIRouter

from config import InfluxDB116 as InfluxDB
from db_service import InfluxdbService
from response_service import check_exception
from utils import InfluxTime, TimeRange, Day

router = APIRouter()

# InfluxDB
influxdbService = InfluxdbService(influxdb=InfluxDB)


@router.post("/write")
@check_exception
async def all_data(time: TimeRange):
    start = InfluxTime.to_influx_time(time.start)
    stop = InfluxTime.to_influx_time(time.stop)
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
