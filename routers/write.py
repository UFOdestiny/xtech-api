# -*- coding: utf-8 -*-
# @Name     : write.py
# @Date     : 2022/8/30 10:45
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from fastapi import APIRouter

from config import InfluxDBProduct as InfluxDB
from service.db_service import InfluxdbService
from service.response_service import check_exception
from utils.Model import Data

router = APIRouter()

# InfluxDB
influxdbService = InfluxdbService(influxdb=InfluxDB)


@router.post("/write")
@check_exception
async def write_data(data: Data):
    pass
    return
