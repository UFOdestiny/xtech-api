# -*- coding: utf-8 -*-
# @Name     : write.py
# @Date     : 2022/8/30 10:45
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import random

from fastapi import APIRouter

from config import InfluxDB116 as InfluxDB
from db_service import InfluxdbService
from response_service import check_exception
from utils.Model import Data
from utils.InfluxTime import InfluxTime

router = APIRouter()

# InfluxDB
influxdbService = InfluxdbService(influxdb=InfluxDB)


@router.post("/write")
@check_exception
async def write_data(data: Data):
    pass
    return
