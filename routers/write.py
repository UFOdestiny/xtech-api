# -*- coding: utf-8 -*-
# @Name     : write.py
# @Date     : 2022/8/30 10:45
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 数据写入

from fastapi import APIRouter

from config import InfluxDBProduct as InfluxDB
from service.InfluxService import InfluxService
from service.ResponseService import check_exception
from utils.Model import Data


router = APIRouter()

# InfluxDB
influxdbService = InfluxService()


@router.post("/write")
@check_exception
async def write_data(data: Data):
    username = data.header["username"]
    password = data.header["password"]
    if username and password:
        pass
    content = data.body
    print(content)
