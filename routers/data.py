# -*- coding: utf-8 -*-
# @Name     : data.py
# @Date     : 2023/1/9 18:07
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
from fastapi import APIRouter

from config import InfluxDBProduct as InfluxDB
from service.InfluxService import InfluxdbService
from service.ResponseService import check_exception
from utils.Model import QueryData

router = APIRouter()

# InfluxDB
influxdbService = InfluxdbService(influxdb=InfluxDB)


@router.post("/query")
@check_exception
async def get_data(data: QueryData):
    name = data.query["name"]
    if name == "opcontractquote":
        pass
    elif name == "opcontractinfo":
        pass
    elif name == "opnominalamount":
        pass
    elif name == "optargetquote":
        pass
