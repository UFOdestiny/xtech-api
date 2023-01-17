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
    name = data.name
    # "%Y-%m-%d %H:%M:%S"
    time_series = data.time
    start_time = time_series[0]
    end_time = time_series[1]

    if name == "opcontractquote":
        print("opcontractquote")
    elif name == "opcontractinfo":
        print("opcontractinfo")
    elif name == "opnominalamount":
        print("opnominalamount")
    elif name == "optargetquote":
        print("optargetquote")
