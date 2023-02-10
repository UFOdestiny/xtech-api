# -*- coding: utf-8 -*-
# @Name     : query.py
# @Date     : 2023/1/9 18:07
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from fastapi import APIRouter

from service.InfluxService import InfluxdbService
from service.ResponseService import check_exception
from utils.InfluxTime import InfluxTime
from utils.Model import QueryData

router = APIRouter()

# InfluxDB
influxdbService = InfluxdbService()


@router.post("/query")
@check_exception
async def get_data(data: QueryData):
    name = data.name
    # "%Y-%m-%d %H:%M:%S"
    time_series = data.time

    start_time = InfluxTime.to_influx_time(time_series[0])

    end_time = InfluxTime.to_influx_time(time_series[1])

    targetcode = data.targetcode
    opcode = data.opcode

    query = f"""
                from(bucket: "xtech")
                  |> range(start: {start_time}, stop: {end_time})
                  |> filter(fn: (r) => r["_measurement"] == "{name}")
                  |> filter(fn: (r) => r["targetcode"] == "{targetcode}")
                  
            """

    if name in ["optargetquote", "opnominalamount", "putdminuscalld"]:
        pass

    if name in ["opcontractquote", "opcontractinfo"]:
        query += f"""|> filter(fn: (r) => r["opcode"] == "{opcode}")"""

    query += """|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")"""

    # print(query)
    df = influxdbService.query_api.query_data_frame(query)
    df.drop(["result", "table", "_start", "_stop"], axis=1, inplace=True)

    df["_time"] = df["_time"].apply(lambda x: str(x)[:-6])
    res = df.values.tolist()
    return res
