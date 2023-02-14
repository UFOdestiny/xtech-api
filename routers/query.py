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


@router.post("/")
@check_exception
async def get_data(data: QueryData):
    name = data.name
    # "%Y-%m-%d %H:%M:%S"
    time_series = data.time

    start_time = InfluxTime.utc(time_series[0])

    end_time = InfluxTime.utc(time_series[1])

    targetcode = data.targetcode
    opcode = data.opcode

    query = f"""
                from(bucket: "{influxdbService.INFLUX.bucket}")
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
    df = influxdbService.query_data_raw(query)

    # df.drop(["result", "table", "_start", "_stop", "_measurement"], axis=1, inplace=True)
    #
    # df["_time"] = df["_time"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"), inplace=True)
    res = df.values.tolist()

    for i in res[:10]:
        print(i)
    return res
