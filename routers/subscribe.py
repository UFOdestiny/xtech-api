# -*- coding: utf-8 -*-
# @Name     : subscribe.py
# @Date     : 2023/2/7 21:56
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from fastapi import APIRouter

from service.InfluxService import InfluxdbService
from service.ResponseService import check_exception
from utils.Model import Subscribe

router = APIRouter()

influxdbService = InfluxdbService()


@router.post("/query")
@check_exception
async def get_data(data: Subscribe):
    code_list = data.code_list
