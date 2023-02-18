# -*- coding: utf-8 -*-
# @Name     : subscribe.py
# @Date     : 2023/2/7 21:56
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import json

from fastapi import APIRouter

from service.InfluxService import InfluxService
from service.ResponseService import check_exception
from utils.Model import Subscribe

router = APIRouter()

influxdbService = InfluxService()


@router.post("/")
@check_exception
async def get_data(data: Subscribe):
    code_list = data.code_list
    dct = {'code_list': code_list}
    with open("../static/subscribe.json", "w") as f:
        json.dump(dct, f)
