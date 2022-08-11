# -*- coding: utf-8 -*-
# @Name     : user.py
# @Date     : 2022/8/11 14:37
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :
from fastapi import APIRouter

from response_service import ResponseService, check_exception
from model import UserLogin

router = APIRouter()
# 返回服务
result = ResponseService()


@router.post("/login")
@check_exception
async def login(user: UserLogin):
    # mysql = MysqlService()
    return result.return_success(user.username, user.password)
