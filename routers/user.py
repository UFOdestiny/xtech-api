# -*- coding: utf-8 -*-
# @Name     : user.py
# @Date     : 2022/8/11 14:37
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from datetime import timedelta

from fastapi import APIRouter, Depends

from config import TokenConfig
from service.MysqlService import MysqlService
from service.ResponseService import check_exception
from routers.verify import verify_token, create_access_token, password_exception
from utils.AES import Aes
from utils.Model import UserLogin

mysql = MysqlService()
router = APIRouter()

aes = Aes()


@router.get('/test/{name}', dependencies=[Depends(verify_token)])
@check_exception
async def fmt(name):
    print(type(name))
    new_name = name.title()
    return {'result': new_name}


@router.post("/login")
@check_exception
async def login_for_access_token(user: UserLogin):  # form_data通过表单数据来发送信息
    deciphered_username = aes.decrypt(user.username)
    success = mysql.get_user(deciphered_username, user.password)
    if success:
        access_token = create_access_token(
            data={"username": user.username, "password": user.password},  # 这里的data字典内容随意，可以是用户名或用户ID
            expires_delta=timedelta(minutes=TokenConfig.ACCESS_TOKEN_EXPIRE_MINUTES)  # token有效时间
        )
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        raise password_exception
