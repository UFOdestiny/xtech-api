# -*- coding: utf-8 -*-
# @Name     : user.py
# @Date     : 2022/8/11 14:37
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from datetime import datetime, timedelta
from typing import Optional

import jwt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt import ExpiredSignatureError

from utils import Aes, UserLogin
from config import TokenConfig
from db_service import MysqlService
from response_service import check_exception

mysql = MysqlService()
router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")
aes = Aes()


# 生成token
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    # 检测token的有效时间是否为空，如果为空，则默认设置有效时间为60分钟
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=TokenConfig.ACCESS_TOKEN_EXPIRE_MINUTES)
    # 更新到我们之前传进来的字典
    to_encode.update({"exp": expire})
    # jwt 编码 生成我们需要的token
    encoded_jwt = jwt.encode(payload=to_encode, key=TokenConfig.SECRET_KEY, algorithm=TokenConfig.ALGORITHM)
    return encoded_jwt


credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="认证失败！",
    headers={"WWW-Authenticate": "Bearer"},
)


async def verify_token(token=Depends(oauth2_scheme)):
    try:
        return jwt.decode(token, TokenConfig.SECRET_KEY, TokenConfig.ALGORITHM)
    except ExpiredSignatureError:
        raise credentials_exception


@router.get('/test/{name}', dependencies=[Depends(verify_token)])
@check_exception
async def fmt(name):
    print(type(name))
    new_name = name.title()
    return {'result': new_name}


# form_data: OAuth2PasswordRequestForm = Depends()
# 请求接口
# 定义url路径，以及相应模型格式
@router.post("/login")
@check_exception
async def login_for_access_token(user: UserLogin):  # form_data通过表单数据来发送信息
    deciphered_username = aes.decrypt(user.username)
    success = mysql.get_user(deciphered_username, user.password)
    print(success)
    if success:
        access_token = create_access_token(
            data={"username": user.username, "password": user.password},  # 这里的data字典内容随意，可以是用户名或用户ID
            expires_delta=timedelta(minutes=TokenConfig.ACCESS_TOKEN_EXPIRE_MINUTES)  # token有效时间
        )
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        raise credentials_exception
