# -*- coding: utf-8 -*-
# @Name     : verify.py
# @Date     : 2022/8/12 14:35
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : token认证

from datetime import datetime, timedelta
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt import ExpiredSignatureError

from config import TokenConfig

credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Token认证失败！",
    headers={"WWW-Authenticate": "Bearer"},
)

password_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="账号或密码错误！",
    headers={"WWW-Authenticate": "Bearer"},
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


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


async def verify_token(token=Depends(oauth2_scheme)):
    try:
        return jwt.decode(token, TokenConfig.SECRET_KEY, TokenConfig.ALGORITHM)
    except ExpiredSignatureError:
        raise credentials_exception
