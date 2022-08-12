# -*- coding: utf-8 -*-
# @Name     : model.py
# @Date     : 2022/8/5 13:23
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : data mode

from pydantic import BaseModel


class UserLogin(BaseModel):
    username: str
    password: str


class TimeRange(BaseModel):
    start: str
    stop: str
    type: int


class Token(BaseModel):
    access_token: str
    token_type: str
