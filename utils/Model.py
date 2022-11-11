# -*- coding: utf-8 -*-
# @Name     : Model.py
# @Date     : 2022/8/30 15:36
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : API模型

from pydantic import BaseModel


class UserLogin(BaseModel):
    username: str
    password: str


class TimeRange(BaseModel):
    start: str
    stop: str


class Day(BaseModel):
    day: str


class Data(BaseModel):
    header: dict
    body: dict
