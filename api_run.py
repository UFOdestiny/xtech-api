# -*- coding: utf-8 -*-
# @Name     : api_run.py
# @Date     : 2022/8/3 15:03
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : start the api project

from typing import Union
from fastapi import FastAPI
from db_service import MysqlService
from response_service import ResponseService, check_exception
from pydantic import BaseModel


class UserLogin(BaseModel):
    username: str
    password: str


app = FastAPI()


@app.get("/")
@check_exception
async def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
@check_exception
async def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.post("/login")
@check_exception
async def login(user: UserLogin):
    # mysql = MysqlService()
    res = ResponseService()
    return res.return_success(user.username, user.password)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run('api_run:app', host='127.0.0.1', port=8000, reload=True, debug=True, workers=1)
