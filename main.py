# -*- coding: utf-8 -*-
# @Name     : main.py
# @Date     : 2022/8/3 15:03
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 启动项目

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from routers import user, NotionalPrincipal, write, query, subscribe
from routers.tasks import repeat_task

app = FastAPI(title="X-TECH", version="1.0.0", )

app.add_middleware(
    CORSMiddleware,
    # 允许跨域的源列表，例如 ["http://www.example.org"] 等等，["*"] 表示允许任何源
    allow_origins=["*"],
    # 跨域请求是否支持 cookie，默认是 False，如果为 True，allow_origins 必须为具体的源，不可以是 ["*"]
    allow_credentials=False,
    # 允许跨域请求的 HTTP 方法列表，默认是 ["GET"]
    allow_methods=["*"],
    # 允许跨域请求的 HTTP 请求头列表，默认是 []，可以使用 ["*"] 表示允许所有的请求头
    # 当然 Accept、Accept-Language、Content-Language 以及 Content-Type 总之被允许的
    allow_headers=["*"],
    # 可以被浏览器访问的响应头, 默认是 []，一般很少指定
    # expose_headers=["*"]
    # 设定浏览器缓存 CORS 响应的最长时间，单位是秒。默认为 600，一般也很少指定
    # max_age=1000
)

# 引入路由
app.include_router(user.router, prefix="/user", tags=["user"], )

app.include_router(write.router, prefix="/write", tags=["write"], )

app.include_router(query.router, prefix="/query", tags=["query"], )

app.include_router(NotionalPrincipal.router, prefix="/NotionalPrincipal", tags=["panel"], )

app.include_router(subscribe.router, prefix="/subscribe", tags=["subscribe"], )


@app.on_event("startup")
@repeat_task(seconds=60, wait_first=True)
def repeat_loop() -> None:
    # u = Update()
    # u.run()
    pass


if __name__ == '__main__':
    import uvicorn

    uvicorn.run('main:app', host='127.0.0.1', port=8000, reload=True, debug=True, workers=1)
