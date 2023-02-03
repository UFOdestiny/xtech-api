# -*- coding: utf-8 -*-
# @Name     : Data.py
# @Date     : 2022/9/9 9:52
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 登陆聚宽API，以便后续的查找。

from jqdatasdk import JQDataClient, auth
from config import JoinQuantSetting
from threading import Lock


class Authentication(JoinQuantSetting, type):
    _instance_lock = Lock()
    _auth = None

    def __call__(cls, *args, **kwargs):
        with cls._instance_lock:
            if cls._auth:
                # print(id(cls), cls.__name__, "跳过实例化")
                pass
            else:
                # print(id(cls), cls.__name__, cls._auth)
                auth(cls.username, cls.password)
                cls._auth = "already"

        return type.__call__(cls, *args, **kwargs)


if __name__ == "__main__":
    print(JQDataClient.instance())
    auth("15210597532", "jin00904173")
    print(JQDataClient.instance())
