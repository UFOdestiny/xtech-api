# -*- coding: utf-8 -*-
# @Name     : Data.py
# @Date     : 2022/9/9 9:52
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 登陆聚宽API，以便后续的查找。

from jqdatasdk import JQDataClient, auth
from config import JoinQuantSetting


class Authentication(JoinQuantSetting, type):
    def __call__(cls, *args, **kwargs):
        if not JQDataClient.instance():
            auth(cls.username, cls.password)
        return type.__call__(cls, *args, **kwargs)


if __name__ == "__main__":
    pass
