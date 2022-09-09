# -*- coding: utf-8 -*-
# @Name     : Singleton.py
# @Date     : 2022/9/9 12:47
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

class Singleton(type):
    def __call__(cls, *args, **kwargs):
        if hasattr(cls, "_instance"):
            return cls._instance
        cls._instance = type.__call__(cls, *args, **kwargs)
        return cls._instance
