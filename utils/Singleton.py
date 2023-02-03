# -*- coding: utf-8 -*-
# @Name     : Singleton.py
# @Date     : 2022/9/9 12:47
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : 单例
import threading


class Singleton(type):
    def __call__(cls, *args, **kwargs):
        if hasattr(cls, "_instance"):
            # print("repeat")
            return cls._instance
        cls._instance = type.__call__(cls, *args, **kwargs)
        return cls._instance


class Singleton2(type):
    _instance = None
    _init_flag = True
    _instance_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with Singleton2._instance_lock:
            if cls._instance:
                print("跳过实例化")
                return cls._instance
            else:
                cls._instance = type.__call__(cls, *args, **kwargs)
                return cls._instance
