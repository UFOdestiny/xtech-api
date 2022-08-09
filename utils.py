# -*- coding: utf-8 -*-
# @Name     : utils.py
# @Date     : 2022/8/9 10:33
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from datetime import datetime
import time


class InfluxTime:
    @staticmethod
    def to_influx_time(t):
        if type(t) == str:
            return time.strptime(t, '%Y-%m-%dT%H:%M:%SZ')
        elif type(t) == float:
            return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(t))


if __name__ == '__main__':
    print(InfluxTime.to_influx_time(time.time()))
    print(InfluxTime.to_influx_time("2022-08-09T10:50:00Z"))
