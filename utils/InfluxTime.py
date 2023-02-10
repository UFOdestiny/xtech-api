# -*- coding: utf-8 -*-
# @Name     : InfluxTime.py
# @Date     : 2022/8/30 15:37
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : influxDB时间格式处理
import time
from pandas._libs.tslibs.timestamps import Timestamp
from datetime import datetime, timedelta


class InfluxTime:
    @staticmethod
    def to_influx_time(t):
        influx_format = "%Y-%m-%dT%H:%M:%SZ"
        yearmd_format = '%Y-%m-%d'
        yearmd_hourm_format = "%Y-%m-%d %H:%M"
        yearmd_hourms_format = "%Y-%m-%d %H:%M:%S"

        if type(t) == str:
            if t.isnumeric():
                if len(t) >= 12:
                    return time.strftime(influx_format, time.localtime(int(t) / 1000 - 3600 * 8))
            elif len(t) == 10:  # 2022-08-09
                structure = time.strptime(t, yearmd_format)
                return time.strftime(influx_format, structure)
            elif len(t) == 16:
                structure = time.strptime(t, yearmd_hourm_format)
                return time.strftime(influx_format, structure)
            elif len(t) == 19:
                structure = time.strptime(t, yearmd_hourms_format)
                return time.strftime(influx_format, structure)
            elif len(t) == 20:
                # return time.strptime(t, influx_format)
                return t

        elif type(t) == int:
            return time.strftime(influx_format, time.localtime(t / 10 ** 3))

        elif type(t) == float:
            return time.strftime(influx_format, time.localtime(t))

        elif type(t) == Timestamp:
            return time.strftime(yearmd_hourms_format, time.localtime(t.value / 10 ** 9))

    @staticmethod
    def now():
        return InfluxTime.to_influx_time(time.time())


class SplitTime:
    def __init__(self):
        self.format = "%Y-%m-%d %H:%M:%S"

    def split(self, start, end, interval_day=7):
        start = datetime.strptime(start, self.format)
        end = datetime.strptime(end, self.format)
        interval = timedelta(days=interval_day)
        result = []

        while end - start > interval:
            result.append([start, start + interval])
            start += interval

        result.append([start, end])

        for i in range(len(result)):
            for j in [0, 1]:
                result[i][j] = result[i][j].strftime(self.format)

        return result


if __name__ == '__main__':
    # print(InfluxTime.to_influx_time("2022-08-09"))
    # print(InfluxTime.to_influx_time("2022-08-09 10:28"))
    # print(InfluxTime.to_influx_time("2022-08-09 10:28:00"))
    # print(InfluxTime.to_influx_time("2022-08-09T10:50:00Z"))
    # print(InfluxTime.to_influx_time(time.time()))
    # print(InfluxTime.to_influx_time('1660026181729'))
    # print(InfluxTime.to_influx_time('1660026181729'))
    #
    # print(InfluxTime.now())
    # print(time.time())

    s = SplitTime()

    print(s.split("2022-08-09 10:27:00", "2022-08-10 10:28:00", interval_day=1))
