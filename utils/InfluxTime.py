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
    def utc(t):
        influx_format = "%Y-%m-%dT%H:%M:%SZ"
        yearmd_format = '%Y-%m-%d'
        yearmd_hourm_format = "%Y-%m-%d %H:%M"
        yearmd_hourms_format = "%Y-%m-%d %H:%M:%S"

        if type(t) == str:  # -8h
            timestamp = time.mktime(time.strptime(t, yearmd_hourms_format)) - 3600 * 8
            structure = time.localtime(timestamp)
            return time.strftime(influx_format, structure)
            # if t.isnumeric():
            #     if len(t) >= 12:
            #         return time.strftime(influx_format, time.localtime(int(t) / 1000 - 3600 * 8))
            # elif len(t) == 10:  # 2022-08-09
            #     structure = time.strptime(t, yearmd_format)
            #     return time.strftime(influx_format, structure)
            # elif len(t) == 16:
            #     structure = time.strptime(t, yearmd_hourm_format)
            #     return time.strftime(influx_format, structure)
            # elif len(t) == 19:
            #     structure = time.strptime(t, yearmd_hourms_format)
            #     return time.strftime(influx_format, structure)
            # elif len(t) == 20:
            #     # return time.strptime(t, influx_format)
            #     return t
        #
        # elif type(t) == int:
        #     return time.strftime(influx_format, time.localtime(t / 10 ** 3))

        elif type(t) == float:
            return time.strftime(influx_format, time.localtime(t - 3600 * 8))

        elif type(t) == Timestamp:
            return time.strftime(yearmd_hourms_format, time.localtime(t.value / 10 ** 9))

    @staticmethod
    def now():
        return InfluxTime.utc(time.time())


class SplitTime:
    format = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def split(start, end, interval_day=7):
        start = datetime.strptime(start, SplitTime.format)
        end = datetime.strptime(end, SplitTime.format)
        interval = timedelta(days=interval_day)
        result = []

        while end - start > interval:
            result.append([start, start + interval])
            start += interval

        result.append([start, end])

        for i in range(len(result)):
            for j in [0, 1]:
                result[i][j] = result[i][j].strftime(SplitTime.format)

        return result


if __name__ == '__main__':
    # print(InfluxTime.utc("2022-08-09"))
    # print(InfluxTime.utc("2022-08-09 10:28"))
    print(InfluxTime.utc("2022-08-09 10:28:00"))
    # print(InfluxTime.utc("2022-08-09T10:50:00Z"))
    # print(InfluxTime.utc(time.time()))
    # print(InfluxTime.utc('1660026181729'))
    # print(InfluxTime.utc('1660026181729'))
    #
    print(InfluxTime.now())
    print(time.time())

    # s = SplitTime()

    # print(s.split("2022-08-09 10:27:00", "2022-08-10 10:28:00", interval_day=1))
