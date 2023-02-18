# -*- coding: utf-8 -*-
# @Name     : InfluxTime.py
# @Date     : 2022/8/30 15:37
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : influxDB时间格式处理
import time
from datetime import datetime, timedelta


class InfluxTime:
    influx_format = "%Y-%m-%dT%H:%M:%SZ"
    yearmd_format = '%Y-%m-%d'
    yearmd_hourm_format = "%Y-%m-%d %H:%M"
    yearmd_hourms_format = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def utc(*args, timestamp_=False):
        res = []
        for t in args:
            if not t:
                res.append(None)
                continue
            if type(t) == str:
                if t.isnumeric():
                    timestamp = int(t) / 10e4
                else:
                    if len(t) == 10:
                        timestamp = time.mktime(time.strptime(t, InfluxTime.yearmd_format))
                    else:
                        timestamp = time.mktime(time.strptime(t, InfluxTime.yearmd_hourms_format))
            else:
                timestamp = t

            if timestamp_:
                res.append(timestamp)
                continue
            # timestamp -= 3600 * 8 localtime
            structure = time.gmtime(timestamp)
            string_ = time.strftime(InfluxTime.influx_format, structure)
            res.append(string_)

        if len(args) == 1:
            return res[0]
        else:
            return res

    @staticmethod
    def utcnow():
        return InfluxTime.utc(time.time())

    @staticmethod
    def utc8now():
        timestamp = time.time()
        structure = time.localtime(timestamp)
        string_ = time.strftime(InfluxTime.yearmd_hourms_format, structure)
        return string_


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
    # print(InfluxTime.utc("2022-08-09 10:28:00", "2022-08-10 10:28:00"))
    # print(InfluxTime.utc("2022-08-09T10:50:00Z"))
    # print(InfluxTime.utc(time.time()))
    # print(InfluxTime.utc('1660026181729'))
    # print(InfluxTime.utc('1660026181729'))
    #
    # print(InfluxTime.utcnow())
    # print(time.time())
    # print(InfluxTime.utc("1676450847655"))

    # s = SplitTime()

    # print(s.split("2022-08-09 10:27:00", "2022-08-10 10:28:00", interval_day=1))
    # g = ['2023-02-17', '2023-02-17', '2023-02-17']

    # print(InfluxTime.utc(*g, timestamp_=True))

    # InfluxTime.utc("1676563200.0")
    print(time.localtime(1676563200))
