# -*- coding: utf-8 -*-
# @Name     : influx.py
# @Date     : 2022/8/3 15:02
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : manipulate influxDB

import random
import time

from config import InfluxDB116 as InfluxDB
from db_service import InfluxdbService

influxdbService = InfluxdbService(influxdb=InfluxDB)


# influxdbService.write_data("test1", "location", "qingdao", "temperature", 54)


# tables = influxdbService.query_data()

class GenerateTestData:
    def __init__(self):
        self.oneday = int(8.64e13)
        self.now = time.time_ns()
        self.timestamp_series = [i + self.now for i in range(-self.oneday * 50, 0, self.oneday)]
        # self.time_series = list(map(InfluxTime.to_influx_time, self.timestamp_series))

    def generate(self):
        sequence = [
            f"test1,code=a price={random.random() * 100} {self.timestamp_series[i]}" for
            i in range(len(self.timestamp_series))]
        return sequence

    def submit(self):
        influxdbService.write_data_execute(self.generate())


class GetTestData:
    def __init__(self):
        self.start = "-10d"

    def get(self):
        return influxdbService.query_data(start=self.start)

    def __call__(self, *args, **kwargs):
        return self.get()


if __name__ == '__main__':
    generate = GenerateTestData()
    generate.submit()

    # get = GetTestData()
    # data = get()
