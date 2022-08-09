# -*- coding: utf-8 -*-
# @Name     : influx.py
# @Date     : 2022/8/3 15:02
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : manipulate influxDB

from db_service import InfluxdbService
from config import InfluxDBLocal as InfluxDB
import time
from utils import InfluxTime
import random

influxdbService = InfluxdbService(influxdb=InfluxDB)


# influxdbService.write_data("test1", "location", "qingdao", "temperature", 54)


# tables = influxdbService.query_data()

class GenerateTestData:
    def __init__(self):
        self.now = time.time_ns()
        self.timestamp_series = [i + self.now for i in range(-1000000000000, 0, 1000000000)]
        # self.time_series = list(map(InfluxTime.to_influx_time, self.timestamp_series))

    def generate(self):
        sequence = [
            f"test1,code=a price={random.random() * 100} {self.timestamp_series[i]}" for
            i in range(1000)]
        return sequence

    def submit(self):
        influxdbService.write_data_execute(self.generate())


class GetTestData:
    def __init__(self):
        self.start = "-10d"

    def get(self):
        return influxdbService.query_data(start=self.start)

    def __call__(self, *args, **kwargs):
        return [i.get_value() for i in self.get()]


if __name__ == '__main__':
    # generate = GenerateTestData()
    # generate.submit()

    get = GetTestData()
    data = get()
