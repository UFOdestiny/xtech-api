# -*- coding: utf-8 -*-
# @Name     : InfluxService.py
# @Date     : 2022/9/9 12:31
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : Influx服务连接

import datetime, time

from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from urllib3 import Retry

from config import InfluxDBProduct as InfluxDB
from utils.InfluxTime import InfluxTime
from utils.Singleton import Singleton

from utils.Logger import Logger


class InfluxdbService(metaclass=Singleton):
    def __init__(self, influxdb=InfluxDB):
        self.log = Logger(path="../logger")
        self.INFLUX = influxdb
        self.client = InfluxDBClient(url=self.INFLUX.url, token=self.INFLUX.token, org=self.INFLUX.org,
                                     retries=Retry(connect=5, read=2, redirect=5))

        if self.client.ping():
            print("InfluxDB ok!")
        else:
            print("InfluxDB fail!")

        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        """
        WriteOptions(batch_size=500,
                                                                          flush_interval=10_000,
                                                                          jitter_interval=2_000,
                                                                          retry_interval=5_000,
                                                                          max_retries=5,
                                                                          max_retry_delay=30_000,
                                                                          exponential_base=2),
        """

        self.query_api = self.client.query_api()

        self.delete_api = self.client.delete_api()

    def write_data(self, measurement_name, tag_key, tag_value, field_key, field_value, time=''):
        record = Point(measurement_name=measurement_name). \
            tag(key=tag_key, value=tag_value). \
            field(field=field_key, value=field_value)
        if record:
            record.time(time)
        self.write_api.write(bucket=self.INFLUX.bucket, org=self.INFLUX.org, record=record)
        self.log.info("write ok")

    def write_data_execute(self, record):
        self.write_api.write(bucket=self.INFLUX.bucket, org=self.INFLUX.org, record=record)
        self.log.info(f"write ok {len(record)}")

    def query_data(self, start="-1h", stop='', filters=''):
        source = f"from(bucket:\"{self.INFLUX.bucket}\")"

        pipe_forward = "|>"

        time_range = f"range(start: {start})"
        stop_range = f", stop: {stop})"
        if stop:
            time_range = time_range.replace(")", stop_range)

        query = source + pipe_forward + time_range

        if filters:
            filter_skeleton = f"filter(fn: (r) => {filters})"
            query = query + pipe_forward + filter_skeleton

        print(query)
        tables = self.query_api.query(query, org=self.INFLUX.org)

        return self.process_result(tables)

    def query_data_raw(self, raw_query):

        print(raw_query)
        tables = self.query_api.query(raw_query, org=self.INFLUX.org)

        return self.process_result(tables)

    def process_result(self, tables):
        result = []
        for table in tables:
            for record in table.records:
                result.append(record)
        return result

    def delete_data(self, start, stop, measurement=None, ):
        if not measurement:
            measurement = self.INFLUX.measurement

        start_ = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
        stop_ = datetime.datetime.strptime(stop, "%Y-%m-%dT%H:%M:%SZ")
        time_interval = []
        while start_ + datetime.timedelta(days=1) < stop_:
            time_interval.append([start_, start_ + datetime.timedelta(days=1)])
            start_ = start_ + datetime.timedelta(days=1)
        time_interval.append([start_, stop_])
        for i in range(len(time_interval)):
            time_interval[i][0] = datetime.datetime.strftime(time_interval[i][0], "%Y-%m-%dT%H:%M:%SZ")
            time_interval[i][1] = datetime.datetime.strftime(time_interval[i][1], "%Y-%m-%dT%H:%M:%SZ")

        for i in time_interval:
            print(i)
            self.delete_api.delete(i[0], i[1],
                                   f'_measurement="{measurement}"',
                                   bucket=self.INFLUX.bucket,
                                   org=self.INFLUX.org)
        # self.delete_api.delete(start, stop,
        #                        f'_measurement="{measurement}"',
        #                        bucket=self.INFLUX.bucket,
        #                        org=self.INFLUX.org)

    def empty(self, measurement):
        self.delete_api.delete(start="1970-01-01T00:00:00Z",
                               stop=InfluxTime.to_influx_time(time.time()),
                               # predicate=f'_measurement="{self.INFLUX.measurement}"',
                               predicate=f'_measurement="{measurement}"',
                               bucket=self.INFLUX.bucket,
                               org=self.INFLUX.org)
        print("clear!")


if __name__ == "__main__":
    influxdbService = InfluxdbService()
    # print(f"{time.time() * 1000 * 1000 * 1000:.0f}")
    # q = ['test1,targetcode=510050.XSHG price=2.76,pct=2.754 1662706943248528896']
    # influxdbService.write_data_execute(q)
    # influxdbService.empty("optargetquote")
    # mysqlService = MysqlService()
    # influxdbService.delete_data("2022-11-01T00:00:00Z", "2022-11-30T23:00:00Z", "opcontractquote")
    q = [
        'test1,targetcode=510050.XSHG price=2.76,pct=2.754 1673956372162814720',
    ]
    influxdbService.write_data("test", "a", 1, "b", 2, "2023-01-16T09:30:05.000Z")
    influxdbService.query_data()
