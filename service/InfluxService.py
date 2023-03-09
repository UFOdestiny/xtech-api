# -*- coding: utf-8 -*-
# @Name     : InfluxService.py
# @Date     : 2022/9/9 12:31
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : Influx服务连接

import datetime
import time

from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
from urllib3 import Retry

from config import InfluxDBProduct as InfluxDB
from utils.InfluxTime import InfluxTime
from utils.Logger import Logger
from utils.Singleton import Singleton


class BatchingCallback:
    """
    InfluxDB batch写入结果的返回类
    """

    def success(self, conf: (str, str, str), data: str):
        """Successfully writen batch."""
        print(f"Written batch: {conf}, data: {len(data)}")

    def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        """Unsuccessfully writen batch."""
        print(f"Cannot write batch: {conf}, data: {len(data)} due: {exception}")

    def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        """Retryable error."""
        print(f"Retryable error occurs for batch: {conf}, data: {len(data)} retry: {exception}")


class InfluxService(metaclass=Singleton):
    def __init__(self, influxdb=InfluxDB):
        self.log = Logger()
        self.INFLUX = influxdb

        self.batch_callback = BatchingCallback()
        self.client = InfluxDBClient(url=self.INFLUX.url, token=self.INFLUX.token, org=self.INFLUX.org, timeout=0,
                                     retries=Retry(connect=5, read=2, redirect=5), debug=False)

        if self.client.ping():
            self.log.info(f"InfluxDB Connected")
        else:
            self.log.error(f"InfluxDB Error")
            raise Exception

        self.option = WriteOptions(batch_size=50_000,
                                   flush_interval=10_000,
                                   jitter_interval=2_000,
                                   retry_interval=1_000,
                                   max_retries=10,
                                   max_retry_delay=30_000,
                                   exponential_base=2)

        self.write_api_syn = self.client.write_api(SYNCHRONOUS)

        self.query_api = self.client.query_api()
        self.delete_api = self.client.delete_api()

    def write_synchronous(self, df, measurement, tag_columns, **kwargs):
        api = self.client.write_api(write_options=SYNCHRONOUS)
        api.write(bucket=self.INFLUX.bucket, org=self.INFLUX.org, record=df,
                  data_frame_measurement_name=measurement,
                  data_frame_tag_columns=tag_columns,
                  **kwargs)
        api.flush()

    def write_batch(self, df, measurement, tag_columns, **kwargs):
        with self.client as client:
            batch_size = min(50000, max(len(df) // 2, 1))

            option = WriteOptions(batch_size=batch_size, flush_interval=10_000, jitter_interval=2_000,
                                  retry_interval=1_000, max_retries=10, max_retry_delay=30_000, exponential_base=2)

            api = client.write_api(write_options=option, success_callback=self.batch_callback.success,
                                   error_callback=self.batch_callback.error,
                                   retry_callback=self.batch_callback.retry)
            with api as a:
                a.write(bucket=self.INFLUX.bucket, org=self.INFLUX.org, record=df,
                        data_frame_measurement_name=measurement, data_frame_tag_columns=tag_columns,
                        **kwargs)

    def write_pandas(self, df, measurement, tag_columns, **kwargs):
        if measurement in ["opcontractquote", "opnominalamount", "putdminuscalld", "opdiscount", "cpr",
                           "optargetderivativevol_1d", "optargetderivativevol_1h", "optargetderivativevol_2h",
                           "optargetderivativeprice_1d", "optargetderivativeprice_1h", "optargetderivativeprice_2h",
                           "optargetderivativeprice_5m", "optargetderivativeprice_15m", "optargetderivativeprice_30m",
                           ]:

            self.write_synchronous(df, measurement, tag_columns, **kwargs)
        else:
            self.write_batch(df, measurement, tag_columns, **kwargs)

        # self.log.info(f"{len(df)} records of {measurement} has been written")
        return f"{len(df)} records of {measurement} written"

    def query_influx(self, start, end, measurement, targetcode=None, opcode=None, df=True, keep=None, filter_=None,
                     unique=None, unzip=False):
        """
        查询InfluxDB
        :param start: 开始时间，北京时间格式
        :param end:
        :param measurement:
        :param targetcode: 标的代码
        :param opcode: 合约代码
        :param df: True返回DataFrame格式，False返回list格式
        :param keep: 保留列
        :param filter_: 自定义过滤条件
        :param unique: 去重列
        :param unzip: 前端格式返回，
        :return:
        """
        start, end = InfluxTime.utc(start, end)

        q = f"""
                from(bucket: "{self.INFLUX.bucket}")
                |> range(
             """
        if start:
            q += f"""start: {start}, """
        if end:
            q += f"""stop: {end}"""
        q += ")"

        if measurement:
            q += f"""|> filter(fn: (r) => r["_measurement"] == "{measurement}")"""

        if targetcode:
            q += f"""|> filter(fn: (r) => r["targetcode"] == "{targetcode}")"""
        if opcode:
            q += f"""|> filter(fn: (r) => r["opcode"] == "{opcode}")"""

        if filter_:
            q += filter_

        q += """
        |> drop(columns: ["_start", "_stop"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
             """

        if unique:
            q += f"""|> unique(column: "{unique}")"""

        if keep:
            q += f"""|> keep(columns: ["""
            for i in keep:
                q += f"\"{i}\","
            q += "])"

        # print(q)

        df_ = self.query_api.query_data_frame(q)
        if len(df_) == 0:
            return None

        df_.drop(["result", "table"], axis=1, inplace=True)

        if "_time" in df_.columns:
            df_["_time"] = df_["_time"].apply(lambda x: x.tz_convert('Asia/Shanghai').strftime("%Y-%m-%d %H:%M:%S"))

        # print(df_)
        if df:
            return df_
        else:
            lst = df_.values.tolist()
            if unzip:
                return list(zip(*lst))
            else:
                return lst

    def delete_data(self, start, stop, measurement=None, ):
        """
        以天为单位，删除数据
        :param start:
        :param stop:
        :param measurement:
        :return:
        """
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

    def empty(self, measurement):
        """
        清空表
        :param measurement:
        :return:
        """
        self.delete_api.delete(start="1970-01-01T00:00:00Z",
                               stop=InfluxTime.utc(time.time()),
                               predicate=f'_measurement="{measurement}"',
                               bucket=self.INFLUX.bucket,
                               org=self.INFLUX.org)
        print("clear!")


if __name__ == "__main__":
    influxdbService = InfluxService()
    # print(f"{time.time() * 1000 * 1000 * 1000:.0f}")
    # q = ['test1,targetcode=510050.XSHG price=2.76,pct=2.754 1662706943248528896']
    # influxdbService.write_batch(q)

    # mysqlService = MysqlService()
    # influxdbService.client.drop_database("test_hello_world")

    influxdbService.delete_data("2023-03-01T00:00:00Z", "2023-03-07T00:00:00Z", "opnominalamount")
    # influxdbService.empty("opnominalamount")
    # influxdbService.delete_data("2020-01-01T00:00:00Z", "2023-02-16T00:00:00Z", "opcontractinfo")
    # q = [
    #     'test1,targetcode=510050.XSHG price=2.76,pct=2.754 1673956372162814720',
    # ]
    # influxdbService.write_data("test", "a", 1, "b", 2, "2023-01-16T09:30:05.000Z")
    # influxdbService.query_data()

    # df = influxdbService.query_influx("2023-02-01 00:00:00", "2023-02-14 00:00:00", "opcontractquote",
    #                                   "510050.XSHG", "10004405.XSHG")
    # print(df)
    #
    # print(df.columns)
