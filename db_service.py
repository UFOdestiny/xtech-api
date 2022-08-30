# -*- coding: utf-8 -*-
# @Name     : db_service.py
# @Date     : 2022/8/3 15:10
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB
from influxdb_client import InfluxDBClient, Point
from config import Mysql
from config import InfluxDB116 as InfluxDB
from influxdb_client.client.write_api import SYNCHRONOUS
from utils import InfluxTime
import time
from urllib3 import Retry


class Singleton(type):
    def __call__(cls, *args, **kwargs):
        if hasattr(cls, "_instance"):
            return cls._instance
        cls._instance = type.__call__(cls, *args, **kwargs)
        return cls._instance


class MysqlService(metaclass=Singleton):
    def __init__(self):
        self.pool = PooledDB(creator=pymysql, maxconnections=5, blocking=True,
                             host=Mysql.host,
                             user=Mysql.user,
                             password=Mysql.password,
                             port=Mysql.port,
                             database=Mysql.db)

    def get_data(self, sql):
        conn = self.pool.connection()
        cur = conn.cursor(cursor=DictCursor)
        cur.execute(sql)
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result

    def update_data(self, sql):
        conn = self.pool.connection()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    def get_user(self, username, password):
        sql = f"select username,password from user where username='{username}' and password='{password}'"
        rst = self.get_data(sql)
        return len(rst) == 1

    def get_user_data_num(self, username):
        sql = "select api_level,api_data_num from xtech_center_user where username='{}'".format(username)
        rst = self.get_data(sql)
        return rst[0]

    def update_user_data_num_real(self, user_data_num_real, username):
        sql = "update xtech_center_user set api_data_num={} where username='{}'".format(user_data_num_real, username)
        self.update_data(sql)


class InfluxdbService(metaclass=Singleton):
    def __init__(self, influxdb=InfluxDB):
        self.INFLUX = influxdb
        self.client = InfluxDBClient(url=self.INFLUX.url, token=self.INFLUX.token, org=self.INFLUX.org,
                                     retries=Retry(connect=5, read=2, redirect=5))

        if self.client.ping():
            print("InfluxDB连接成功！")
        else:
            print("InfluxDB连接失败！")

        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

        self.query_api = self.client.query_api()

        self.delete_api = self.client.delete_api()

    def write_data(self, measurement_name, tag_key, tag_value, field_key, field_value, time=''):
        record = Point(measurement_name=measurement_name). \
            tag(key=tag_key, value=tag_value). \
            field(field=field_key, value=field_value)
        if record:
            record.time(time)
        self.write_api.write(bucket=self.INFLUX.bucket, org=self.INFLUX.org, record=record)
        print("写入成功")

    def write_data_execute(self, record):
        self.write_api.write(bucket=self.INFLUX.bucket, org=self.INFLUX.org, record=record)
        print("写入成功")

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

    def process_result(self, tables):
        result = []
        for table in tables:
            for record in table.records:
                result.append(record)
        return result

    def delete_data(self, start, stop):
        self.delete_api.delete(start, stop,
                               f'_measurement="{self.INFLUX.measurement}"',
                               bucket=self.INFLUX.bucket,
                               org=self.INFLUX.org)

    def empty(self):
        self.delete_api.delete(start="1970-01-01T00:00:00Z",
                               stop=InfluxTime.to_influx_time(time.time()),
                               predicate=f'_measurement="{self.INFLUX.measurement}"',
                               bucket=self.INFLUX.bucket,
                               org=self.INFLUX.org)
        print("清空！")


if __name__ == "__main__":
    influxdbService = InfluxdbService(influxdb=InfluxDB)
    influxdbService.empty()
    # mysqlService = MysqlService()
