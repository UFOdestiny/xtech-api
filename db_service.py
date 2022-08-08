# -*- coding: utf-8 -*-
# @Name     : db_service.py
# @Date     : 2022/8/3 15:10
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import pandas as pd
import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB
from influxdb_client import InfluxDBClient
from config import Mysql
from config import InfluxDB_117


class MysqlService:
    def __init__(self):
        self.pool = PooledDB(creator=pymysql, maxconnections=5, blocking=True, host=Mysql.host, user=Mysql.user,
                             password=Mysql.password, port=Mysql.port, database=Mysql.db)

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

    def get_user(self, username):
        sql = "select username,password,api_token,api_token_time from xtech_center_user where username='{}'".format(
            username)
        rst = self.get_data(sql)
        return rst[0]

    def get_user_data_num(self, username):
        sql = "select api_level,api_data_num from xtech_center_user where username='{}'".format(username)
        rst = self.get_data(sql)
        return rst[0]

    def update_user_data_num_real(self, user_data_num_real, username):
        sql = "update xtech_center_user set api_data_num={} where username='{}'".format(user_data_num_real, username)
        self.update_data(sql)


class InfluxdbService:
    def __init__(self):
        self.fac_client = InfluxDBClient(url=InfluxDB_117.url, token=InfluxDB_117.token, org=InfluxDB_117.org)

    def get_influxdb_fac_data(self, sql):
        InfluxDB_fac = self.fac_client
        result = InfluxDB_fac.query(sql)
        df = pd.DataFrame(list(result.get_points()))

        return df
