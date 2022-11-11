# -*- coding: utf-8 -*-
# @Name     : MysqlService.py
# @Date     : 2022/9/9 12:31
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : Mysql服务连接

import pymysql
from dbutils.pooled_db import PooledDB
from pymysql.cursors import DictCursor
from utils.Singleton import Singleton
from config import Mysql


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
