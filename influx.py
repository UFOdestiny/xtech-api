# -*- coding: utf-8 -*-
# @Name     : influx.py
# @Date     : 2022/8/3 15:02
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : manipulate influxDB

from influxdb import InfluxDBClient

connection = InfluxDBClient(
    host='175.25.50.117',
    port='8086',
    username='xtechinflux',
    password='xtech1234',
    database='xtech'
)

connection.ping()

if __name__ == '__main__':
    print('Hello World!')
