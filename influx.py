# -*- coding: utf-8 -*-
# @Name     : influx.py
# @Date     : 2022/8/3 15:02
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : manipulate influxDB

from db_service import InfluxdbService
from config import InfluxDBLocal as InfluxDB

influxdbService = InfluxdbService(influxdb=InfluxDB)
# influxdbService.write_data("test1", "location", "qingdao", "temperature", 54)
tables = influxdbService.query_data()

if __name__ == '__main__':
    pass
