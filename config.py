# -*- coding: utf-8 -*-
# @Name     : config.py
# @Date     : 2022/8/3 15:05
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : config file

import json
import numpy as np
from decimal import Decimal

# 开发环境
MYSQL_HOST = "175.25.50.117"
MYSQL_PORT = 12845
MYSQL_DB = "xtech_dev"
MYSQL_USER = "root"
MYSQL_PASSWORD = "xtech111"

# 测试环境
# MYSQL_HOST = "175.25.50.117"
# MYSQL_PORT = 12845
# MYSQL_DB = "xtech_unit"
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "xtech111"


# 生产环境
# MYSQL_HOST = "8.140.125.134"
# MYSQL_PORT = 3306
# MYSQL_DB = "xtech"
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "xtech2021!"

INFLUXDB_HOST = "175.25.50.117"
INFLUXDB_PORT = 8086
INFLUXDB_USER = "xtechinflux"
INFLUXDB_PWD = "xtech1234"
INFLUXDB_ALT_DB = "alternative"
INFLUXDB_FAC_DB = "factor"


class MyJSONEncoder(json.JSONEncoder):
    """
    重构JSONEncoder中的default方法
    """

    def default(self, obj):
        if isinstance(obj, Decimal):
            # Convert decimal instances to strings.
            return str(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)
