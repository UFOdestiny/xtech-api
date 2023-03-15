# -*- coding: utf-8 -*-
# @Name     : RedisClear.py
# @Date     : 2023/3/13 9:58
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import os
import sys
import redis

root_path = os.path.abspath(__file__)
root_path = '/'.join(root_path.split('/')[:-2])
sys.path.append(root_path)

from config import RedisSetting

conn_list = []
for db in [0, 1]:
    conn_list.append(redis.Redis(host=RedisSetting.host, port=RedisSetting.port, decode_responses=True,
                                 db=db, username=RedisSetting.username, password=RedisSetting.password))

if __name__ == "__main__":
    for conn in conn_list:
        conn.flushdb()
