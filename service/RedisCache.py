# -*- coding: utf-8 -*-
# @Name     : RedisCache.py
# @Date     : 2023/3/10 17:20
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import os
import sys
import json
import pickle
import zlib
from datetime import timedelta
from redis import StrictRedis

root_path = os.path.abspath(__file__)
root_path = '/'.join(root_path.split('/')[:-2])
sys.path.append(root_path)

from config import RedisSetting
from utils.Singleton import Singleton


class RedisCache(RedisSetting, metaclass=Singleton):
    """ RedisCache helps store urls and their responses to Redis
        Initialization components:
            client: a Redis client connected to the key-value database for
                the webcrawling cache (if not set, a localhost:6379
                default connection is used).
            expires (datetime.timedelta): timedelta when content will expire
                (default: 30 days ago)
            encoding (str): character encoding for serialization
            compress (bool): boolean indicating whether compression with zlib should be used
    """

    def __init__(self, db):
        self.client = StrictRedis(host=self.host, port=self.port, db=db,
                                  username=self.username, password=self.password)
        self.expires = timedelta(hours=12)
        self.encoding = 'utf-8'
        self.compress = True

    def __getitem__(self, url):
        """Load static from Redis for given URL"""
        record = self.client.get(url)
        if record:
            if self.compress:
                record = zlib.decompress(record)
            try:
                return json.loads(record.decode(self.encoding))
            except UnicodeDecodeError:
                return pickle.loads(record)
        else:
            raise KeyError(url + ' does not exist')

    def __setitem__(self, url, result):
        """Save static to Redis for given url"""
        try:
            dump = json.dumps(result)
            data = bytes(dump, self.encoding)
        except TypeError:
            data = pickle.dumps(result)

        if self.compress:
            data = zlib.compress(data)
        self.client.setex(url, self.expires, data)

    def __contains__(self, item):
        return self.client.exists(item)

    def clear(self):
        self.client.flushdb()


if __name__ == "__main__":
    r = RedisCache(db=0)
    r.clear()
    # r["url"] = "test"
    print(r["2023-03-16510050.XSHG"])
