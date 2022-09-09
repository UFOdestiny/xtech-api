# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/9/9 11:33
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from service.InfluxService import InfluxdbService
import requests


class WriteData:
    username = "ydh"
    password = "123"
    header = {"username": username, "password": password}

    def __init__(self):
        self.data = {"header": self.header, "body": None}
        self.db = InfluxdbService()

    def generate(self, data):
        self.data["body"] = data

    def send(self, data):
        self.generate(data)
        requests.post(url="", data=data)
