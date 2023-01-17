# -*- coding: utf-8 -*-
# @Name     : WriteData.py
# @Date     : 2022/9/9 11:33
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : post传输
import json

import requests


class PostData:
    username = "ydh"
    password = "123"
    header = {"username": username, "password": password}

    def __init__(self):
        self.data = {"header": self.header, "body": None}

    def generate(self, data):
        self.data["body"] = data

    def send(self, data):
        self.generate(data)
        requests.post(url="http://127.0.0.1:8000/data/query", data={
            "name": "optargetquote",
            "time": [
                "2023-01-09 00:00:00", "2023-01-10 00:00:00"
            ],
            "targetcode": "000016.XSHE",
            "opcode": ""
        })


if __name__ == "__main__":
    js = {
        "name": "optargetquote",
        "time": [
            "2023-01-09 00:00:00", "2023-01-10 00:00:00"
        ],
        "targetcode": "000016.XSHE",
        "opcode": ""
    }
    d = json.dumps(js)

    res = requests.post(url="http://127.0.0.1:8000/data/query", data=d)
    print(res.json())
