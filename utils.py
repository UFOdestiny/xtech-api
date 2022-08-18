# -*- coding: utf-8 -*-
# @Name     : utils.py
# @Date     : 2022/8/9 10:33
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import time
from Crypto.Cipher import AES
from config import AesConfig
from Crypto.Util.Padding import pad, unpad
import base64
from pydantic import BaseModel


class UserLogin(BaseModel):
    username: str
    password: str


class TimeRange(BaseModel):
    start: str
    stop: str


class Day(BaseModel):
    day: str


class InfluxTime:
    @staticmethod
    def to_influx_time(t):
        influx_format = "%Y-%m-%dT%H:%M:%SZ"
        yearmd_format = '%Y-%m-%d'
        yearmd_hourm_format = "%Y-%m-%d %H:%M"

        if type(t) == str:
            if t.isnumeric():
                if len(t) >= 12:
                    return time.strftime(influx_format, time.localtime(int(t) / 1000 - 3600 * 8))
            if len(t) == 10:  # 2022-08-09
                structure = time.strptime(t, yearmd_format)
                return time.strftime(influx_format, structure)
            if len(t) == 16:
                structure = time.strptime(t, yearmd_hourm_format)
                return time.strftime(influx_format, structure)
            if len(t) == 20:
                # return time.strptime(t, influx_format)
                return t

        if type(t) == int:
            return time.strftime(influx_format, time.localtime(t / 1000))

        if type(t) == float:
            return time.strftime(influx_format, time.localtime(t))


class Aes:
    def __init__(self, key=AesConfig.DEFAULT_KEY, iv=AesConfig.DEFAULT_IV):
        self.key = key.encode('utf-8')
        self.iv = iv.encode('utf-8')
        self.mode = AES.MODE_CBC

    # 加密函数，如果text不是16的倍数【加密文本text必须为16的倍数！】，那就补足为16的倍数
    def encrypt(self, text):
        if type(text) == str:
            text = text.encode('utf-8')

        cryptor = AES.new(self.key, self.mode, self.iv)
        # 这里密钥key 长度必须为16（AES-128）、24（AES-192）、或32（AES-256）Bytes 长度.目前AES-128足够用
        pad_text = pad(text, 16, 'pkcs7')
        ciphertext = cryptor.encrypt(pad_text)
        return base64.b64encode(ciphertext).decode('utf-8')

    # 解密后，去掉补足的空格用strip() 去掉
    def decrypt(self, text):
        if type(text) == str:
            text = text.encode('utf-8')
        cryptor = AES.new(self.key, self.mode, self.iv)
        decode_text = base64.b64decode(text)
        plain_text = cryptor.decrypt(decode_text)
        unpad_text = unpad(plain_text, 16, 'pkcs7')
        return unpad_text.rstrip().rstrip(b"\x10").decode('utf-8')


if __name__ == '__main__':
    print(InfluxTime.to_influx_time("2022-08-09"))
    print(InfluxTime.to_influx_time("2022-08-09 10:28"))
    print(InfluxTime.to_influx_time("2022-08-09T10:50:00Z"))
    print(InfluxTime.to_influx_time(time.time()))
    print(InfluxTime.to_influx_time('1660026181729'))
    print(InfluxTime.to_influx_time('1660026181729'))

    pc = Aes()
    e = pc.encrypt("123")
    d = pc.decrypt(e)
    print(e, d)
