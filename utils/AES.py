# -*- coding: utf-8 -*-
# @Name     : AES.py
# @Date     : 2022/8/30 15:35
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from config import AesConfig
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64


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
    pc = Aes()
    e = pc.encrypt("123")
    d = pc.decrypt(e)
    print(e, d)
