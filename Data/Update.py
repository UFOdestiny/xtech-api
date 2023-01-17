# -*- coding: utf-8 -*-
# @Name     : Update.py
# @Date     : 2023/1/9 18:38
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from utils.Logger import Logger


class Update:
    def __init__(self):
        self.logger = Logger(path="logger")

    def run(self):
        self.logger.info("数据更新")
