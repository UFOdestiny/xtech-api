# -*- coding: utf-8 -*-
# @Name     : response_service.py
# @Date     : 2022/8/5 9:26
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

from functools import wraps
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder


class ResponseService:
    def __init__(self):
        self.success_code = 200
        self.param_error_code = 400
        self.data_traffic_code = 400
        self.verify_error_code = 401
        self.request_method_error_code = 405
        self.service_error_code = 500

    def return_success(self, data, num):
        success_return = {
            'header':
                {
                    'code': self.success_code,
                    'msg': 'success'
                },
            'body':
                {
                    'data': data,
                    'number': num
                }
        }
        return success_return

    def return_param_error(self, msg):
        param_error_return = {
            'header':
                {
                    'code': self.param_error_code,
                    'msg': msg
                },
            'body':
                {
                    'data': []
                }
        }
        return param_error_return

    def return_service_error(self):
        service_error_return = {
            'header':
                {
                    'code': self.service_error_code,
                    'msg': '服务器异常，请联系工作人员！'
                },
            'body':
                {
                    'data': []
                }
        }
        return service_error_return

    def return_no_data(self):
        service_no_data_return = {
            'header':
                {
                    'code': self.success_code,
                    'msg': '请求成功，暂无数据。请切换不同条件重新请求！'
                },
            'body':
                {
                    'data': [],
                    'number': 0
                }
        }
        return service_no_data_return

    def return_verify_error(self, msg):
        verify_error_return = {
            'header':
                {
                    'code': self.verify_error_code,
                    'msg': msg
                },
            'body':
                {
                    'data': []
                }
        }
        return verify_error_return

    def return_data_traffic(self, data_num, available_data_num):
        verify_error_return = {
            'header':
                {
                    'code': self.data_traffic_code,
                    'msg': '请求数据量超过今日可使用流量，如需增加流量请联系工作人员！'
                },
            'body':
                {
                    '请求返回数据数量': data_num,
                    '今日可用剩余流量': available_data_num
                }
        }
        return verify_error_return

    def return_request_method_error(self, msg):
        verify_error_return = {
            'header':
                {
                    'code': self.request_method_error_code,
                    'msg': msg
                },
            'body':
                {
                    'data': []
                }
        }
        return verify_error_return

    def return_request(self, request_return):
        request_success_return = {
            "code": request_return['header']['code'],
            "message": request_return['header']['msg'],
            "data": request_return['body']['data']
        }

        return request_success_return


def check_exception(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        response_service = ResponseService()
        try:
            rst = await func(*args, **kwargs)
        except Exception as e:
            print(str(e))
            if '400' in repr(e):
                rst = response_service.return_param_error(str(e))
            else:
                rst = response_service.return_service_error()
        return JSONResponse(content=jsonable_encoder(rst))

    return wrapper


def process_return(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        response_service = ResponseService()
        rst = func(*args, **kwargs)
        return response_service.return_request(rst)

    return wrapper
