# -*- coding: utf-8 -*-
# @Name     : influx.py
# @Date     : 2022/8/3 15:02
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : manipulate influxDB

"""
ID: 09cad76767b96000
Description	Token:
4p9Te6UFjL1slmPxyDehrqWcQuZpCbEPxTP1sHC4QsKIew_qowxW3GQw5wBKqJyt3UbSx6e-bjSnzNNwCn2jnQ== (all-access)
T7FGylDVQRdD76SNbm2YCcbZZ_VnL-BYOPToMGTxUFOk1tRrxJLooDMCq6j_VZGumtspMze3g05_WhN8sccOkA== (operator)
User Name: xtech
User ID: 09cac465267c4000
"""
from influxdb_client import InfluxDBClient, Point

bucket = "xtech"
token = "a5BtBqQptxJeRFdlnWPyybLDTF9HASLvz0gtY0beZ6s0n2os6HWnBf_9YgJl6TXFFqCafHBkB3NovSiFXzNNyA=="
org = "xtech"
#175.25.50.116
client = InfluxDBClient(
    url="http://127.0.0.1:8080",
    token=token,
    org=org,
    verify_ssl=False,
    timeout=6000,
)

# query_api = client.query_api()

# p = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)

# query = ' from(bucket:"my-bucket")\
# |> range(start: -10m)\
# |> filter(fn:(r) => r._measurement == "my_measurement")\
# |> filter(fn: (r) => r.location == "Prague")\
# |> filter(fn:(r) => r._field == "temperature" ) '

# result = query_api.query(org=org, query=query)
# results = []
# for table in result:
#     for record in table.records:
#         results.append((record.get_field(), record.get_value()))

# print(results)

if __name__ == '__main__':
    print(client.ping())