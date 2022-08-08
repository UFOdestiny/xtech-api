# -*- coding: utf-8 -*-
# @Name     : influx.py
# @Date     : 2022/8/3 15:02
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     : manipulate influxDB

from influxdb_client import InfluxDBClient, Point
from config import INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_TOKEN

client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG,
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
