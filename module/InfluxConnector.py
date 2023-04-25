import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import requests
import json

class InfluxConnector(object):
    def __int__(self):
        self.token_val = os.environ.get('Influx_Token_Value')

    def write_to_influx(self, full_list):
        self.client = influxdb_client.InfluxDBClient(url=os.environ.get('Influx_DB_URL'), token=self.token_val,
                                                     org=os.environ.get('Influx_Org_Name'), verify_ssl=False)
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        batch_size = 1000
        for i in range(0, len(full_list), batch_size):
            write_api.write(bucket=os.environ.get('Influx_Bucket_Name'), org=os.environ.get('Influx_Org_Name'), record=full_list[i:i+batch_size])
        self.client.close()

    def read_from_influx(self, query):
        self.client = influxdb_client.InfluxDBClient(url=os.environ.get('Influx_DB_URL'), token=self.token_val,
                                                     org=os.environ.get('Influx_Org_Name'), verify_ssl=False)
        query_api = self.client.query_api()
        result = query_api.query(query)
        self.client.close()
        return result