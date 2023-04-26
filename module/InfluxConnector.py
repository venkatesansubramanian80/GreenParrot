import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from flightsql import FlightSQLClient

import os

class InfluxConnector(object):
    def __int__(self):
        self.token_val = os.environ.get('INFLUX_TOKEN_VALUE')

    def write_to_influx(self, full_list):
        self.token_val = os.environ.get('INFLUX_TOKEN_VALUE')
        self.client = influxdb_client.InfluxDBClient(url=os.environ.get('INFLUX_DB_URL'), token=self.token_val,
                                                org=os.environ.get('INFLUX_ORG_NAME'), verify_ssl=False)
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        batch_size = 1000
        for i in range(0, len(full_list), batch_size):
            write_api.write(bucket=os.environ.get('INFLUX_BUCKET_NAME'), org=os.environ.get('INFLUX_ORG_NAME'), record=full_list[i:i+batch_size])
        self.client.close()

    def read_from_influx(self, query):
        self.token_val = os.environ.get('INFLUX_TOKEN_VALUE')
        self.client = FlightSQLClient(
            host=os.environ.get('INFLUX_HOST_NAME'),
            token=self.token_val,
            metadata={"bucket-name": os.environ.get('INFLUX_BUCKET_NAME')})
        info = self.client.execute(query)
        reader = self.client.do_get(info.endpoints[0].ticket)

        data = reader.read_all()
        df = data.to_pandas()
        return df
