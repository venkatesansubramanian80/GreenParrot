from flask_restful import Resource
from module.InfluxConnector import InfluxConnector
import os

class technical_analysis_resource(Resource):
    def get(self):
        try:
            influx_connector = InfluxConnector()
            sql_query = f"SELECT * FROM {os.environ.get('TECHNICAL_ANALYSIS_TABLE')}"
            print(sql_query)
            df = influx_connector.read_from_influx(sql_query)
            df = df[["time", "symbol", "close", "high", "low", "open", "volume"]]
            return df.to_json(orient='records'), 200
        except Exception as e:
            return {'error': str(e)}, 500