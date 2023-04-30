from flask import Blueprint, request, jsonify
from module.InfluxConnector import InfluxConnector
import os
import requests

technical_analysis = Blueprint('technical_analysis', __name__)

@technical_analysis.route('/api/v1/get_technical_analysis_data', methods=['GET'])
def get_technical_analysis_data():
    if request.method == 'GET':
        try:
            influx_connector = InfluxConnector()
            sql_query = f"SELECT * FROM {os.environ.get('TECHNICAL_ANALYSIS_TABLE')}"
            print(sql_query)
            df = influx_connector.read_from_influx(sql_query)
            df = df[["time", "symbol", "close", "high", "low", "open", "volume"]]
            return df.to_json(orient='records'), 200
        except Exception as e:
            return {'error': str(e)}, 500