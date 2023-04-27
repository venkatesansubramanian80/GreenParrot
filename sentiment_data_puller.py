from flask import Flask, request, jsonify
from flask_restful import Resource, Api
import module.InfluxConnector as influx
import os

app = Flask(__name__)
api = Api(app)

@app.route('/', methods=['GET'])
def index():
    return 'Hello World!', 200

@app.route('/api/v1/get_sentiment_daa', methods=['GET'])
def get_sentiment_daa():
    if request.method == 'GET':
        try:
            influx_connector = influx.InfluxConnector()
            sql_query = f"SELECT * FROM {os.environ.get('FUNDAMENTAL_NEWS_TABLE')}"
            print(sql_query)
            df = influx_connector.read_from_influx(sql_query)
            return df.to_json(orient='records'), 200
        except Exception as e:
            return {'error': str(e)}, 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')