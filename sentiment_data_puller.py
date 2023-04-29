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
            df = df[["time", "symbol", "Entities", "Gpe", "Locations", "Organizations", "Others", "Sentiment", "title"]]
            return df.to_json(orient='records'), 200
        except Exception as e:
            return {'error': str(e)}, 500

@app.route('/api/v1/update_sentiment_data', methods=['POST'])
def update_sentiment_data():
    if request.method == 'POST':
        try:

            influx_frendly_data = lambda measurement_name, time_value, field_values, tag_values: {
                "measurement": measurement_name,
                "time": time_value,
                "fields": field_values,
                "tags": tag_values
            }

            input_date_value = request.json['date']
            input_symbol_value = request.json['symbol']
            input_sentiment_value = request.json['sentiment']
            input_title_value = request.json['title']

            influx_connector = influx.InfluxConnector()
            df_data = influx_connector.read_from_influx(f"select * from {os.environ.get('FUNDAMENTAL_NEWS_TABLE')} where time = '{input_date_value}' and symbol = '{input_symbol_value}' and title = '{input_title_value}'")

            influx_friendly_data_list = []
            for single_index, single_row in df_data.iterrows():
                influx_friendly_data = influx_frendly_data(
                    os.environ.get('FUNDAMENTAL_NEWS_TABLE'),
                    single_row['time'],
                    {
                        "Entities": single_row['Entities'],
                        "Gpe": single_row['Gpe'],
                        "Locations": single_row['Locations'],
                        "Organizations": single_row['Organizations'],
                        "Others": single_row['Others'],
                        "Sentiment": input_sentiment_value,
                        "title": input_title_value,
                        "Topic_categories_csv": single_row['Topic_categories_csv'],
                        "Topic_probs_csv": single_row['Topic_probs_csv']
                    },
                    {
                        "symbol": single_row['symbol'],
                        "guid": single_row['guid']
                    }
                )
                influx_friendly_data_list.append(influx_friendly_data)
            influx_connector.write_to_influx(influx_friendly_data_list)

            return {'success': 'Sentiment data updated successfully'}, 200
        except Exception as e:
            return {'error': str(e)}, 500

@app.route('/api/v1/get_technical_data', methods=['GET'])
def get_technical_data():
    if request.method == 'GET':
        try:
            influx_connector = influx.InfluxConnector()
            sql_query = f"SELECT * FROM {os.environ.get('TECHNICAL_ANALYSIS_TABLE')}"
            df = influx_connector.read_from_influx(sql_query)
            if(len(df) == 0):
                return {'error': 'No Technical Analysis data found'}, 500
            return df.to_json(orient='records'), 200
        except Exception as e:
            return {'error': str(e)}, 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')