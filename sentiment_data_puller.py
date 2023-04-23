from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from module.db_connector import insert_into_sqlite, read_from_sqlite, update_delete_sqlite
import os
import pandas as pd

app = Flask(__name__)
api = Api(app)

@app.route('/api/v1/get_sentiment_daa', methods=['GET'])
def get_sentiment_daa():
    if request.method == 'GET':
        try:
            sql_query = f"SELECT date_value,Title,Source,symbol,guid,sentiment FROM {os.environ.get('Fundamental_News_Table')}"
            df = read_from_sqlite(sql_query)
            return df.to_json(orient='records'), 200
        except Exception as e:
            return {'error': str(e)}, 500

@app.route('/api/v1/update_sentiment_daa', methods=['POST'])
def update_sentiment_daa():
    if request.method == 'POST':
        try:
            data = request.get_json()
            sql_query = f"UPDATE {os.environ.get('Fundamental_News_Table')} SET sentiment = ? WHERE date_value = ? AND symbol = ? AND guid = ? AND Title = ? AND Source = ?"
            update_delete_sqlite(sql_query, data)
            return {'message': 'success'}, 200
        except Exception as e:
            return {'error': str(e)}, 500

@app.route('/api/v1/bulk_insert_sentiment_daa', methods=['POST'])
def bulk_insert_sentiment_daa():
    if request.method == 'POST':
        try:
            # Get dataframe json from request body, convert to dataframe and insert into sqlite
            data = request.get_json()
            df = pd.read_json(data)
            insert_into_sqlite(df, os.environ.get('Fundamental_News_Table'))
            return {'message': 'success'}, 200
        except Exception as e:
            return {'error': str(e)}, 500