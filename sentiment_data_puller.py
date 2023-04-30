from flask import Flask, request, jsonify
from flask_restful import Resource, Api
import module.InfluxConnector as influx
import os
from technical_analysis import technical_analysis
from fundamental_analysis import fundamental_analysis

app = Flask(__name__)

app.register_blueprint(technical_analysis)
app.register_blueprint(fundamental_analysis)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)