from flask import Flask
from flask_restful import Api
from technical_analysis_resource import technical_analysis_resource
from fundamental_analysis_resource import fundamental_analysis_resource


app = Flask(__name__)
api = Api(app)

api.add_resource(technical_analysis_resource, '/technical_analysis')
api.add_resource(fundamental_analysis_resource, '/fundamental_analysis')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')