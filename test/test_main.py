import unittest
from unittest.mock import patch, Mock
import urllib.request
import subprocess
import os
from main import financial_strength_retreival, fundamental_analysis, technical_analysis


class MainTestCase(unittest.TestCase):
    influx_frendly_data = lambda measurement_name, time_value, field_values, tag_values: {
        "measurement": measurement_name,
        "time": time_value,
        "fields": field_values,
        "tags": tag_values
    }

    @patch('requests.get')
    def test_financial_strength_retreival(self, mock_get):
        os.environ['Fin_Stren_Provider'] = ''
        os.environ['Fin_BalSheet_Function'] = ''
        os.environ['Fin_BalSheet_Provider'] = ''
        os.environ['Fin_Stren_Table'] = ''

        mock_data1 = {}
        mock_data2 = {}

        mock_response1 = Mock()
        mock_response1.content = mock_data1
        mock_response1.status_code = 200

        mock_response2 = Mock()
        mock_response2.content = mock_data2
        mock_response2.status_code = 200
        mock_get.side_effect = [mock_response1, mock_response2]

        expected_response = {}
        financial_strength_results = financial_strength_retreival('Test_Symbol', self.influx_frendly_data, 'api_key',
                                                                  'function', 'current_date')
        self.assertEquals(financial_strength_results, expected_response)

    @patch('requests.get')
    def test_fundamental_analysis(self, mock_get):
        os.environ['Fundamental_News_Provider'] = ''
        os.environ['Fundamental_News_Key'] = ''
        os.environ['Fundamental_News_Host'] = ''
        os.environ['Fundamental_News_Table'] = ''

        mock_data = {}

        mock_response = Mock()
        mock_response.content = mock_data
        mock_response.status_code = 200

        mock_get.side_effect = [mock_response]

        expected_response = {}
        fundamental_analysis_val = fundamental_analysis('Test_Symbol', 'current_date', self.influx_frendly_data)

        self.assertEquals(fundamental_analysis_val, expected_response)

    @patch('requests.get')
    def test_technical_analysis(self, mock_get):
        os.environ['Technical_Analysis_Function'] = ''
        os.environ['Technical_Analysis_Provider'] = ''
        os.environ['Technical_Analysis_Table'] = ''

        mock_data = {}

        mock_response = Mock()
        mock_response.content = mock_data
        mock_response.status_code = 200

        mock_get.side_effect = [mock_response]

        expected_response = {}
        tech_analysis_results = technical_analysis('Test_Symbol', self.influx_frendly_data, 'api_key')
        self.assertEquals(tech_analysis_results, expected_response)
