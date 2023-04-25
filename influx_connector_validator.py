import module.InfluxConnector as influx
import os

# Initialize InfluxConnector
influx_connector = influx.InfluxConnector()

# Call read_from_influx method
query = f'select * from {os.environ.get("Fundamental_News_Table")}'
result = influx_connector.read_from_influx(query)
print(result)
