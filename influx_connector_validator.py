import module.InfluxConnector as influx
import os
from sqlalchemy import create_engine
import pandas as pd

# Initialize SQLite DB connection
def sqlite_db_connection():
    if not os.path.exists('stocks.db'):
        open('stocks.db', 'a').close()
    engine = create_engine('sqlite:///stocks.db')
    return engine

def influx_frendly_data(table_name, date_value, data, tags):
    influx_frendly_data = [
        {
            "measurement": table_name,
            "tags": tags,
            "time": date_value,
            "fields": data
        }
    ]
    return influx_frendly_data

# Initialize InfluxConnector
influx_connector = influx.InfluxConnector()

# Call read_from_influx method
query = f'select * from {os.environ.get("Fundamental_News_Table")}'
result = influx_connector.read_from_influx(query)


engine = sqlite_db_connection()



# How to fix sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) near "truncate": syntax error
# https://stackoverflow.com/questions/3300464/how-to-fix-sqlalchemy-exc-operationalerror-sqlite3-operationalerror-near-tr

df_data = pd.DataFrame()
influx_data = []
for single_index, single_row in df_data.iterrows():
    single_influx_data = influx_frendly_data(os.environ.get("Fundamental_News_Table"),
                        single_row['date_value'],
                        {
                            "title": single_row['Title'],
                            "Entities": single_row['entities'],
                            "Sentiment": single_row['sentiment'],
                            "Organizations": single_row['organizations'],
                            "Gpe": single_row['gpe'],
                            "Locations": single_row['locations'],
                            "Others": single_row['others'],
                            "Topic_probs_csv": single_row['topic_probs_csv'],
                            "Topic_categories_csv": single_row['topic_categories_csv']
                        },
                        {
                            "symbol": single_row['symbol'],
                            "guid": single_row['guid']
                        })
    influx_data.append(single_influx_data)


print("Execution Completed")