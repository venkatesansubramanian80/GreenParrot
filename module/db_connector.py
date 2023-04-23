from sqlalchemy import create_engine
import os
import pandas as pd

def sqlite_db_connection():
    if not os.path.exists('stocks.db'):
        open('stocks.db', 'a').close()
    engine = create_engine('sqlite:///stocks.db')
    return engine

def insert_into_sqlite(df, table_name):
    try:
        df.to_sql(table_name, con=sqlite_db_connection(), if_exists='append', index=False)
        return True
    except Exception as e:
        print(e)
        return False

def read_from_sqlite(sql_query):
    try:
        df = pd.read_sql_table(sql_query, con=sqlite_db_connection())
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()

def update_delete_sqlite(table_name, query):
    try:
        sqlite_db_connection().execute(query)
        return True
    except Exception as e:
        print(e)
        return False