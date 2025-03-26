# from _typeshed import NoneType
import pandas as pd
import polars as pl
import duckdb as duck
import prefect
from pprint import pprint
from influxdb_client_3 import InfluxDBClient3, Point
from pymongo import MongoClient, mongo_client
from datetime import datetime


data = pl.read_csv("./iot_telemetry_data.csv")
# data = data.set_index("id")
pprint(data)
dataDicts = data.to_dicts()
client = MongoClient()
db = client["test-database"]
post = {"time": datetime.now(), "name": "user"}
posts = db.table
# for row in dataDicts:
# query = {"_id": row["device"]}
# update = {"$set": row}
# posts.update_many(query, update, upsert=True)
# post_id = posts.update_many({"id": "id"}, dataDicts)
# pprint(data.null_count().sum().max())
pprint(posts.find()[405185])
print(posts.count_documents({}))
# def load():
#     try:
#        db = client["database"]
#        data_dict = data.to_dicts()
#        commit = db.data
#        commit_id = commit.insert_many(data_dict).inserted_ids

#     except:

"""
import pandas as pd
from pymongo import MongoClient

def load_data():
    # Read sensor data from a CSV file
    df = pd.read_csv('data/sensor_data.csv')
    
    # Simple transformation: forward fill missing values
    df.fillna(method='ffill', inplace=True)
    
    # Establish connection to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['sensor_db']
    collection = db['readings']
    
    # Convert DataFrame to dictionary records and load in batches
    records = df.to_dict(orient='records')
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        collection.insert_many(records[i:i+batch_size])
    
    print("Data loaded successfully.")

if __name__ == '__main__':
    load_data()
"""
