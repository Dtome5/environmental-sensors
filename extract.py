# from _typeshed import NoneType
import polars as pl
import duckdb as duck
import prefect
import pprint
from influxdb_client_3 import InfluxDBClient3
from pymongo import MongoClient, mongo_client
from datetime import datetime


data = pl.read_csv("./iot_telemetry_data.csv")
##########################################################
# data.insert_column(
#     data.shape[1], pl.Series("time", [datetime.now() for _ in range(data.shape[0])])
# )
# print(data.columns, data)
# client = InfluxDBClient3(host="localhost", port="8086", org="my-org", username="dtome")

# client._write_api.write(
#     bucket="environment",
#     record=data,
#     data_frame_measurement="motion",
#     data_frame_timestamp_column="time",
# )
# queryselect = "select * from motion"
# reader = client.query(query=queryselect, language="sql")
# table = reader.read_all()
# print(table.to_pandas())
# db.create_database

##########################################################
# from google.cloud import bigquery
# from dotenv import dotenv_values
# from fastapi import FastAPI

dataDicts = data.to_dicts()
client = MongoClient()
db = client["test-database"]
post = {"time": datetime.now(), "name": "user"}
posts = db.table
# post_id = posts.insert_many(dataDicts).inserted_ids
pprint.pprint(_)
print(posts.count_documents({}))
