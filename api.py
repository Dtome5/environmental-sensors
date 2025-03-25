from flask import Flask
import json
import polars as pl

app = Flask.__name__
data = pl.read_csv(
    "/home/dtome/Code/Python/etlBasics/playground-series-s4e10/train.csv"
)

json_data = data.write_json()
