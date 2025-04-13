import pandas as pd
import prefect
from pprint import pprint
from pymongo import MongoClient, mongo_client, UpdateOne
from datetime import datetime


post = {"time": datetime.now(), "name": "user"}

# Extract data into a dataframe
data = pd.read_csv("./iot_telemetry_data.csv")
data["id"] = range(data.shape[0])
data_dicts = data.to_dict("records")


# Check if data is in database
def check():
    res = False
    table = connection()
    if table.count_documents({}) > 0:
        res = True
    return res


# Connect to the database
def connection():
    # Using default settings
    client = MongoClient()
    db = client["test-database"]
    table = db.table
    return table


# Initial Loading
def load():
    table = connection()
    table.delete_many({})
    table_id = table.insert_many(data_dicts)
    print(f"Successfully loaded {table.count_documents({})} entries added")


# Update the database
def update():
    table = connection()
    batch_size = 1000
    operations = []

    # Prepare bulk operations
    for data_dict in data_dicts:  # Assuming data_dicts is a list of update documents
        operations.append(
            UpdateOne(
                {"_id": data_dict["id"]},  # Use appropriate ID field
                {"$set": data_dict},  # Update with document data
                upsert=True,  # Adds new values
            )
        )

        # Execute in batches
        if len(operations) == batch_size:
            table.bulk_write(operations, ordered=False)
            operations = []

        # Process remaining operations
    if operations:
        table.bulk_write(operations, ordered=False)

    print(f"Successfully processed {len(data_dicts)} documents")
