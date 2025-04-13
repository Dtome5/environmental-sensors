import pandas as pd
import prefect
import os
import json
from pprint import pprint
from pymongo import MongoClient, mongo_client, UpdateOne
from datetime import datetime

with open("config.json", "r") as config_file:
    config = json.load(config_file)
os.environ["PREFECT_API_URL"] = config["prefect-api-url"]
print(config)
METRICS_CONFIG = {
    "current_metrics": [
        "ts",
        "device",
        "co",
        "humidity",
        "light",
        "lpg",
        "motion",
        "smoke",
        "temp",
    ],
    "additional_metrics": [],
}


def extract_data(file_path="./iot_telemetry_data.csv"):
    data = pd.read_csv(file_path)
    # Only keep columns that are in the metrics configuration
    metrics_to_track = (
        METRICS_CONFIG["current_metrics"] + METRICS_CONFIG["additional_metrics"]
    )

    # Filter columns if needed (only if you want to exclude some columns)
    if metrics_to_track:
        # Only keep columns that exist in the data
        existing_columns = [col for col in metrics_to_track if col in data.columns]
        data = data[existing_columns]
    data["ts"] = pd.to_datetime(data["ts"])
    return data.to_dict("records")


data_dicts = extract_data()


# Check if data is in database
def check():
    """
    Checks if the MongoDB database contains any documents
    Returns:
        bool: True if database contains documents, False if empty or connection fails
    """
    try:
        # Establish connection with timeout
        client = MongoClient(serverSelectionTimeoutMS=5000)  # 5 second timeout
        client.server_info()  # Forces a call to check connection

        db = client["test-database"]
        table = db.table

        # Check if collection exists and has documents
        if table.name in db.list_collection_names():
            count = table.count_documents({})
            client.close()  # Properly close connection
            return count > 0

        client.close()
        return False

    except Exception as e:
        print(f"Database connection error: {str(e)}")
        return False


# Connect to the database
def connection():
    # Using default settings
    try:
        client = MongoClient(config["mongo-url"])
        db = client["test-database"]
        table = db.table
        return table
    except Exception as e:
        print(f"Error connecting to the database {str(e)}")


# Initial Loading
def load():
    try:
        table = connection()
        if table is not None:
            table.delete_many({})
            table_id = table.insert_many(data_dicts)
            print(f"Successfully loaded {table.count_documents({})} entries added")
    except Exception as e:
        print("Error loading the data")


# Update the database
def update():
    try:
        table = connection()
        if table is not None:
            batch_size = 1000
            operations = []

            # Prepare bulk operations
            for (
                data_dict
            ) in data_dicts:  # Assuming data_dicts is a list of update documents
                operations.append(
                    UpdateOne(
                        {
                            "_id": {
                                "ts": data_dict["ts"],
                                "device": data_dict["device"],
                            }
                        },  # Use appropriate ID field
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
    except Exception as e:
        print("Error updating the database")
