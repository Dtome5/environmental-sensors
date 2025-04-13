# api.py
from flask import Flask, jsonify, request
from pymongo import MongoClient
from datetime import datetime, timedelta
import json

app = Flask(__name__)


def get_db_connection():
    client = MongoClient()
    db = client["test-database"]
    return db


@app.route("/api/metrics", methods=["GET"])
def get_metrics():
    db = get_db_connection()

    # Get query parameters
    location = request.args.get("location")
    metric_type = request.args.get("metric_type")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    # Build query
    query = {}
    if location:
        query["location"] = location
    if metric_type:
        query["metric_type"] = metric_type
    if start_date:
        query["timestamp"] = {"$gte": datetime.fromisoformat(start_date)}
    if end_date:
        if "timestamp" in query:
            query["timestamp"]["$lte"] = datetime.fromisoformat(end_date)
        else:
            query["timestamp"] = {"$lte": datetime.fromisoformat(end_date)}

    # Execute query
    metrics = list(db.table.find(query, {"_id": 0}).limit(1000))

    # Convert datetime objects to strings for JSON serialization
    for metric in metrics:
        if "timestamp" in metric and isinstance(metric["timestamp"], datetime):
            metric["timestamp"] = metric["timestamp"].isoformat()

    return jsonify(metrics)


@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    db = get_db_connection()
    days = int(request.args.get("days", 1))

    # Get alerts from the last X days
    start_date = datetime.now() - timedelta(days=days)
    alerts = list(db.alerts.find({"timestamp": {"$gte": start_date}}, {"_id": 0}))

    # Convert datetime objects to strings for JSON serialization
    for alert in alerts:
        if "timestamp" in alert and isinstance(alert["timestamp"], datetime):
            alert["timestamp"] = alert["timestamp"].isoformat()

    return jsonify(alerts)


@app.route("/api/aggregated", methods=["GET"])
def get_aggregated():
    db = get_db_connection()
    location = request.args.get("location")
    metric_type = request.args.get("metric_type")

    # Build query
    query = {}
    if location:
        query["location"] = location
    if metric_type:
        query["metric_type"] = metric_type

    # Get aggregated metrics
    metrics = list(db.aggregated_metrics.find(query, {"_id": 0}))

    return jsonify(metrics)


if __name__ == "__main__":
    app.run(debug=True)
