def aggregate_metrics(table):
    """Aggregate metrics for city planning purposes."""
    pipeline = [
        {
            "$group": {
                "_id": {
                    "location": "$location",
                    "metric_type": "$metric_type",
                    "date": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}
                    },
                },
                "avg_value": {"$avg": "$value"},
                "max_value": {"$max": "$value"},
                "min_value": {"$min": "$value"},
                "count": {"$sum": 1},
            }
        },
        {
            "$project": {
                "location": "$_id.location",
                "metric_type": "$_id.metric_type",
                "date": "$_id.date",
                "avg_value": 1,
                "max_value": 1,
                "min_value": 1,
                "count": 1,
                "_id": 0,
            }
        },
    ]

    # Execute aggregation
    aggregate_results = list(table.aggregate(pipeline))

    # Store in aggregated metrics collection
    db = table.database
    agg_table = db.aggregated_metrics

    # Insert or update aggregated results
    for result in aggregate_results:
        agg_table.update_one(
            {
                "location": result["location"],
                "metric_type": result["metric_type"],
                "date": result["date"],
            },
            {"$set": result},
            upsert=True,
        )

    return len(aggregate_results)
