from influxdb_client_3 import InfluxDBClient3
import time

# Configure client
client = InfluxDBClient3(host="localhost", database="sensor_db")

# Simulate sensor data
data = {
    "measurement": "environment",
    "tags": {"location": "void-linux-server", "sensor_id": "sensor_001"},
    "fields": {"temperature": 25.5, "humidity": 60},
    "time": int(time.time() * 1e9),  # Nanosecond precision
}

# Write data
client.write(data)

# Execute SQL query
result = client.query("SELECT * FROM environment LIMIT 5")
print(result)
# Print results
# for row in result:
#     print(row)
print("Data written successfully!")
