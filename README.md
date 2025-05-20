# Batch Data Pipeline

This project automates the ETL (Extract, Transform, Load) process for IoT telemetry data using Prefect for workflow orchestration and MongoDB for data storage. The pipeline checks for existing data and either initializes the database or updates it hourly.

## Features
- **Dockerized Services**: MongoDB and Python app run in isolated containers.
- **Prefect Orchestration**: Automated scheduling of data updates.
- **Bulk Operations**: Efficient database updates using MongoDB bulk writes.
- **Persistent Storage**: MongoDB data persists across container restarts.

## Prerequisites
- Docker and Docker Compose installed.
- Python 3.9+ (if running outside Docker).
- `iot_telemetry_data.csv` file in the project root.

## Setup

### 1. Clone the Repository
* git clone [your-repository-url]
* cd [repository-directory]
• Run the program using ”docker-compose up --build”.
