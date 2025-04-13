
#!/bin/bash

# Airflow DAG Runner Script
# This script provides commands to manage Airflow DAGs

# Set Airflow environment variables if needed
# You can customize these as per your installation
export AIRFLOW_HOME=${AIRFLOW_HOME:-~/airflow}

# Function to display usage information
show_usage() {
  echo "Airflow DAG Runner"
  echo "Usage: $0 [command]"
  echo ""
  echo "Commands:"
  echo "  start       - Start Airflow services (webserver and scheduler)"
  echo "  stop        - Stop Airflow services"
  echo "  status      - Check status of Airflow DAGs"
  echo "  list        - List all available DAGs"
  echo "  trigger     - Trigger a specific DAG"
  echo "  pause       - Pause a specific DAG"
  echo "  unpause     - Unpause a specific DAG"
  echo "  initialize  - Initialize Airflow database"
  echo "  help        - Show this help message"
  echo ""
}

# Function to start Airflow services
start_airflow() {
  echo "Starting Airflow services..."
  
  # Start the web server in the background
  airflow webserver -p 8080 &
  
  # Start the scheduler in the background
  airflow scheduler &
  
  echo "Airflow services started. Web UI available at http://localhost:8080"
}

# Function to stop Airflow services
stop_airflow() {
  echo "Stopping Airflow services..."
  
  # Find and kill webserver and scheduler processes
  pkill -f "airflow webserver"
  pkill -f "airflow scheduler"
  
  echo "Airflow services stopped."
}

# Function to check status of DAGs
check_status() {
  echo "Checking status of all DAGs..."
  airflow dags list
}

# Function to list all DAGs
list_dags() {
  echo "Listing all available DAGs..."
  airflow dags list
}

# Function to trigger a specific DAG
trigger_dag() {
  if [ -z "$1" ]; then
    echo "Error: DAG ID is required."
    echo "Usage: $0 trigger <dag_id> [execution_date]"
    return 1
  fi
  
  DAG_ID="$1"
  EXECUTION_DATE="$2"
  
  if [ -z "$EXECUTION_DATE" ]; then
    echo "Triggering DAG: $DAG_ID..."
    airflow dags trigger "$DAG_ID"
  else
    echo "Triggering DAG: $DAG_ID for execution date: $EXECUTION_DATE..."
    airflow dags trigger -e "$EXECUTION_DATE" "$DAG_ID"
  fi
}

# Function to pause a DAG
pause_dag() {
  if [ -z "$1" ]; then
    echo "Error: DAG ID is required."
    echo "Usage: $0 pause <dag_id>"
    return 1
  fi
  
  DAG_ID="$1"
  echo "Pausing DAG: $DAG_ID..."
  airflow dags pause "$DAG_ID"
}

# Function to unpause a DAG
unpause_dag() {
  if [ -z "$1" ]; then
    echo "Error: DAG ID is required."
    echo "Usage: $0 unpause <dag_id>"
    return 1
  fi
  
  DAG_ID="$1"
  echo "Unpausing DAG: $DAG_ID..."
  airflow dags unpause "$DAG_ID"
}

# Function to initialize Airflow database
initialize_airflow() {
  echo "Initializing Airflow database..."
  airflow db init
  
  # Create an admin user (optional)
  echo "Creating admin user..."
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
  
  echo "Airflow database initialized."
}

# Main script logic
case "$1" in
  start)
    start_airflow
    ;;
  stop)
    stop_airflow
    ;;
  status)
    check_status
    ;;
  list)
    list_dags
    ;;
  trigger)
    trigger_dag "$2" "$3"
    ;;
  pause)
    pause_dag "$2"
    ;;
  unpause)
    unpause_dag "$2"
    ;;
  initialize)
    initialize_airflow
    ;;
  help|*)
    show_usage
    ;;
esac

exit 0
