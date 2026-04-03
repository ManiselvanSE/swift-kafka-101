#!/bin/bash
# Phase 4: Initialize Kafka Connect Connectors
# This script waits for Kafka Connect to be ready, then submits all connector configurations

set -e

CONNECT_HOST="${CONNECT_HOST:-localhost:8083}"
MAX_RETRIES=30
RETRY_DELAY=2

echo "============================================================"
echo "Phase 4: Kafka Connect Connector Initialization"
echo "============================================================"

# Function to check if Connect is ready
check_connect_ready() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Checking Kafka Connect readiness..."
    
    for i in $(seq 1 $MAX_RETRIES); do
        if curl -s "http://${CONNECT_HOST}/connectors" > /dev/null 2>&1; then
            echo "[✓] Kafka Connect is ready!"
            return 0
        fi
        echo "[*] Attempt $i/$MAX_RETRIES: Kafka Connect not ready yet, waiting ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
    done
    
    echo "[✗] Kafka Connect failed to start after $((MAX_RETRIES * RETRY_DELAY)) seconds"
    return 1
}

# Function to submit a connector
submit_connector() {
    local connector_file=$1
    local connector_name=$(basename "$connector_file" .json)
    
    echo ""
    echo "Submitting connector: $connector_name"
    echo "File: $connector_file"
    
    response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -d @"$connector_file" \
        "http://${CONNECT_HOST}/connectors")
    
    if echo "$response" | grep -q '"name"'; then
        echo "[✓] Connector '$connector_name' submitted successfully"
        return 0
    else
        echo "[✗] Failed to submit connector '$connector_name'"
        echo "Response: $response"
        return 1
    fi
}

# Check if Connect is ready
if ! check_connect_ready; then
    echo "[✗] Cannot proceed without Kafka Connect"
    exit 1
fi

echo ""
echo "============================================================"
echo "Submitting Connectors"
echo "============================================================"

# Submit all connector configurations
cd "$(dirname "$0")/connectors"

success_count=0
fail_count=0

for connector_file in *.json; do
    if [ -f "$connector_file" ]; then
        if submit_connector "$connector_file"; then
            ((success_count++))
        else
            ((fail_count++))
        fi
    fi
done

echo ""
echo "============================================================"
echo "Connector Submission Summary"
echo "============================================================"
echo "✓ Successful: $success_count"
echo "✗ Failed: $fail_count"
echo ""

# List all submitted connectors
echo "Current Connectors:"
curl -s "http://${CONNECT_HOST}/connectors" | jq '.'

echo ""
echo "============================================================"
echo "Phase 4 Setup Complete!"
echo "============================================================"
echo ""
echo "Next Steps:"
echo "1. Verify connectors are running:"
echo "   curl -s http://${CONNECT_HOST}/connectors | jq '.'"
echo ""
echo "2. Check connector status:"
echo "   curl -s http://${CONNECT_HOST}/connectors/package-events-delivered-sink/status | jq '.'"
echo ""
echo "3. Verify PostgreSQL data:"
echo "   psql -h localhost -U swifttrack -d swifttrack -c 'SELECT * FROM package_events_delivered;'"
echo ""
