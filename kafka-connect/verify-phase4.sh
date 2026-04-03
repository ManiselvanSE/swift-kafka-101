#!/bin/bash
# Phase 4: Verify PostgreSQL Data and Kafka Connect Connectors
# Check that data is flowing from Kafka topics to PostgreSQL tables

set -e

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-swifttrack}"
POSTGRES_DB="${POSTGRES_DB:-swifttrack}"
CONNECT_HOST="${CONNECT_HOST:-localhost:8083}"

echo "============================================================"
echo "Phase 4: Verification Script"
echo "============================================================"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check PostgreSQL connectivity
check_postgres() {
    echo "Checking PostgreSQL connectivity..."
    if ! PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1" > /dev/null 2>&1; then
        echo -e "${RED}[✗] Cannot connect to PostgreSQL${NC}"
        return 1
    fi
    echo -e "${GREEN}[✓] PostgreSQL connection successful${NC}"
    return 0
}

# Function to check Kafka Connect
check_kafka_connect() {
    echo "Checking Kafka Connect connectivity..."
    if ! curl -s "http://${CONNECT_HOST}/connectors" > /dev/null 2>&1; then
        echo -e "${RED}[✗] Cannot connect to Kafka Connect${NC}"
        return 1
    fi
    echo -e "${GREEN}[✓] Kafka Connect connection successful${NC}"
    return 0
}

# Function to verify connector status
verify_connector_status() {
    echo ""
    echo "============================================================"
    echo "Connector Status"
    echo "============================================================"
    
    connectors=("package-events-delivered-sink" "package-events-by-location-sink" "package-count-by-status-sink" "package-delivery-hops-sink")
    
    for connector in "${connectors[@]}"; do
        echo ""
        echo "Connector: $connector"
        status=$(curl -s "http://${CONNECT_HOST}/connectors/${connector}/status" 2>/dev/null || echo '{}')
        
        if echo "$status" | jq -e '.connector.state' > /dev/null 2>&1; then
            state=$(echo "$status" | jq -r '.connector.state')
            if [ "$state" = "RUNNING" ]; then
                echo -e "  State: ${GREEN}$state${NC}"
            else
                echo -e "  State: ${YELLOW}$state${NC}"
            fi
        else
            echo -e "  State: ${RED}NOT FOUND${NC}"
        fi
        
        echo "  Full Status:"
        echo "$status" | jq '.' | sed 's/^/    /'
    done
}

# Function to show table row counts
show_table_counts() {
    echo ""
    echo "============================================================"
    echo "PostgreSQL Table Row Counts"
    echo "============================================================"
    echo ""
    
    PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" << 'EOF'
SELECT 
    tablename,
    (SELECT COUNT(*) FROM package_events_delivered) as package_events_delivered,
    (SELECT COUNT(*) FROM package_events_by_location) as package_events_by_location,
    (SELECT COUNT(*) FROM package_count_by_status) as package_count_by_status,
    (SELECT COUNT(*) FROM package_delivery_hops) as package_delivery_hops
FROM pg_tables 
WHERE schemaname = 'public' 
LIMIT 1;
EOF
}

# Function to show sample data
show_sample_data() {
    echo ""
    echo "============================================================"
    echo "Sample Data from Each Table"
    echo "============================================================"
    
    echo ""
    echo "--- package_events_delivered ---"
    PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "SELECT package_id, status, location FROM package_events_delivered LIMIT 5;" 2>/dev/null || echo "No data"
    
    echo ""
    echo "--- package_events_by_location ---"
    PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "SELECT location, package_id, status FROM package_events_by_location LIMIT 5;" 2>/dev/null || echo "No data"
    
    echo ""
    echo "--- package_count_by_status ---"
    PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "SELECT status, count FROM package_count_by_status ORDER BY count DESC;" 2>/dev/null || echo "No data"
    
    echo ""
    echo "--- package_delivery_hops ---"
    PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "SELECT package_id, hop_count, last_location FROM package_delivery_hops ORDER BY hop_count DESC LIMIT 5;" 2>/dev/null || echo "No data"
}

# Main execution
main() {
    # Check connectivity
    if ! check_postgres; then
        exit 1
    fi
    
    if ! check_kafka_connect; then
        echo -e "${YELLOW}[!] Warning: Kafka Connect not available, skipping connector status${NC}"
    else
        verify_connector_status
    fi
    
    # Show data
    show_table_counts
    show_sample_data
    
    echo ""
    echo "============================================================"
    echo "Verification Complete"
    echo "============================================================"
}

main
