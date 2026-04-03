# Phase 4: Kafka Connect → PostgreSQL Sink

**Objective**: Integrate Phase 3 stream processor output with PostgreSQL database using Kafka Connect JDBC sink connectors.

**Status**: ✅ Ready for Implementation and Testing

---

## Overview

Phase 4 extends the SwiftTrack architecture by:
1. Creating PostgreSQL tables mapped to Phase 3 output topics
2. Setting up Kafka Connect JDBC sink connectors
3. Streaming processed data from Kafka topics to database tables
4. Verifying end-to-end data persistence

### Architecture

```
Phase 3 (Streams Processor Output Topics)
    ↓
    ├─ package-events-delivered
    ├─ package-events-by-location
    ├─ package-count-by-status
    └─ package-delivery-hops
    ↓
Kafka Connect JDBC Sink Connectors (4 connectors)
    ↓
PostgreSQL Database (swifttrack)
    ├─ package_events_delivered
    ├─ package_events_by_location
    ├─ package_count_by_status
    └─ package_delivery_hops
```

---

## Prerequisites

- Phase 3 stream processor running and producing to output topics
- Docker and Docker Compose installed
- PostgreSQL 15 available (included in docker-compose.yml)
- Kafka Connect 7.9.0 (built from ./kafka-connect/Dockerfile)

---

## Files & Structure

```
kafka-connect/
├── Dockerfile                      # Kafka Connect image with JDBC driver
├── init-postgres.sql              # PostgreSQL schema initialization
├── init-connectors.sh             # Script to submit connectors to Kafka Connect
├── verify-phase4.sh               # Script to verify data in PostgreSQL
└── connectors/
    ├── package-delivered-sink.json        # Sink for package-events-delivered
    ├── package-by-location-sink.json      # Sink for package-events-by-location
    ├── package-status-count-sink.json     # Sink for package-count-by-status
    └── package-delivery-hops-sink.json    # Sink for package-delivery-hops
```

---

## Setup Instructions

### Step 1: Build Kafka Connect Image

```bash
cd kafka-connect
docker build -t swifttrack-kafka-connect:1.0 .
cd ..
```

### Step 2: Start All Services

```bash
docker-compose up -d

# Verify all services are running
docker-compose ps
```

Expected output:
```
NAME               STATUS
kafka-1            Up (healthy)
kafka-2            Up (healthy)
kafka-3            Up (healthy)
schema-registry    Up (healthy)
postgres           Up (healthy)
kafka-connect      Up (healthy)
```

### Step 3: Wait for Initialization

PostgreSQL needs time to:
1. Start the container
2. Initialize the database  
3. Execute init-postgres.sql (create tables)

Give it 10-15 seconds, then verify:

```bash
docker-compose logs postgres | grep "database system is ready"
```

### Step 4: Verify PostgreSQL Schema

```bash
# Connect to PostgreSQL container
docker exec -it postgres psql -U swifttrack -d swifttrack

# In psql shell:
\dt
# Shows: package_events_delivered, package_events_by_location, 
#        package_count_by_status, package_delivery_hops

\q
```

### Step 5: Initialize Kafka Connect Connectors

```bash
# Make script executable
chmod +x kafka-connect/init-connectors.sh

# Submit all connectors
kafka-connect/init-connectors.sh
```

Or manually submit each connector:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d @kafka-connect/connectors/package-delivered-sink.json \
  http://localhost:8083/connectors
```

### Step 6: Verify Connector Status

```bash
# Get all connectors
curl -s http://localhost:8083/connectors | jq '.'

# Check specific connector status
curl -s http://localhost:8083/connectors/package-events-delivered-sink/status | jq '.'
```

### Step 7: Run Phase 3 Streams Processor (Reset Required)

Before running the processor, reset the consumer group to reprocess all messages:

```bash
# Reset consumer group to earliest offset
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --reset-offsets \
  --to-earliest \
  --execute \
  --topic package-events

# Run the Phase 3 processor
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:2.0
```

### Step 8: Verify Data in PostgreSQL

```bash
# Option 1: Use verification script
bash kafka-connect/verify-phase4.sh

# Option 2: Manually query PostgreSQL
docker exec -it postgres psql -U swifttrack -d swifttrack << EOF
SELECT 'package_events_delivered' as table_name, COUNT(*) as row_count 
FROM package_events_delivered
UNION ALL
SELECT 'package_events_by_location', COUNT(*) FROM package_events_by_location
UNION ALL
SELECT 'package_count_by_status', COUNT(*) FROM package_count_by_status
UNION ALL
SELECT 'package_delivery_hops', COUNT(*) FROM package_delivery_hops
ORDER BY table_name;
EOF
```

Expected output after Phase 3 processor completes:
```
         table_name         | row_count
-----------------------------+----------
 package_count_by_status    |        4
 package_delivery_hops      |       20
 package_events_by_location |       60
 package_events_delivered   |       20
```

---

## Connector Configurations

### Connector 1: package-events-delivered-sink

**Source Topic**: `package-events-delivered`  
**Target Table**: `package_events_delivered`  
**Key Fields**: `package_id` (upsert enabled)

Maps the final delivery status for each package.

Configuration:
```json
{
  "insert.mode": "upsert",
  "pk.fields": "package_id"
}
```

### Connector 2: package-events-by-location-sink

**Source Topic**: `package-events-by-location`  
**Target Table**: `package_events_by_location`  
**Key Fields**: `location,package_id` (composite key)

Tracks package events grouped by location.

Configuration:
```json
{
  "insert.mode": "upsert",
  "pk.fields": "location,package_id"
}
```

### Connector 3: package-count-by-status-sink

**Source Topic**: `package-count-by-status`  
**Target Table**: `package_count_by_status`  
**Key Fields**: `status` (upsert enabled)

Maintains aggregate counts by package status.

Configuration:
```json
{
  "insert.mode": "upsert",
  "pk.fields": "status"
}
```

### Connector 4: package-delivery-hops-sink

**Source Topic**: `package-delivery-hops`  
**Target Table**: `package_delivery_hops`  
**Key Fields**: `package_id` (upsert enabled)

Tracks the number of hops/locations each package visited.

Configuration:
```json
{
  "insert.mode": "upsert",
  "pk.fields": "package_id"
}
```

---

## Troubleshooting

### Issue 1: Kafka Connect Container Fails to Start

**Symptoms**:
```
Error: docker: Error response from daemon: failed to build image, build canceled
```

**Cause**: Dockerfile build failure (missing base image or confluentinc-hub issues)

**Solution**:
```bash
# Try pulling the base image explicitly
docker pull confluentinc/cp-kafka-connect:7.9.0

# Rebuild
cd kafka-connect && docker build . && cd ..
```

### Issue 2: Connectors Stay in FAILED State

**Symptoms**:
```
curl http://localhost:8083/connectors/...-sink/status
# Returns: "state": "FAILED"
```

**Common Causes**:
1. PostgreSQL not ready
2. Invalid connection string
3. Table doesn't exist
4. Schema mismatch between Avro and PostgreSQL

**Solution**:
```bash
# Check connector logs
docker logs kafka-connect | grep package-events-delivered-sink

# Verify PostgreSQL is ready
docker exec postgres pg_isready

# Verify tables exist
docker exec -it postgres psql -U swifttrack -d swifttrack -c "\dt"
```

### Issue 3: No Data Appearing in PostgreSQL

**Symptoms**:
- Connectors show RUNNING state
- PostgreSQL tables exist but are empty
- Phase 3 processor is running

**Possible Causes**:
1. Output topics don't have data
2. Consumer group wrong in connector (uses group id from properties)
3. Connector started before Phase 3 produced data

**Solution**:
```bash
# Check if output topics have data
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events-delivered \
  --from-beginning \
  --max-messages 1

# If no data: Re-run Phase 3 processor
# If data exists: Check connector logs for parsing errors
docker logs kafka-connect | grep "ERROR"
```

### Issue 4: PostgreSQL Connection Refused

**Symptoms**:
```
java.net.ConnectException: Connection refused
```

**Cause**: PostgreSQL container not running or not ready

**Solution**:
```bash
# Check PostgreSQL status
docker-compose ps postgres

# Check if port is accessible
docker exec kafka-connect nc -zv postgres 5432

# View PostgreSQL logs
docker logs postgres
```

---

## Verification Queries

### Check Total Row Counts

```sql
SELECT 
    'package_events_delivered' as table_name, 
    COUNT(*) as row_count 
FROM package_events_delivered
UNION ALL
SELECT 'package_events_by_location', COUNT(*) FROM package_events_by_location
UNION ALL
SELECT 'package_count_by_status', COUNT(*) FROM package_count_by_status
UNION ALL
SELECT 'package_delivery_hops', COUNT(*) FROM package_delivery_hops
ORDER BY table_name;
```

### View Delivered Packages

```sql
SELECT package_id, status, location, event_time_ms 
FROM package_events_delivered
ORDER BY created_at DESC
LIMIT 10;
```

### Check Delivery Hops Distribution

```sql
SELECT hop_count, COUNT(*) as package_count
FROM package_delivery_hops
GROUP BY hop_count
ORDER BY hop_count;
```

### View Status Aggregates

```sql
SELECT status, count
FROM package_count_by_status
ORDER BY count DESC;
```

---

## Common Commands

### Start Services
```bash
docker-compose up -d
```

### Stop Services
```bash
docker-compose down
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka-connect
```

### Connect to PostgreSQL
```bash
docker exec -it postgres psql -U swifttrack -d swifttrack
```

### Connect to Kafka
```bash
# Consume from output topic
docker exec -it kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events-delivered \
  --from-beginning
```

### Check Connector REST API
```bash
# List all connectors
curl -s http://localhost:8083/connectors

# Get connector config
curl -s http://localhost:8083/connectors/package-events-delivered-sink

# Get connector status
curl -s http://localhost:8083/connectors/package-events-delivered-sink/status

# Pause a connector
curl -X PUT http://localhost:8083/connectors/package-events-delivered-sink/pause

# Resume a connector
curl -X PUT http://localhost:8083/connectors/package-events-delivered-sink/resume

# Delete a connector
curl -X DELETE http://localhost:8083/connectors/package-events-delivered-sink
```

---

## Phase 4 Success Criteria

✅ Phase 4 is complete when:

1. All 4 connectors are in RUNNING state
2. PostgreSQL tables have data matching Phase 3 output
3. Upsert logic works (re-running Phase 3 updates records, not duplicates)
4. All 20 processed messages appear in database tables

Expected final state:
```
20 records in package_events_delivered (1 per unique package)
60 records in package_events_by_location (3 events × 20 packages)
4 records in package_count_by_status (Picked Up, In Transit, Out for Delivery, Delivered)
20 records in package_delivery_hops (1 per package with final hop count)
```

---

## Next: Phase 5

Once Phase 4 is verified, Phase 5 will focus on:
- TopologyTestDriver unit tests for stream processor
- Performance profiling and optimization
- Failover and recovery scenario testing
