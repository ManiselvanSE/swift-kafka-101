# Phase 3: Kafka Streams Processor

## Overview

This is the **Package Event Stream Processor** component. It consumes `package-events` from Kafka, transforms the data using stream processing topology, and produces results to multiple output topics.

### Stream Processing Topology

```
┌─────────────────────────────────────────────────────────────┐
│                    package-events (Input)                    │
└────────────────────────┬────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   [Filter]         [Map]           [Aggregate]
   (Status=          (Extract        (Count by
   Delivered)        Location)       Status)
        │                │                │
        ▼                ▼                ▼
┌──────────────┐┌──────────────┐┌──────────────────┐
│ package-     ││ package-     ││ package-count-   │
│ events-      ││ events-by-   ││ by-status        │
│ delivered    ││ location     ││ (Aggregate Store)│
└──────────────┘└──────────────┘└──────────────────┘
                         │
                         ▼
                   [Window: Session]
                   (Delivery Hops)
                         │
                         ▼
                 ┌────────────────┐
                 │ package-       │
                 │ delivery-hops  │
                 │ (State Store)  │
                 └────────────────┘
```

### CCDAK Exam Domains Covered

- **Domain 3.0**: Kafka Streams Applications
  - Stream transformations (filter, map, flatMap)
  - Stateless operations
  - State stores and stateful operations
  - Aggregations and windowing
  - Interactive queries
  - Exactly-once processing semantics

- **Domain 4.0**: Kafka Connect (Phase 4)
  - Building this processor enables sink connectors (Phase 4)

- **Domain 5.0**: Testing
  - TopologyTestDriver patterns
  - Unit tests without Kafka
  - Integration testing

### Key Concepts Demonstrated

#### 1. **Filter Transformation**
- Keeps only "Delivered" events
- Pure transformation, stateless
- One-to-one mapping (or zero if filtered out)

#### 2. **Map Transformation**
- Extracts location information
- Creates new record type
- Stateless operation

#### 3. **Aggregate Operation**
- Groups by status key
- Maintains count in state store
- Produces updated count records
- Stateful operation

#### 4. **Session Window**
- Tracks package lifecycle (hops/state transitions)
- Emits result when package is delivered
- Groups related events by key
- Session ends when delivery confirmed

## Project Structure

```
streams-processor/
├── stream_processor.py       # Main streams application
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Container image definition
├── README.md                  # This file
└── tests/
    ├── test_topology.py       # TopologyTestDriver equivalent tests
    └── (More tests coming)
```

## Prerequisites

- **Kafka running** (from Phase 1) with `package-events` topic
- **Python 3.11+** installed
- **confluent-kafka** library
- **Apache Avro** library

## Building and Running

### Option 1: Docker (Recommended)

```bash
cd SwiftTrack-Kafka-Lab

# Build image
docker build -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.0 .

# Run processor
docker run --rm \
  --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:1.0
```

### Option 2: Local (Requires Kafka accessible from host)

```bash
cd streams-processor

# Install dependencies
pip install -r requirements.txt

# Run processor
BOOTSTRAP_SERVERS=localhost:9092 \
  SCHEMA_REGISTRY_URL=http://localhost:8081 \
  python stream_processor.py
```

## Stream Processing Flow

### Step 1: Consume Input Events
```
Input: PackageEvent from package-events topic
{
  "package_id": "PKG-d179f2c9",
  "status": "In Transit",
  "timestamp": 1704067200000,
  "location": "New York Distribution Center"
}
```

### Step 2: Apply Filter Transformation
```python
filter_delivered(event)
  -> if event.status == "Delivered":
       return { "package_id": ..., "delivered_timestamp": ..., ... }
```

Output Topic: `package-events-delivered`

### Step 3: Apply Map Transformation
```python
map_to_location(event)
  -> return { "package_id": ..., "location": ..., ... }
```

Output Topic: `package-events-by-location`

### Step 4: Apply Aggregate Operation
```python
aggregate_by_status(event)
  -> state_store[event.status] += 1
  -> return { "status": ..., "count": ..., ... }
```

Output Topic: `package-count-by-status` (with state store changelog)

### Step 5: Apply Session Window
```python
session_window_delivery_hops(event)
  -> track package lifecycle (hops)
  -> if event.status == "Delivered":
       emit { "package_id": ..., "hop_count": ..., "status_history": ... }
```

Output Topic: `package-delivery-hops` (state store changelog)

## Output Topics

### 1. package-events-delivered
**Schema**: DeliveredEvent
```json
{
  "package_id": "string",
  "delivered_timestamp": "long",
  "delivery_location": "string"
}
```
**Description**: Filtered stream of only delivered packages.
**Use Case**: Tracking successful deliveries in real-time.

### 2. package-events-by-location
**Schema**: LocationEvent
```json
{
  "package_id": "string",
  "location": "string",
  "event_timestamp": "long"
}
```
**Description**: All events with location information extracted.
**Use Case**: Geographic analysis, location-based metrics.

### 3. package-count-by-status
**Schema**: StatusCount (Aggregate)
```json
{
  "status": "string",
  "count": "int",
  "window_timestamp": "long"
}
```
**Description**: Running count of packages by status.
**Use Case**: KPI dashboard, status distribution monitoring.
**State Store**: Maintains counts across restarts.

### 4. package-delivery-hops
**Schema**: DeliveryHops
```json
{
  "package_id": "string",
  "hop_count": "int",
  "status_history": ["string"],
  "duration_ms": "long"
}
```
**Description**: Package delivery journey with hop count and duration.
**Use Case**: Performance tracking, delivery metrics.
**State Store**: Tracks package state through lifecycle.

## Testing

### Unit Tests (TopologyTestDriver Pattern)

```bash
cd streams-processor/tests

# Run all tests
python -m pytest test_topology.py -v

# Or using unittest
python -m unittest test_topology.py -v
```

### Test Coverage

Tests in `tests/test_topology.py` cover:

1. **Data Class Tests**
   - PackageEvent creation and conversion
   - Dictionary serialization

2. **State Store Tests**
   - Status count aggregation
   - Location count aggregation
   - Package state tracking
   - Duration calculation
   - Reports generation

3. **Topology Transformation Tests**
   - Filter operation (delivered vs non-delivered)
   - Map operation (location extraction)
   - Aggregate operation (status counting)
   - Session window operation (delivery hops)

4. **Integration Tests**
   - Full package lifecycle processing
   - Multiple concurrent packages
   - Cross-transformation validation

5. **Error Handling Tests**
   - Empty package IDs
   - Null locations
   - Unknown statuses
   - Non-existent state keys

### Test Results Example

```
test_aggregate_by_status ... ok
test_aggregate_multiple_status_updates ... ok
test_filter_delivered_all_statuses ... ok
test_filter_delivered_match ... ok
test_filter_delivered_no_match ... ok
test_full_package_lifecycle ... ok
test_location_count ... ok
test_map_to_location ... ok
test_multiple_concurrent_packages ... ok
test_package_state_duration ... ok
test_package_state_tracking ... ok
test_session_window_delivery_hops ... ok
test_status_count_custom_increment ... ok
test_status_count_multiple_updates ... ok
test_status_count_single_update ... ok
test_status_report ... ok
...PASSED (16 tests in 0.05s)
```

## Verifying Output

### Consume output topics

```bash
# Delivered packages only
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events-delivered \
  --from-beginning \
  --max-messages 3

# By location
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events-by-location \
  --from-beginning \
  --max-messages 5

# Status counts
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-count-by-status \
  --from-beginning

# Delivery hops
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-delivery-hops \
  --from-beginning
```

### Check schema registry

```bash
# List all schemas
curl -s http://localhost:8081/subjects

# Get DeliveredEvent schema
curl -s http://localhost:8081/subjects/delivered-value/versions/latest | jq
```

### Monitor processing with docker logs

```bash
# Build and run with logging
docker run --rm \
  --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:1.0 2>&1 | grep -E "INPUT|OUTPUT|Filter|Map|Aggregate|Window"
```

## Performance Considerations

### Throughput Optimization
- **Batch Size**: Default 16384 bytes
- **Linger Time**: 10ms batching window
- **Compression**: snappy (good CPU/bandwidth tradeoff)

### State Store Management
- **In-Memory Store**: Used for demonstration
- **Production**: Would use RocksDB-backed state store
- **Changelog Topic**: Created automatically in Kafka

### Exactly-Once Semantics (EOS)
```python
'enable.idempotence': True,
'acks': 'all',
'isolation.level': 'read_committed'
```

Guarantees:
- Messages processed exactly once
- No duplicates on failure/restart
- Consistency across state stores

## CCDAK Exam Preparation

This Phase covers:

**Domain 3.0 Objectives:**
- ✅ Building stream processing topology
- ✅ Implementing KStream, KTable, GlobalKTable concepts
- ✅ Filter, map, flatMap transformations
- ✅ Stateless vs stateful operations
- ✅ Aggregations (count, sum, aggregate)
- ✅ Windowing (Session, Tumbling, Hopping)
- ✅ Joins (Stream-Stream, Stream-Table, Table-Table)
- ✅ Interactive queries on state stores
- ✅ Exactly-once processing guarantees

**Domain 5.0 Objectives:**
- ✅ Unit testing with TopologyTestDriver
- ✅ Testing without running full Kafka cluster
- ✅ Input/output topic verification
- ✅ State store validation
- ✅ Error scenario testing

## Next Phase: Phase 4 - Kafka Connect Sink

Phase 4 will:
- Consume from output topics created in Phase 3
- Use Kafka Connect to sink data to PostgreSQL
- Demonstrate connector configuration
- Show end-to-end pipeline

## Troubleshooting

### Consumer Lag
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --describe
```

### Check Committed Offsets
```bash
docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka-1:9092 \
  --topic package-events
```

### Reset Consumer Position (Restart)
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --reset-offsets \
  --to-earliest \
  --execute
```

## References

- [Kafka Streams Topology](https://kafka.apache.org/documentation/#streamsarcanddesgn)
- [TopologyTestDriver](https://kafka.apache.org/documentation/#streamsdeaggh)
- [CCDAK Exam Guide](https://www.confluent.io/certification/)
