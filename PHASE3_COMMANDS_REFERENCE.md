# Phase 3: Kafka Streams Processor - Commands Reference

**Date**: 2026-04-03  
**Purpose**: Complete list of commands used to implement, build, and test Phase 3

---

## 1. Topic Creation Commands

Create the four output topics for stream transformations:

```bash
# Topic 1: Delivered events (filtered)
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --create --topic package-events-delivered --partitions 3 --replication-factor 3

# Topic 2: Location events (mapped)
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --create --topic package-events-by-location --partitions 3 --replication-factor 3

# Topic 3: Status counts (aggregated) - single partition for global state
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --create --topic package-count-by-status --partitions 1 --replication-factor 3

# Topic 4: Delivery hops (windowed)
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --create --topic package-delivery-hops --partitions 3 --replication-factor 3

# Verify topics created
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list | grep package-events
```

---

## 2. Docker Image Build Commands

Build different versions of the processor image:

```bash
# Build initial version (v1.0) - had schema access issues
cd "e:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab"
docker build --no-cache -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.0 .

# Build version with fastavro dependency (v1.1)
docker build --no-cache -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.1 .

# Build version with extended timeout (v1.2)
docker build --no-cache -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.2 .

# Build version with debug logging
docker build -f Dockerfile.debug \
  -t swifttrack-streams-debug:1.0 .

# Build final production version (v2.0) - with fixed schema access
docker build --no-cache -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:2.0 .

# List all processor images
docker image ls | grep streams-processor
```

---

## 3. Processor Execution Commands

Run the processor in different modes:

```bash
# Run in foreground (blocks until timeout)
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:2.0

# Run in background with output capture
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:2.0 &

# Run debug version with verbose logging
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-debug:1.0

# Build and run in one command
cd "e:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab" && \
  docker build --no-cache -f streams-processor/Dockerfile \
    -t swifttrack-streams-processor:2.0 . && \
  docker run --rm --network swifttrack-kafka-lab_default \
    -e BOOTSTRAP_SERVERS=kafka-1:9092 \
    -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    swifttrack-streams-processor:2.0
```

---

## 4. Consumer Group Management

Monitor and manage the processor's consumer group:

```bash
# Describe consumer group status
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
  --group streams-processor-group --describe

# List all consumer groups
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 --list

# Check all groups with details
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
  --all-groups --describe

# Reset consumer group to earliest
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
  --group streams-processor-group --reset-offsets --to-earliest --execute \
  --topic package-events
```

---

## 5. Output Topic Verification

Consume and verify messages in output topics:

```bash
# Verify delivered events topic (5 messages expected)
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-events-delivered --from-beginning --max-messages 3

# Expected output:
# {"package_id": "PKG-5531c949", "delivered_timestamp": 1775161077507, "delivery_location": "Los Angeles Hub"}
# {"package_id": "PKG-5531c949", "delivered_timestamp": 1775161077507, "delivery_location": "Los Angeles Hub"}
# {"package_id": "PKG-4669442e", "delivered_timestamp": 1775161075898, "delivery_location": "Los Angeles Hub"}


# Verify location events topic (20 messages expected)
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-events-by-location --from-beginning --max-messages 2

# Expected output:
# {"package_id": "PKG-d179f2c9", "location": "Customer Location", "event_timestamp": 1775161076401}
# {"package_id": "PKG-d179f2c9", "location": "New York Distribution Center", "event_timestamp": 1775161076501}


# Verify status count aggregation topic
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-count-by-status --from-beginning --max-messages 4

# Expected output:
# {"status": "Package Picked Up", "count": 1, "window_timestamp": 1775193740130}
# {"status": "In Transit", "count": 1, "window_timestamp": 1775193740139}
# {"status": "Out for Delivery", "count": 1, "window_timestamp": 1775193740146}
# {"status": "Delivered", "count": 1, "window_timestamp": 1775193740154}


# Verify delivery hops topic with lifecycle tracking
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-delivery-hops --from-beginning --max-messages 2

# Expected output:
# {"package_id": "PKG-5531c949", "hop_count": 1, "status_history": ["Delivered"], "duration_ms": 0}
# {"package_id": "PKG-5531c949", "hop_count": 3, "status_history": ["Package Picked Up", "In Transit", "Out for Delivery", "Delivered"], "duration_ms": 302}


# Consume with key information
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-events-delivered --from-beginning --property print.key=true
```

---

## 6. Input Topic Verification

Verify the Phase 2 input messages are still in the topic:

```bash
# Check topic exists and has messages
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --describe --topic package-events

# Expected output:
# Topic: package-events   TopicId: S_Z4-SvjQQG-kmwGbTPmNw PartitionCount: 3   ReplicationFactor: 3


# Check log sizes to verify messages exist
docker exec kafka-1 kafka-log-dirs --bootstrap-server kafka-1:9092 \
  --describe --topic-list package-events

# Expected output:
# Partition sizes: package-events-0: 532 bytes, package-events-1: 969 bytes, package-events-2: 487 bytes


# Consume raw messages without deserialization
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-events --from-beginning --max-messages 1

# Expected output shows Avro binary format:
# PKG-d179f2c9    PKG-d179f2c9"Package Picked Up...
```

---

## 7. Python Testing Commands

Run unit tests for the stream processor topology:

```bash
# Navigate to project
cd "e:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab\streams-processor"

# Run all tests
python -m pytest tests/test_topology.py -v

# Run specific test class
python -m pytest tests/test_topology.py::TestStreamTopology -v

# Run with coverage
python -m pytest tests/test_topology.py --cov=stream_processor --cov-report=html

# Run and show print statements
python -m pytest tests/test_topology.py -v -s
```

---

## 8. Docker Network and Container Management

```bash
# Check Docker network
docker network ls | grep swifttrack

# Inspect network
docker network inspect swifttrack-kafka-lab_default

# View running containers
docker ps | grep swifttrack

# View all containers (including stopped)
docker ps -a | grep streams-processor

# Stop running processor container
docker stop <CONTAINER_ID>

# Remove image
docker rmi swifttrack-streams-processor:1.0

# Clean up all processor images
docker rmi $(docker images | grep streams-processor | awk '{print $3}')
```

---

## 9. Development Workflow Commands

```bash
# Copy processor code into Docker
cd "e:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab"

# Edit files
code streams-processor/stream_processor.py

# Rebuild image after code changes
docker build --no-cache -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:2.0 .

# Run with auto-restart for development
docker run --name streams-test --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:2.0

# Check logs from running container
docker logs <CONTAINER_ID>

# Monitor in real-time
docker logs -f <CONTAINER_ID>
```

---

## 10. Debugging Commands

```bash
# Check processor initialization
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-debug:1.0

# Test Schema Registry connectivity
docker run --rm --network swifttrack-kafka-lab_default \
  curlimage:latest curl http://schema-registry:8081/subjects

# Test Kafka broker connectivity
docker run --rm --network swifttrack-kafka-lab_default \
  confluentinc/cp-kafka:7.9.0 \
  kafka-broker-api-versions --bootstrap-server kafka-1:9092

# Check partition assignment
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --describe --topic package-events
```

---

## 11. Cleanup and Reset Commands

```bash
# Reset consumer group completely
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
  --group streams-processor-group --delete

# Delete output topics (caution!)
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --delete --topic package-events-delivered
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --delete --topic package-events-by-location
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --delete --topic package-count-by-status
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --delete --topic package-delivery-hops

# Clean up Docker images
docker rmi swifttrack-streams-processor:1.0
docker rmi swifttrack-streams-processor:1.1
docker rmi swifttrack-streams-processor:1.2
docker rmi swifttrack-streams-processor:2.0
docker rmi swifttrack-streams-debug:1.0

# Remove Dockerfile.debug
rm Dockerfile.debug
```

---

## 12. Final Verification Workflow

Complete verification checklist with commands:

```bash
# 1. Verify topics exist
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list | grep package-events

# 2. Build processor
cd "e:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab"
docker build --no-cache -f streams-processor/Dockerfile -t swifttrack-streams-processor:2.0 .

# 3. Run processor
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:2.0
# Wait for completion (~65 seconds)

# 4. Check consumer group
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
  --group streams-processor-group --describe
# Verify: Total offset sum = 20 (4 + 12 + 4)

# 5. Verify each output topic
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-events-delivered --from-beginning --max-messages 3
# Verify: JSON output with package_id, delivered_timestamp, delivery_location

docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-events-by-location --from-beginning --max-messages 2
# Verify: JSON output with package_id, location, event_timestamp

docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-count-by-status --from-beginning --max-messages 4
# Verify: 4 different status types with counts

docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-delivery-hops --from-beginning --max-messages 2
# Verify: JSON with hop_count, status_history array, duration_ms

# ✅ Phase 3 Verification Complete!
```

---

## Summary of Commands by Category

| Category | Command Count | Key Commands |
|----------|---------------|--------------|
| Topic Creation | 4 | `kafka-topics --create` |
| Docker Build | 5 | `docker build` |
| Processor Execution | 4 | `docker run` |
| Consumer Groups | 4 | `kafka-consumer-groups` |
| Output Verification | 5 | `kafka-console-consumer` |
| Input Verification | 3 | `kafka-topics --describe`, `kafka-log-dirs` |
| Testing | 4 | `pytest` commands |
| Network Management | 3 | `docker network` |
| Debugging | 4 | Various diagnostic commands |
| Cleanup | 3 | `docker rmi`, `kafka-topics --delete` |

**Total: 45+ commands documented**

---

## Notes

- All commands assume Kafka cluster is running on Docker network `swifttrack-kafka-lab_default`
- Bootstrap servers: `kafka-1:9092`, `kafka-2:9092`, `kafka-3:9092`
- Schema Registry: `http://schema-registry:8081`
- Phase 2 input topic: `package-events` (20 messages)
- Processor consumer group: `streams-processor-group`
- Processing time: ~62 seconds (includes poll timeouts)
- Success rate: 100% (20/20 messages)

