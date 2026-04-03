# 🎬 SwiftTrack Kafka Lab - Complete Demo Walkthrough

> **📌 NOTE:** For actual demo execution results and troubleshooting, see [DEMO_EXECUTION_REPORT.md](DEMO_EXECUTION_REPORT.md)

**Complete step-by-step guide to run the entire 6-phase Kafka pipeline (15-20 minutes)**

---

## 📋 Prerequisites Checklist

Before starting, ensure:
- [ ] Docker & Docker Compose installed
- [ ] 4+ GB RAM available
- [ ] Ports available: 9092, 9093, 9094, 8081, 8083, 5432, 9090, 3000
- [ ] Project cloned: `git clone https://github.com/yourusername/swifttrack-kafka-lab.git`
- [ ] Python 3.9+ (if running produced on host machine - see **Docker Alternative** below)

---

## 🚀 PHASE 1: Start Infrastructure (2 minutes)

### Step 1.1: Start all services
```bash
cd swifttrack-kafka-lab
docker-compose up -d
```

**Output should show:**
```
✔ kafka-1                    Started  1.1s
✔ kafka-2                    Started  1.0s
✔ kafka-3                    Started  1.1s
✔ schema-registry            Started  1.2s
✔ postgres                   Started  1.1s
✔ kafka-connect              Started  1.3s
✔ prometheus                 Started  1.2s
✔ grafana                    Started  1.2s
```

### Step 1.2: Verify all services are healthy
```bash
docker-compose ps
```

**Expected output (all showing "Up"):**
```
NAME                  SERVICE              STATUS
kafka-1              kafka-1              Up (healthy)
kafka-2              kafka-2              Up (healthy)
kafka-3              kafka-3              Up (healthy)
schema-registry      schema-registry      Up
postgres             postgres             Up (healthy)
kafka-connect        kafka-connect        Up (health: starting)
prometheus           prometheus           Up (healthy)
grafana              grafana              Up (healthy)
```

> ⏱️ **Wait 30 seconds** for kafka-connect healthcheck to pass

### Step 1.3: Verify Prometheus is scraping
```bash
curl http://localhost:9090/api/v1/targets
```

Should show targets like `kafka-1`, `kafka-2`, `kafka-3`, `prometheus` with `"health":"up"`

---

## 📤 PHASE 2: Produce Avro Events (3 minutes)

### Step 2.1: Install producer dependencies
```bash
cd producer-app
pip install -r requirements.txt
```

**Dependencies installed:**
- confluent-kafka
- fastavro
- requests

### Step 2.2: Run the producer

#### Option A: Using Docker (✅ Recommended for cross-platform compatibility)
```bash
# Use absolute path (works on all platforms)
docker run --rm --network swifttrack-kafka-lab_default \
  -v "E:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab\producer-app:/app" \
  python:3.11 bash -c "cd /app && \
    pip install -q confluent-kafka==2.2.0 avro==1.11.3 requests authlib httpx cachetools websocket-client && \
    python producer.py"
```

> **Note:** Replace path with your actual project location if different. On Mac/Linux, use `/path/to/SwiftTrack-Kafka-Lab/producer-app:/app`

#### Option B: Host Machine Python (Requires proper environment setup)
```bash
# First ensure all dependencies are installed
pip install --upgrade pip
pip install confluent-kafka==2.2.0 avro==1.11.3 requests authlib httpx cachetools websocket-client

# Then run producer
cd producer-app
python producer.py
```

> ⚠️ **Note:** If you get `ModuleNotFoundError` errors with Option B, use Option A (Docker) instead.

**Expected output:**
```
[2026-04-03 08:02:57] INFO: Starting PackageProducer...
[2026-04-03 08:02:57] INFO: Using existing schema version 1
[2026-04-03 08:02:57] INFO: Creating Kafka producer with idempotence enabled, acks=all, compression=snappy
[2026-04-03 08:02:57] INFO: PackageProducer initialized successfully
[2026-04-03 08:02:57] INFO: Simulating 5 packages with 4 events each
[2026-04-03 08:02:59] INFO: Message sent: PackageID=PKG-9eee2413, Status=*, Partition=0, Offset=20
[2026-04-03 08:02:59] INFO: Message sent: PackageID=PKG-9eee2413, Status=*, Partition=0, Offset=21
...
[2026-04-03 08:02:59] INFO: All messages sent successfully. Flushed producer.
[2026-04-03 08:02:59] INFO: PackageProducer completed successfully
```

✅ **Success Indicators:**
- No errors or exceptions
- All 20 messages sent (5 packages × 4 events each)
- Output shows message offsets incrementing
- Producer closed gracefully

### Step 2.3: Verify topics created
```bash
docker-compose exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:9092 --list
```

**Expected output:**
```
__consumer_offsets
_connect-configs
_connect-offsets
_connect-status
_schemas
raw_products
```

**Topic details:**
- `raw_products` ← Created by producer (contains 1000 Avro events)
- `processed_products` ← Will be created by streams processor (optional, depends on topology)
- `_schemas` ← Schema Registry internal topic
- `_connect-*` ← Kafka Connect internal topics
- `__consumer_offsets` ← Consumer group offsets tracking

### Step 2.4: Verify events in topic

#### ✅ Option A: Consume with Timeout (MOST RELIABLE)
```bash
docker run --rm --network swifttrack-kafka-lab_default \
  confluentinc/cp-kafka:7.9.0 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic raw-products \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 5000
```

**Expected output (Avro binary mixed with readable text):**
```
PKG-f425c1c1"Package Picked Up~~索g0Chicago Sorting Facility
PKG-f425c1c1In Transit~~索g"Customer Location
PKG-f425c1c1 Out for Delivery~~索g8New York Distribution Center
Processed a total of 3 messages
```

> ℹ️ **Important:** The garbled characters (`~~索g0`) are Avro binary encoding. This is **normal and expected**!
> - Readable parts = String values (package ID, status, location)
> - Garbled parts = Binary schema metadata and type information
> - The consumer successfully read all messages ✅

#### Option B: Check Topic Metadata
```bash
docker run --rm --network swifttrack-kafka-lab_default \
  confluentinc/cp-kafka:7.9.0 kafka-topics \
  --bootstrap-server kafka-1:9092 \
  --describe --topic raw-products
```

**Expected output:**
```
Topic: raw-products  Partition: 0  Leader: 1  Replicas: [1]  Isr: [1]
```

✅ **Success Indicators - Look for ALL of these:**
- ✅ Consumer output shows message **IDs and status values** (even if garbled)
- ✅ Line shows "**Processed a total of X messages**" (not 0!)
- ✅ Topic describes successfully with leader assigned
- ✅ No "UNKNOWN_TOPIC_OR_PARTITION" errors

---

## ⚙️ PHASE 3: Stream Processing (5 minutes)

### Step 3.1: Install stream processor dependencies
```bash
cd ../streams-processor
pip install -r requirements.txt
```

**Key dependencies:**
- kafka-python (Streams client)
- pytest (testing framework)
- pytest-cov (coverage reporting)

### Step 3.2: Review topology
```bash
# See the processing logic
cat topology.py
```

**The topology does:**
1. **Consumes** from `raw_products` topic
2. **Transforms** each product:
   - Standardizes category names (UPPERCASE)
   - Calculates discount_price = price * 0.9
   - Adds processing_timestamp
3. **Produces** to `processed_products` topic

### Step 3.3: Run the stream processor

#### Option A: Using Docker (✅ Recommended)
```bash
# Use absolute path (works on all platforms: Windows, Mac, Linux)
docker run --rm --network swifttrack-kafka-lab_default \
  -v "E:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab\streams-processor:/app" \
  python:3.11 bash -c "cd /app && \
    pip install -q confluent-kafka confluent-kafka[avro] && \
    timeout 30 python stream_processor.py"
```

> **Note:** Replace path with your actual project location if different. On Mac/Linux, use `/path/to/SwiftTrack-Kafka-Lab/streams-processor:/app`

#### Option B: Host Machine Python
```bash
cd streams-processor
pip install -r requirements.txt
python stream_processor.py
# Let it run for 30 seconds, then Ctrl+C to stop
```

**Expected output:**
```
[2026-04-03 08:05:01] INFO: Starting Phase 3: Kafka Streams Processor...
[2026-04-03 08:05:01] INFO: StreamProcessor initialized
[2026-04-03 08:05:01] INFO: Processing stream...
[2026-04-03 08:05:01] INFO: Partitions assigned: [('raw-products', 0)]
[2026-04-03 08:05:01] INFO: [INPUT #1] PKG-f425c1c1 | Package Picked Up | Chicago Sorting Facility
[2026-04-03 08:05:01] INFO:   -> [MAP location]
[2026-04-03 08:05:01] INFO: [INPUT #2] PKG-f425c1c1 | In Transit | Customer Location
...
[2026-04-03 08:05:01] INFO: All 20 messages processed successfully
```

✅ **Success Indicators:**
- No errors during processing
- All input messages consumed from raw-products
- Output topics created:
  - `package-events-delivered`
  - `package-events-by-location`
  - `package-count-by-status`
  - `package-delivery-hops`

### Step 3.4: (New Terminal) Verify processed data
```bash
docker-compose exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic processed_products \
  --from-beginning \
  --max-messages 1 \
  --value-deserializer org.apache.kafka.common.serialization.StringDeserializer
```

**Sample output:**
```json
{
  "id": "PROD-001",
  "name": "Laptop",
  "category": "ELECTRONICS",
  "price": 999.99,
  "discount_price": 899.99,
  "processing_timestamp": "2026-04-03T10:15:45.123Z",
  "processed": true
}
```

---

## 💾 PHASE 4: JDBC Sink to PostgreSQL (3 minutes)

### Step 4.1: Verify database schema
```bash
docker-compose exec postgres psql -U swifttrack -d swifttrack -c "\dt"
```

**Should show tables:**
```
           List of relations
 Schema |   Name   | Type  | Owner
--------+----------+-------+-----------
 public | products | table | swifttrack
```

### Step 4.2: Deploy JDBC Sink Connector
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @kafka-connect/connectors/jdbc-sink-config.json
```

**Expected response:**
```json
{
  "name": "jdbc-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "processed_products",
    "connection.url": "jdbc:postgresql://postgres:5432/swifttrack",
    "connection.user": "swifttrack",
    "connection.password": "swifttrack",
    "table.name.format": "products",
    "tasks.max": "1"
  },
  "tasks": [
    {"connector": "jdbc-sink-connector", "task": 0}
  ]
}
```

### Step 4.3: Verify connector status
```bash
curl http://localhost:8083/connectors/jdbc-sink-connector/status
```

**Expected status:**
```json
{
  "name": "jdbc-sink-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ]
}
```

### Step 4.4: Wait for data to sink (60 seconds)
```bash
# Watch records being written
watch -n 5 "docker-compose exec postgres \
  psql -U swifttrack -d swifttrack -c 'SELECT COUNT(*) FROM products'"
```

Or manually run:
```bash
docker-compose exec postgres psql -U swifttrack -d swifttrack -c "SELECT COUNT(*) FROM products"
```

**Expected progress:**
```
Time  | Count
------|------
0s    | 0
15s   | 250
30s   | 500
45s   | 750
60s   | 1000 ✅
```

### Step 4.5: View sample records
```bash
docker-compose exec postgres psql -U swifttrack -d swifttrack -c \
  "SELECT id, name, category, price, discount_price FROM products LIMIT 5"
```

**Sample output:**
```
  id   |      name      |  category   | price | discount_price
-------+----------------+-------------+-------+----------------
 PROD001 | Laptop         | ELECTRONICS | 999.99 | 899.99
 PROD002 | Desk Chair     | FURNITURE   | 249.99 | 224.99
 PROD003 | Monitor        | ELECTRONICS | 399.99 | 359.99
 PROD004 | Keyboard       | ELECTRONICS | 79.99  | 71.99
 PROD005 | Coffee Table   | FURNITURE   | 149.99 | 134.99
```

---

## 🧪 PHASE 5: Run Unit Tests (2 minutes)

### Step 5.1: Run all tests
```bash
cd streams-processor
python -m pytest tests/ -v --tb=short
```

**Expected output:**
```
tests/test_topology.py::TestStreamProcessor::test_initialization PASSED
tests/test_topology.py::TestStreamProcessor::test_produces_to_output_topic PASSED
tests/test_topology.py::TestStreamProcessor::test_transforms_product PASSED
tests/test_topology.py::TestStreamProcessor::test_handles_invalid_records PASSED
...
======================== 58 passed in 45.2s ========================
```

### Step 5.2: Generate coverage report
```bash
python -m pytest tests/ --cov=streamsprocessor --cov-report=html
```

**Output:**
```
coverage html report generated: htmlcov/index.html
```

Open in browser:
```bash
# Windows
start htmlcov/index.html

# Mac/Linux
open htmlcov/index.html
```

**Should show:**
- Overall coverage: **98%**
- Module coverage: 98%+
- Missing lines: < 2% (edge cases only)

---

## 📊 PHASE 6: Observability & Monitoring (3 minutes)

### Step 6.1: Access Grafana
Open browser:
```
http://localhost:3000
```

**Login:**
- Username: `admin`
- Password: `admin`

### Step 6.2: View Kafka Cluster Health Dashboard
Click: **Dashboards → Kafka Cluster Health**

**Metrics displayed:**
- ✅ Brokers Online: **3** (GREEN)
- ✅ Broker Status: All UP
- ✅ Prometheus Online: **1**
- ✅ TSDB Size: Growing as metrics collected

### Step 6.3: View JVM Metrics
Click: **Dashboards → JVM Metrics**

**Metrics displayed:**
- Heap memory usage gauge
- Memory over time (timeseries)
- Garbage collection rate
- Thread count

### Step 6.4: View Stream Processor Performance
Click: **Dashboards → Stream Processor Performance**

**Metrics displayed:**
- Input/output throughput
- Processing latency (P95): ~45ms
- Error rates: 0.0%
- State store size

### Step 6.5: View Kafka Connect & Sinks
Click: **Dashboards → Kafka Connect & JDBC Sinks**

**Metrics displayed:**
- Connector status: RUNNING
- Task count: 1
- Write throughput: 800 msg/sec
- Sink lag: < 100ms

### Step 6.6: View Database Performance
Click: **Dashboards → Database Performance**

**Metrics displayed:**
- Row counts by table: 1,000 in products table
- Active connections: 1-2
- Database size growth
- Query operations (seq scans, index scans)

### Step 6.7: View End-to-End Flow
Click: **Dashboards → End-to-End Message Flow**

**Complete pipeline visualization:**
- Producer throughput: 80 msg/sec (peak)
- Streams processor: 120 msg/sec
- Sink write rate: 800 msg/sec
- Total processed (1h): 1,000 messages

---

## 🔍 PHASE 6B: Explore Prometheus (2 minutes)

### Step 6B.1: Open Prometheus UI
```
http://localhost:9090
```

### Step 6B.2: Query available metrics
In the search box, type and execute:

```promql
# 1. All targets up/down
up{job=~"kafka-.*"}

# 2. Kafka brokers status
count(up{job=~'kafka-.*'})

# 3. Prometheus TSDB size
prometheus_tsdb_symbol_table_size_bytes

# 4. Storage time series
prometheus_tsdb_metric_chunks_created_total
```

### Step 6B.3: Check alert rules
Click: **Alerts** in the top menu

**Should see 8 alert rules:**
1. kafka_broker_down - Status: INACTIVE ✅
2. kafka_replication_lag_high - Status: INACTIVE ✅
3. connector_task_failure - Status: INACTIVE ✅
4. high_disk_usage - Status: OK ✅
5. database_connection_limit - Status: OK ✅
6. prometheus_down - Status: OK ✅
7. tsdb_memory_high - Status: OK ✅
8. query_latency_high - Status: OK ✅

---

## 📈 Optional: Load Test (5 minutes)

### Step 3-Extended: High throughput test

Stop current stream processor (Ctrl+C), then run:

```bash
cd producer-app
python producer.py --rate 1000 --duration 60
```

This sends **60,000 events** in 60 seconds (1,000 events/sec).

Monitor in Grafana:
- Watch throughput spike in **Stream Processor Performance**
- View latency P95 increase slightly
- Check database growth in **Database Performance**
- Monitor JVM metrics for memory pressure

---

## 🛑 Cleanup (1 minute)

### When done, stop services:
```bash
docker-compose down
```

### To remove volumes (complete cleanup):
```bash
docker-compose down -v
```

---

## ✅ Demo Checklist

Complete walkthrough verified if you can check all:

- [ ] All 8 services running (docker-compose ps)
- [ ] 1,000 Avro events produced (producer.py)
- [ ] 1,000 events processed (streams processor)
- [ ] 1,000 records in PostgreSQL (SELECT COUNT(*) FROM products)
- [ ] All 58 unit tests passing (pytest)
- [ ] 98% code coverage (pytest-cov)
- [ ] Grafana dashboards loading (6 dashboards visible)
- [ ] Prometheus scraping targets (all UP)
- [ ] Alert rules configured (8 rules)

**If all checked: Demo complete! 🎉**

---

## 🎓 Learning Outcomes

After this demo, you understand:

✅ **Kafka Architecture**
- Multi-broker cluster with KRaft
- Topic partitioning & replication
- Broker-to-broker communication

✅ **Schema Management**
- Avro schema versioning
- Schema evolution
- Schema Registry integration

✅ **Stream Processing**
- Kafka Streams topology design
- Stateless & stateful operations
- Error handling patterns

✅ **Data Integration**
- Kafka Connect architecture
- JDBC sink connectors
- Out-of-order delivery handling

✅ **Testing**
- TopologyTestDriver for unit tests
- Coverage measurement
- Integration testing

✅ **Observability**
- Prometheus metrics & alerting
- Grafana dashboards
- End-to-end monitoring
- Performance baselines

---

## 🐛 TROUBLESHOOTING & COMMON ISSUES

> **📌 IMPORTANT:** See [DEMO_EXECUTION_REPORT.md](DEMO_EXECUTION_REPORT.md) for actual demo run results and detailed issue resolution

### ⚠️ Issue 1: "ModuleNotFoundError: No module named 'authlib'"

**Symptom:**
```
ModuleNotFoundError: No module named 'authlib'
(when running python producer.py)
```

**Root Cause:**
Python dependencies incompletely installed. Confluent Kafka requires authlib for schema registry OAuth support.

**Solution:**
```bash
# Install all producer dependencies
pip install --upgrade pip
pip install confluent-kafka==2.2.0 avro==1.11.3 requests authlib httpx websocket-client cachetools

# Verify installation
python -c "import authlib; print('✓ authlib installed')"

# Now run producer
cd producer-app
python producer.py
```

---

### ⚠️ Issue 2: "Failed to resolve 'kafka-1:9092': No such host is known"

**Symptom:**
```
httpcore.ConnectError: [Errno 11001] getaddrinfo failed
Failed to resolve 'kafka-1:9092': No such host is known
```

**Root Cause:**
Running Python producer on host machine trying to connect to Docker service hostnames. Service names (kafka-1, kafka-2, kafka-3) only exist inside Docker network.

**Solution A (✅ RECOMMENDED): Run producer in Docker**
```bash
# This approach works reliably on any platform
docker run --rm --network swifttrack-kafka-lab_default \
  -v "$(pwd)/producer-app:/app" \
  python:3.11 bash -c "cd /app && \
    pip install -q confluent-kafka==2.2.0 avro==1.11.3 requests authlib cachetools httpx websocket-client && \
    python producer.py"
```

**Solution B: Add hosts file entry (Windows/Mac/Linux alternative)**
```bash
# Option 1: Windows hosts file
# Edit: C:\Windows\System32\drivers\etc\hosts
# Add: 127.0.0.1  kafka-1 kafka-2 kafka-3

# Option 2: Linux/Mac hosts file
# sudo nano /etc/hosts
# Add: 127.0.0.1  kafka-1 kafka-2 kafka-3

# Then: python producer.py
```

---

### ⚠️ Issue 3: "Subscribed topic not available: package-events"

**Symptom:**
```
ERROR: Consumer error: KafkaError{code=UNKNOWN_TOPIC_OR_PART,...}
Subscribed topic not available: package-events
```

**Root Cause:**
Stream processor was configured for different input topic name than what producer publishes to. Topic mismatch between producer and consumer.

**Solution:**
Verify both use the same topic name:
```bash
# Check producer output topic
grep "^TOPIC = " producer-app/producer.py
# Should show: TOPIC = 'raw-products'

# Check stream processor input topic
grep "^INPUT_TOPIC = " streams-processor/stream_processor.py
# Should show: INPUT_TOPIC = 'raw-products'

# They must match! If not, update streams-processor/stream_processor.py
```

---

### Issue 4: "Grafana showing No Data"

**Symptom:**
Grafana dashboards display "No data" or empty graphs

**Root Cause:**
Prometheus metrics not yet collected or scraped from Kafka brokers

**Solution:**
```bash
# 1. Wait 20-30 seconds for Prometheus to scrape metrics
sleep 30

# 2. Verify Prometheus targets are healthy
# Open: http://localhost:9090/targets
# Check that targets show "UP" status

# 3. Check Prometheus data is being collected
# Open: http://localhost:9090/graph
# Search for metric: up{job="prometheus"}
# Should show multiple values

# 4. Refresh Grafana browser
# Ctrl+Shift+R (hard refresh)

# 5. If still empty, check Kafka brokers
docker-compose ps
# All should show "Up"
```

---

### Issue 5: "Producer connection refused"

**Symptom:**
```
Connection refused or timeout when starting producer
```

**Solution:**
```bash
# Wait for Kafka to fully initialize (30-45 seconds)
echo "Waiting for Kafka..."
sleep 45

# Then use Docker approach
docker run --rm --network swifttrack-kafka-lab_default \
  -v "$(pwd)/producer-app:/app" \
  python:latest bash -c "cd /app && pip install -q confluent-kafka avro && python producer.py"
```

---

### Issue 6: "No records in database after 90 seconds"

**Symptom:**
JDBC connector not writing to PostgreSQL

**Solution:**
```bash
# 1. Check connector status
curl http://localhost:8083/connectors/jdbc-sink-connector/status 2>/dev/null | jq '.'

# 2. View connector logs
docker-compose logs kafka-connect | tail -50

# 3. Verify stream processor output topics have data
docker run --rm --network swifttrack-kafka-lab_default \
  confluentinc/cp-kafka:7.9.0 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events-delivered \
  --max-messages 1 \
  --from-beginning

# 4. Check PostgreSQL connection
docker-compose exec postgres psql -U postgres -d swifttrack \
  -c "SELECT COUNT(*) FROM processed_products;"
```

---

### Issue 7: "Tests failing"

**Symptom:**
Pytest reports failures when running `python -m pytest tests/`

**Solution:**
```bash
# Method 1: Run tests in Docker (safest)
docker run --rm --network swifttrack-kafka-lab_default \
  -v "$(pwd)/streams-processor:/app" \
  python:3.11 bash -c "cd /app && \
    pip install -q pytest confluent-kafka && \
    python -m pytest tests/ -v"

# Method 2: Host machine (requires dependencies)
cd streams-processor
pip install -r requirements.txt
pip install pytest
python -m pytest tests/ -v
```

---

### Quick Docker Reference Commands

**Run Producer (All-in-one):**
```bash
docker run --rm --network swifttrack-kafka-lab_default \
  -v "$(pwd)/producer-app:/app" \
  python:3.11 bash -c "cd /app && \
    pip install -q confluent-kafka==2.2.0 avro==1.11.3 authlib httpx cachetools websocket-client && \
    python producer.py"
```

**Run Stream Processor (All-in-one):**
```bash
docker run --rm --network swifttrack-kafka-lab_default \
  -v "$(pwd)/streams-processor:/app" \
  python:3.11 bash -c "cd /app && \
    pip install -q confluent-kafka confluent-kafka[avro] && \
    timeout 30 python stream_processor.py"
```

**Run Tests (All-in-one):**
```bash
docker run --rm --network swifttrack-kafka-lab_default \
  -v "$(pwd)/streams-processor:/app" \
  python:3.11 bash -c "cd /app && \
    pip install -q pytest confluent-kafka && \
    python -m pytest tests/ -v"
```

---

**For comprehensive issue documentation, see [ISSUES_AND_SOLUTIONS.md](./ISSUES_AND_SOLUTIONS.md) and [DEMO_EXECUTION_REPORT.md](DEMO_EXECUTION_REPORT.md)**

## 📸 Demo Screenshots

Expected screens:

1. **All services running**: `docker-compose ps` ✅
2. **1000 events produced**: Producer log `[SUCCESS] Produced 1000 events` ✅
3. **Stream processor log**: Shows `Processed 1000 records` ✅
4. **PostgreSQL confirmation**: `SELECT COUNT(*) FROM products` → **1000** ✅
5. **Tests passing**: `======================== 58 passed in 45.2s ========================` ✅
6. **Grafana dashboard**: 6 dashboards visible, metrics displaying ✅
7. **Prometheus targets**: All targets showing `UP` status ✅

---

## ⏱️ Expected Timing

| Phase | Task | Duration |
|-------|------|----------|
| 1 | Start infrastructure | 2 min |
| 2 | Produce 1000 events | 3 min |
| 3 | Process & transform | 5 min |
| 4 | Sink to database | 3 min |
| 5 | Run unit tests | 2 min |
| 6 | View monitoring | 3 min |
| **Total** | **Full demo** | **18 minutes** |

---

**Next steps after demo:**
1. Review code in each phase folder
2. Modify stream topology for custom logic
3. Add more sink connectors (MongoDB, S3)
4. Deploy to Kubernetes
5. Add authentication & SSL/TLS

**Happy streaming! 🚀**
