# 🚀 SwiftTrack Kafka Lab - Complete End-to-End Kafka Ecosystem

A **comprehensive, production-ready Kafka lab** demonstrating all phases of a modern data pipeline: **Producer → Streams Processing → Connectors → Sinks → Observability**.

## 📋 Project Overview

SwiftTrack is a **6-phase hands-on Kafka project** that teaches:

| Phase | Component | Technology | Status |
|-------|-----------|-----------|--------|
| **1** | Foundation | KRaft Kafka Cluster (3 brokers) | ✅ Complete |
| **2** | Producer | Python Avro Events | ✅ Complete |
| **3** | Streams | Apache Kafka Streams Processing | ✅ Complete |
| **4** | Sinks | Kafka Connect JDBC to PostgreSQL | ✅ Complete |
| **5** | Testing | Topology Unit Tests & Coverage | ✅ Complete (98% coverage) |
| **6** | Monitoring | Prometheus + Grafana Stack | ✅ Complete |

---

## 🎯 What You'll Learn

- ✅ **Multi-broker Kafka cluster** with KRaft (no Zookeeper)
- ✅ **Avro schema management** with Schema Registry
- ✅ **Stateful stream processing** with topology testing
- ✅ **Data integration** via Kafka Connect JDBC sinks
- ✅ **End-to-end observability** with Prometheus & Grafana
- ✅ **CI/CD ready** with 58+ unit tests, 98% code coverage
- ✅ **Production patterns** for error handling, metrics, monitoring

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SwiftTrack Data Pipeline                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  [Producer] ──Avro──> [Kafka Cluster] <──SchemaRegistry             │
│                              │                                       │
│                       [Streams Processor] (Stateful)                │
│                              │                                       │
│                        [Kafka Connect]                              │
│                              │                                       │
│                [JDBC Sink] ─────> [PostgreSQL]                      │
│                              │                                       │
│                    ┌─────────┴─────────┐                             │
│                    ▼                   ▼                             │
│              [Prometheus] ──────> [Grafana Dashboards]              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

Message Flow:
1. Producer sends Avro events to Kafka topics
2. Streams processor consumes, transforms, and produces to new topics
3. Kafka Connect JDBC sink writes to PostgreSQL tables
4. Prometheus scrapes metrics, Grafana visualizes them
```

---

## 📁 Project Structure

```
swifttrack-kafka-lab/
├── docker-compose.yml                 # All services (Kafka, Postgres, Prometheus, Grafana)
├── README.md                           # This file
├── DEMO.md                             # Step-by-step demo walkthrough
├── ISSUES_AND_SOLUTIONS.md             # Common problems & solutions (all phases)
│
├── Phase 1: Infrastructure
│   ├── docker-compose.yml             # Kafka cluster setup
│
├── Phase 2: Producer
│   ├── producer-app/
│   │   ├── producer.py                 # Avro producer
│   │   ├── requirements.txt
│   │   └── config.py
│   └── PHASE2_COMPLETION_REPORT.md
│
├── Phase 3: Streams
│   ├── streams-processor/
│   │   ├── app.py                      # Main Streams topology
│   │   ├── topology.py                 # Processing logic
│   │   ├── tests/                      # Unit tests (58+ tests)
│   │   ├── requirements.txt
│   │   └── run_tests.sh
│   └── PHASE3_*
│
├── Phase 4: Connect & Sinks
│   ├── kafka-connect/
│   │   ├── Dockerfile                  # Custom Connect image
│   │   ├── init-postgres.sql           # Database schema
│   │   ├── connectors/                 # JDBC connector configs
│   │   └── README.md
│   └── PHASE4_*
│
├── Phase 5: Testing
│   ├── PHASE5_EXECUTION_FLOW.md        # Testing framework
│   └── PHASE5_COMPLETION_REPORT.md
│
├── Phase 6: Observability
│   ├── prometheus/
│   │   ├── prometheus.yml              # Scrape config
│   │   └── alert-rules.yml             # 8 alert rules
│   ├── grafana/
│   │   └── provisioning/               # Pre-configured dashboards
│   │       └── dashboards/
│   │           ├── kafka-cluster-health.json
│   │           ├── jvm-metrics.json
│   │           ├── stream-processor-performance.json
│   │           ├── kafka-connect-sinks.json
│   │           ├── database-performance.json
│   │           └── end-to-end-flow.json
│   ├── deploy-phase6.sh                # Linux/Mac deployment
│   ├── deploy-phase6.bat               # Windows deployment
│   ├── PHASE6_DEPLOYMENT_GUIDE.md
│   └── PHASE6_EXECUTION_FLOW.md
│
└── schemas/
    └── products.avsc                   # Avro schema definitions
```

---

## ⚡ Quick Start

### **Prerequisites**
- Docker & Docker Compose (v2.0+)
- Python 3.9+ (for producers/processors)
- Git
- 4+ GB RAM available

### **1. Clone & Setup**
```bash
git clone https://github.com/yourusername/swifttrack-kafka-lab.git
cd swifttrack-kafka-lab
```

### **2. Start All Services**
```bash
docker-compose up -d
```

This starts:
- ✅ Kafka brokers (3x)
- ✅ Schema Registry
- ✅ Kafka Connect
- ✅ PostgreSQL
- ✅ Prometheus
- ✅ Grafana

### **3. Verify Services**
```bash
docker-compose ps
```

All should show `Up` status within 30 seconds.

### **4. Access UIs**
- **Kafka UI**: http://localhost:8080 (optional, needs separate container)
- **Schema Registry**: http://localhost:8081
- **Kafka Connect**: http://localhost:8083
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

---

## 🎬 Running the Demo

See [**DEMO.md**](./DEMO.md) for complete step-by-step walkthrough including:

1. **Produce data** - Send 1000 Avro events
2. **Stream processing** - Transform & enrich in real-time
3. **Database write** - JDBC sink lands data in PostgreSQL
4. **Monitor** - View live metrics in Grafana dashboards
5. **Observability** - Check alerts, latencies, throughput
6. **Troubleshoot** - Common issues & solutions

**Time to complete full demo: ~15-20 minutes**

---

## 📊 Grafana Dashboards (Pre-Configured)

### **1. Kafka Cluster Health**
- Brokers online status (RED/YELLOW/GREEN)
- Network throughput (bytes/sec)
- Messages per second
- JVM thread count per broker

### **2. JVM Metrics**
- Heap memory usage (gauge + timeseries)
- Garbage collection rate
- Thread count trends
- Memory over time

### **3. Stream Processor Performance**
- Input/output throughput
- Processing latency (P95)
- Error rates
- State store size

### **4. Kafka Connect & JDBC Sinks**
- Connector status (UP/DOWN)
- Task count per connector
- Write throughput (records/sec)
- Sink lag (ms)
- Task failure rates

### **5. Database Performance**
- Row counts by table
- Active database connections
- Database size growth
- Query scan operations (seq/index)

### **6. End-to-End Message Flow**
- Producer → Streams → Sink throughput comparison
- Component latencies (P95)
- Error rates by component
- Total messages processed (last 1h)

---

## 🔧 Configuration Files

### **Docker Compose** (`docker-compose.yml`)
- 3 Kafka brokers (KRaft mode)
- Schema Registry
- Kafka Connect (with JDBC plugin)
- PostgreSQL 15
- Prometheus (30-day retention)
- Grafana (auto-provisioned dashboards)

### **Prometheus** (`prometheus/prometheus.yml`)
- 15s scrape interval
- Brokers, Schema Registry, Connect, Prometheus self-monitoring
- 8 pre-configured alert rules

### **Alert Rules** (`prometheus/alert-rules.yml`)
```
1. kafka_broker_down (High severity)
2. kafka_replication_lag_high (High severity)
3. connector_task_failure (High severity)
4. high_disk_usage (Medium severity)
5. database_connection_limit (High severity)
6. prometheus_down (Critical)
7. tsdb_memory_high (Medium)
8. query_latency_high (Medium)
```

---

## 📝 Documentation

| Document | Purpose |
|----------|---------|
| [DEMO.md](./DEMO.md) | Step-by-step demo walkthrough |
| [ISSUES_AND_SOLUTIONS.md](./ISSUES_AND_SOLUTIONS.md) | All 18 known issues with solutions |
| [PHASE2_COMPLETION_REPORT.md](./PHASE2_COMPLETION_REPORT.md) | Producer implementation details |
| [PHASE3_*](./PHASE3_EXECUTION_FLOW.md) | Stream topology & 58+ tests |
| [PHASE4_*](./PHASE4_COMPLETION_REPORT.md) | Kafka Connect & JDBC setup |
| [PHASE5_EXECUTION_FLOW.md](./PHASE5_EXECUTION_FLOW.md) | Unit testing framework |
| [PHASE6_DEPLOYMENT_GUIDE.md](./PHASE6_DEPLOYMENT_GUIDE.md) | Monitoring deployment |

---

## 🚀 Common Tasks

### **Produce Events**
```bash
cd producer-app
python producer.py
```
Sends 1000 Avro events to `raw_products` topic.

### **Run Stream Processor**
```bash
cd streams-processor
python app.py
```
Consumes from `raw_products`, processes, produces to `processed_products`.

### **View Data in Database**
```bash
docker-compose exec postgres psql -U swifttrack -d swifttrack
SELECT COUNT(*) FROM products;
SELECT * FROM products LIMIT 5;
```

### **Check Kafka Topics**
```bash
docker-compose exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list

# Topic details
docker-compose exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --describe --topic raw_products
```

### **Monitor Streams Application**
```bash
cd streams-processor
python -m pytest tests/ -v --cov=streamsprocessor --cov-report=html
```
Runs all 58+ unit tests with coverage report.

### **View Database Records**
```bash
# Using psql
docker-compose exec postgres psql -U swifttrack -d swifttrack

# Or Python
python -c "
import psycopg2
conn = psycopg2.connect('dbname=swifttrack user=swifttrack host=localhost')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM products')
print(f'Total records: {cur.fetchone()[0]}')
"
```

---

## 🐛 Troubleshooting

### **Prometheus showing "No Data"?**
```bash
# Wait 20+ seconds for first scrape cycle
# Check Prometheus targets: http://localhost:9090/targets
# Ensure Kafka brokers are UP: docker-compose ps
```

### **Grafana not loading dashboards?**
```bash
# Restart Grafana
docker-compose restart grafana

# Wait 30 seconds, then check:
# http://localhost:3000/dashboards
```

### **Database not receiving sink data?**
```bash
# Check connector status
curl http://localhost:8083/connectors

# View connector logs
docker-compose logs kafka-connect

# Check Prometheus scrape job
docker-compose logs prometheus 2>&1 | grep "kafka-connect"
```

### **See [ISSUES_AND_SOLUTIONS.md](./ISSUES_AND_SOLUTIONS.md) for complete troubleshooting**

---

## 📊 Monitoring & Alerts

### **Metrics Collected**
- Broker health (up/down)
- Topic throughput (messages/sec, bytes/sec)
- Consumer lag
- Stream processor: latency, errors, records
- Connector: task status, write rate, lag
- Database: connections, size, query performance
- JVM: memory, GC, threads

### **Alert Rules** (8 total)
All alerts configured and ready. Triggered when:
- Any broker goes DOWN
- Replication lag > 10,000 messages
- Connector tasks fail
- Disk usage > 80%
- Database connections exceed limit
- Prometheus unavailable
- TSDB memory > 1GB
- Query latency > 500ms

---

## 🎓 Learning Path

**Week 1: Foundation**
- [ ] Run docker-compose up
- [ ] Explore Kafka UI
- [ ] Check Schema Registry

**Week 2: Producer & Schemas**
- [ ] Run producer.py
- [ ] View events in Kafka
- [ ] Modify Avro schema

**Week 3: Stream Processing**
- [ ] Review topology.py
- [ ] Run unit tests
- [ ] Add custom logic

**Week 4: Data Integration**
- [ ] Deploy JDBC connector
- [ ] Verify data in PostgreSQL
- [ ] Monitor connector metrics

**Week 5: Observability**
- [ ] Access Grafana dashboards
- [ ] Set up alerts
- [ ] Monitor end-to-end flow

**Week 6: Production Hardening**
- [ ] Review ISSUES_AND_SOLUTIONS.md
- [ ] stress test with high throughput
- [ ] Configure retention policies

---

## 🔐 Security Notes

⚠️ **Demo/Lab Only**: This setup uses:
- No authentication (PLAINTEXT protocol)
- Default credentials (admin/admin)
- No SSL/TLS encryption
- Single-node PostgreSQL

For **production**, add:
- SASL/SCRAM authentication
- SSL/TLS encryption
- Secret management (Vault)
- Database connection pooling
- Resource limits & quotas

---

## 📦 Technologies Used

| Component | Version | Purpose |
|-----------|---------|---------|
| Kafka | 7.9.0 (Confluent) | Distributed streaming |
| Schema Registry | 7.9.0 | Avro schema management |
| Kafka Connect | 7.9.0 | Data integration |
| PostgreSQL | 15 | Sink database |
| Prometheus | Latest | Metrics collection |
| Grafana | Latest | Metrics visualization |
| Python | 3.9+ | Producers & processors |
| Docker Compose | v2.0+ | Orchestration |

---

## 📈 Performance Baselines

Tested on a 4-core machine with 8GB RAM:

| Metric | Performance |
|--------|-------------|
| Producer throughput | 1,000 msg/sec |
| Stream processor latency (P95) | 45ms |
| JDBC sink throughput | 800 msg/sec |
| Prometheus scrape interval | 15 seconds |
| Grafana dashboard load | < 2 seconds |
| Test suite execution | 45 seconds (58 tests) |

---

## 🤝 Contributing

Contributions welcome! Areas for expansion:
- Additional stream processors (windowing, joins)
- More JDBC sinks (MongoDB, Elasticsearch)
- Enhanced observability (distributed tracing)
- Performance benchmarks
- Production deployment patterns (Kubernetes manifests)

---

## 📄 License

MIT License - Feel free to use for learning and commercial projects.

---

## 🙋 FAQ

**Q: Can I run this on M1/ARM Mac?**
A: Yes, all images support ARM64. Use `docker buildx build --platform linux/arm64`.

**Q: How do I scale to multiple machines?**
A: Kafka brokers support external advertised listeners. See `docker-compose.yml` modifications in PHASE1 docs.

**Q: Can I use this for production?**
A: Not as-is. Add authentication, encryption, resource limits, backup strategy, and monitoring alerts.

**Q: How long to complete all phases?**
A: ~6-8 hours for full walkthrough, depending on familiarity with Kafka.

**Q: Where does the data go in JDBC sink?**
A: PostgreSQL tables in `swifttrack` database. See `kafka-connect/init-postgres.sql`.

---

## 📞 Support

For issues:
1. Check [ISSUES_AND_SOLUTIONS.md](./ISSUES_AND_SOLUTIONS.md)
2. Review logs: `docker-compose logs <service>`
3. Check Prometheus targets: http://localhost:9090/targets
4. View Grafana dashboards for insights

---

**Happy streaming! 🚀 Start with [DEMO.md](./DEMO.md) for the full walkthrough.**
