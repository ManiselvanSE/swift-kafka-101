# 🚀 SwiftTrack Kafka Lab - Complete End-to-End Kafka Ecosystem

A **comprehensive, production-ready Kafka lab** demonstrating all phases of a modern data pipeline: **Producer → Streams Processing → Connectors → Sinks → Observability**.

> **Documentation navigation:** See [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) for a task-oriented guide to the project docs, deployment steps, phase reports, and troubleshooting.

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
├── DEMO.md                  # Step-by-step demo walkthrough
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
- Docker & Docker Com