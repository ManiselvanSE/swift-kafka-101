# Phase 6 Execution Flow: Observability & Monitoring

**Phase**: 6/6 (Final)  
**Status**: Implementation Starting  
**Date**: 2026-04-03

---

## Phase 6 Overview

Phase 6 adds comprehensive observability to the SwiftTrack Kafka Lab using **Prometheus** for metrics collection and **Grafana** for visualization. This enables real-time monitoring of:

- Kafka cluster health (brokers, topics, partitions)
- Stream processor performance (throughput, latency, errors)
- Kafka Connect health (connector status, tasks, failures)
- PostgreSQL database metrics (write latency, connection count)
- End-to-end pipeline latency (source to database)

### Why Observability Matters

| Aspect | Benefit |
|--------|---------|
| **Visibility** | See what's happening in real-time |
| **Debugging** | Correlate events with metrics to find root causes |
| **Performance** | Identify bottlenecks before they become problems |
| **Alerting** | Get notified of issues automatically |
| **Trending** | Understand performance over time |

---

## 6-Step Execution Flow

### Step 1️⃣: Understand Monitoring Architecture

The monitoring stack consists of:

```
┌─────────────────────────────────────────────────────────────┐
│                    OBSERVABILITY LAYER                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  Prometheus  │  │   Grafana    │  │  AlertManager│    │
│  │  (Metrics)   │  │  (Dashboard) │  │  (Alerts)    │    │
│  └──────────────┘  └──────────────┘  └──────────────┘    │
│         ↑                ↑                   ↑              │
│         └────────────────┴───────────────────┘              │
│                        ↓                                     │
├─────────────────────────────────────────────────────────────┤
│                  METRICS EXPORTERS                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ JMX Exporter │  │ Node Exporter │  │PostgreSQL    │    │
│  │ (Kafka)      │  │ (Host metrics)│  │ Exporter     │    │
│  └──────────────┘  └──────────────┘  └──────────────┘    │
│         ↓                ↓                   ↓              │
├─────────────────────────────────────────────────────────────┤
│                    DATA SOURCES                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │   Kafka      │  │   Stream     │  │  PostgreSQL  │    │
│  │  Cluster     │  │  Processor   │  │   Database   │    │
│  └──────────────┘  └──────────────┘  └──────────────┘    │
│         ↓                ↓                   ↓              │
├─────────────────────────────────────────────────────────────┤
│                   APPLICATION LAYER                         │
└─────────────────────────────────────────────────────────────┘
```

**Key Components:**

1. **Prometheus** (port 9090)
   - Time-series database for metrics
   - Scrapes metrics from exporters every 15 seconds
   - Stores 15 days of metrics

2. **Grafana** (port 3000)
   - Web UI for dashboards
   - Queries Prometheus for visualization
   - Provides alerting UI

3. **JMX Exporter** (port 5556)
   - Exports Kafka broker metrics
   - Metrics: brokers, topics, partitions, replication lag

4. **Node Exporter** (port 9100)
   - Exports host OS metrics
   - Metrics: CPU, memory, disk, network

5. **PostgreSQL Exporter** (port 9187)
   - Exports database metrics
   - Metrics: connection count, write latency, table sizes

---

### Step 2️⃣: Set Up Infrastructure

#### 2.1 Create Prometheus Configuration

**File**: `prometheus/prometheus.yml`

```yaml
global:
  scrape_interval: 15s  # How often to scrape metrics
  evaluation_interval: 15s
  external_labels:
    monitor: 'SwiftTrack'

# Prometheus itself
scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # Kafka metrics via JMX Exporter
  - job_name: 'kafka'
    static_configs:
      - targets:
          - kafka-1:5556
          - kafka-2:5556
          - kafka-3:5556
        labels:
          group: 'brokers'

  # Host metrics via Node Exporter
  - job_name: 'node'
    static_configs:
      - targets:
          - node-exporter:9100
        labels:
          group: 'hosts'

  # PostgreSQL metrics
  - job_name: 'postgres'
    static_configs:
      - targets:
          - postgres-exporter:9187
        labels:
          group: 'databases'
```

#### 2.2 Create Grafana Datasource Configuration

**File**: `grafana/provisioning/datasources/prometheus.yml`

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
```

#### 2.3 Update docker-compose.yml

Add services:
- `prometheus` - metrics collection
- `grafana` - dashboards
- `jmx-exporter` - Kafka metrics
- `node-exporter` - host metrics
- `postgres-exporter` - database metrics

---

### Step 3️⃣: Deploy Monitoring Stack

**Execution Command:**

```bash
# Build and start monitoring services
docker-compose -f docker-compose.yml up -d prometheus grafana jmx-exporter node-exporter postgres-exporter

# Verify all services are healthy
docker-compose ps

# Expected output:
# prometheus        Up (healthy)
# grafana          Up (healthy)
# jmx-exporter     Up (healthy)
# node-exporter    Up (healthy)
# postgres-exporter Up (healthy)
```

**Verification:**

```bash
# Check Prometheus can reach targets
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets | length'
# Expected: 10+ targets

# Check Grafana is responsive
curl -s http://localhost:3000/api/health | jq '.database'
# Expected: "ok"

# Check Kafka metrics are being scraped
curl -s http://localhost:9090/api/v1/query?query=kafka_server_brokerlogmanager_offlinelogsperbroker
# Expected: metric data visible
```

---

### Step 4️⃣: Create Monitoring Dashboards

#### 4.1 Kafka Cluster Overview Dashboard

Metrics to display:
- Node count (expected: 3)
- Topic count
- Partition distribution
- Replication lag
- Controller status

#### 4.2 Stream Processor Dashboard

Metrics to display:
- Input topic message rate
- Output topic message rate
- Processing latency (p50, p99)
- State store size
- Error rate

#### 4.3 Kafka Connect Dashboard

Metrics to display:
- Connector status (RUNNING/FAILED/PAUSED)
- Task count per connector
- Record lag monitor
- Connector restarts
- Task failures

#### 4.4 Database Dashboard

Metrics to display:
- Write latency
- Connection count
- Table sizes
- Transaction rate
- Lock wait time

#### 4.5 End-to-End Dashboard

Metrics to display:
- Latency from producer to database (total)
- Producer → Kafka latency
- Kafka → Processor latency
- Processor → Database latency
- Queue depth at each stage

---

### Step 5️⃣: Set Up Alerting Rules

**Alerting Rules** (`prometheus/alerts.yml`):

#### Alert 1: Broker Down
```yaml
- alert: KafkaBrokerDown
  expr: count(up{job="kafka"} == 1) < 3
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Kafka broker down"
    description: "{{ $value }} brokers are down"
```

#### Alert 2: High Replication Lag
```yaml
- alert: HighReplicationLag
  expr: kafka_controller_kafkacontroller_offlinepartitionscount > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High replication lag"
    description: "{{ $value }} partitions offline"
```

#### Alert 3: Connector Failed
```yaml
- alert: KafkaConnectorFailed
  expr: kafka_connect_connect_connector_status{state="FAILED"} > 0
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Kafka connector failed"
    description: "Connector {{ $labels.connector }} is in FAILED state"
```

#### Alert 4: High End-to-End Latency
```yaml
- alert: HighEndToEndLatency
  expr: histogram_quantile(0.99, e2e_latency_ms) > 5000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High end-to-end latency"
    description: "P99 latency is {{ $value }}ms (threshold: 5000ms)"
```

---

### Step 6️⃣: Validate Observability Stack

#### 6.1 Access Prometheus

```
http://localhost:9090
```

Verify:
- All targets are green (Status → Targets)
- Metrics are being collected (Graph tab)
- Alert rules are loaded (Alerts tab)

#### 6.2 Access Grafana

```
http://localhost:3000
Username: admin
Password: admin (default)
```

Tasks:
1. Change default admin password
2. Create dashboards (5 total)
3. Add alert notifications
4. Test alerts manually

#### 6.3 Perform Load Test

Generate traffic and verify metrics:

```bash
# Run Phase 2 producer again to generate events
python3 producer.py

# Watch metrics in Grafana in real-time
# Expected changes:
# - Input topic message rate increases
# - Stream processor records/sec increases
# - Output topics message rate increases
# - Database write latency shows up
# - End-to-end latency visible
```

#### 6.4 Trigger Alerts

Stop a component and verify alerts fire:

```bash
# Stop one Kafka broker
docker stop kafka-2

# Expected:
# - Prometheus shows unavailable
# - KafkaBrokerDown alert fires
# - Grafana shows alert notification
```

---

## Key Metrics to Monitor

### Kafka Broker Metrics

| Metric | Meaning | Threshold |
|--------|---------|-----------|
| `kafka_server_brokerlogmanager_offlinelogsperbroker` | Offline partition count | > 0 = danger |
| `kafka_server_replicamanager_underreplicatedpartitions` | Under-replicated partitions | > 0 = danger |
| `kafka_controller_kafkacontroller_offlinepartitionscount` | Offline due to controller | > 0 = danger |
| `kafka_server_socket_network_requestmetrics_requests` | Request rate | Normal = stable |

### Stream Processor Metrics

| Metric | Meaning | Threshold |
|--------|---------|-----------|
| `streams_process_rate` | Records/sec through processor | > 1000 = good |
| `streams_process_latency_avg_ms` | Avg processing latency | < 100ms = good |
| `streams_record_lag_total` | Total lag in input topic | < 1000 = good |
| `task_count_active` | Active task count | = expected |

### Database Metrics

| Metric | Meaning | Threshold |
|--------|---------|-----------|
| `pg_stat_statements_mean_time` | Avg query latency | < 10ms = good |
| `pg_stat_activity_count` | Active connections | < 50 = good |
| `pg_database_size_bytes` | Database size | Growing slowly |
| `pg_sequences_max_value` | Sequence exhaustion | < 95% = good |

---

## Integration with Previous Phases

### Phase 2 (Producer)
- Monitor: Message production rate
- Expected: ~20 msg/sec during test

### Phase 3 (Stream Processor)
- Monitor: Processing latency, state store size
- Expected: <100ms p99 latency

### Phase 4 (Kafka Connect)
- Monitor: Connector status, lag
- Expected: All connectors RUNNING

### Phase 5 (Testing)
- Metrics validate: Unit tests match production behavior

---

## Success Criteria for Phase 6

✅ Prometheus collecting metrics from all sources  
✅ Grafana dashboards created (5 total)  
✅ Alert rules configured and tested  
✅ End-to-end latency visible and < 5 seconds  
✅ All components monitored (Kafka, Processor, Connector, Database)  
✅ Historical data retention (15 days)  
✅ Alerts fire when thresholds exceeded  
✅ Integration with all previous phases validated  

---

## Deliverables Checklist

- ⬜ docker-compose.yml updates (4 new services)
- ⬜ prometheus/prometheus.yml configuration
- ⬜ prometheus/alerts.yml alert rules
- ⬜ grafana provisioning configurations
- ⬜ 5 Grafana dashboards (JSON files)
- ⬜ Monitoring architecture diagram
- ⬜ Metrics reference documentation
- ⬜ Alert testing procedures
- ⬜ PHASE6_EXECUTION_FLOW.md (this file)
- ⬜ Phase 6 issues in ISSUES_AND_SOLUTIONS.md
- ⬜ PHASE6_COMPLETION_REPORT.md
- ⬜ PHASE6_SUMMARY.md

---

## Commands Reference

```bash
# Start monitoring stack
docker-compose up -d prometheus grafana jmx-exporter node-exporter postgres-exporter

# Check services
docker-compose ps | grep -E "prometheus|grafana|exporter"

# View Prometheus
curl http://localhost:9090/api/v1/targets

# View Grafana
curl http://localhost:3000/api/health

# Query metrics
curl 'http://localhost:9090/api/v1/query?query=kafka_server_brokerlogmanager_offlinelogsperbroker'

# Stop all monitoring
docker-compose down
```

---

## Next Steps (When Starting Implementation)

1. ✅ Read this flow document completely
2. ⏳ Create prometheus/prometheus.yml with job configurations
3. ⏳ Create prometheus/alerts.yml with 4+ alert rules
4. ⏳ Update docker-compose.yml with 4 exporter services
5. ⏳ Create grafana/provisioning/dashboards/*.json (5 dashboards)
6. ⏳ Deploy and verify stack is healthy
7. ⏳ Create load test scenarios
8. ⏳ Document issues encountered
9. ⏳ Create PHASE6_COMPLETION_REPORT.md
10. ⏳ Create PHASE6_SUMMARY.md

---

**Document**: PHASE6_EXECUTION_FLOW.md  
**Status**: Ready for Implementation  
**Next**: Proceed with Step 1 (Understand Architecture) → Step 6 (Validate)

