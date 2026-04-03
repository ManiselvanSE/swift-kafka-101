# Phase 6 Deployment Guide: Prometheus & Grafana Setup

**Status**: Ready for Deployment ✅  
**Date**: April 3, 2026  
**Docker Compose Updated**: Yes ✅  
**Configuration Files Created**: Yes ✅  

---

## Quick Start

### Option 1: Automated Deployment (Recommended)

**For Windows (PowerShell):**
```powershell
cd "e:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab"
.\deploy-phase6.bat
```

**For Linux/Mac:**
```bash
cd "/path/to/SwiftTrack-Kafka-Lab"
bash deploy-phase6.sh
```

### Option 2: Manual Deployment

```powershell
cd "e:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab"

# Start Prometheus and Grafana
docker-compose up -d prometheus grafana

# Wait 30 seconds for initialization
Start-Sleep -Seconds 30

# Verify services are running
docker-compose ps
```

---

## File Structure Created

```
SwiftTrack-Kafka-Lab/
├── prometheus/
│   ├── prometheus.yml          # Scrape configuration
│   └── alert-rules.yml         # Alert definitions
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── prometheus.yml  # Prometheus connection
│       └── dashboards/
│           ├── dashboards.yml  # Dashboard provisioning
│           └── kafka-cluster-health.json  # Sample dashboard
├── docker-compose.yml          # UPDATED with Phase 6 services
├── deploy-phase6.sh           # Linux/Mac deployment script
└── deploy-phase6.bat          # Windows deployment script
```

---

## Configuration Details

### Prometheus Configuration (prometheus/prometheus.yml)

```yaml
Global Settings:
  - Scrape interval: 15 seconds
  - Evaluation interval: 15 seconds
  - Retention: 30 days
  - Cluster label: swifttrack

Scrape Targets:
  - Kafka brokers (JMX metrics on :9999)
  - Schema Registry (metrics on :8081)
  - Kafka Connect (REST metrics on :8083)
  - Prometheus itself (health check)
```

### Alert Rules (prometheus/alert-rules.yml)

```yaml
Critical Alerts:
  - KafkaBrokerDown: If any broker goes offline
  - UnderReplicatedPartitions: Replication issues
  - OfflinePartitions: Data unavailability
  - ConnectorTaskFailure: Connector problems
  - TargetDown: Any monitoring target down

Warning Alerts:
  - HighDiskUsage: >100GB on broker
  - PostgresConnectionWarning: >80 connections
  - HighDatabaseGrowth: >100MB/hour
```

### Grafana Configuration

```
Admin User: admin
Admin Password: admin
Sign-up Disabled: Yes
Provisioned Datasource: Prometheus (http://prometheus:9090)
Provisioned Dashboard: Kafka Cluster Health
```

---

## Verification Steps

### Step 1: Verify Services Running

```powershell
docker-compose ps prometheus grafana

# Expected output:
# NAME       STATUS         PORTS
# prometheus Up (healthy)   0.0.0.0:9090->9090/tcp
# grafana    Up (healthy)   0.0.0.0:3000->3000/tcp
```

### Step 2: Test Prometheus Health

```powershell
curl http://localhost:9090/-/healthy

# Expected output: OK or similar success message
```

### Step 3: Test Grafana Health

```powershell
curl http://localhost:3000/api/health | ConvertFrom-Json

# Expected output includes: "ok": "true" or similar
```

### Step 4: Check Prometheus Targets

```powershell
$targets = curl -s http://localhost:9090/api/v1/targets | ConvertFrom-Json
$targets.data.activeTargets | Format-Table @{n='Job';e={$_.labels.job}}, @{n='Instance';e={$_.labels.instance}}, @{n='State';e={$_.state}}

# Expected output shows all targets as "up"
```

### Step 5: Verify Metrics Collection

```powershell
curl -s 'http://localhost:9090/api/v1/query?query=up' | ConvertFrom-Json | Select -ExpandProperty data | Select -ExpandProperty result | Format-Table

# Should show metrics from all targets
```

---

## Access the Tools

### Prometheus
**URL**: http://localhost:9090

**Common Tasks**:
- View targets: http://localhost:9090/targets
- Query metrics: http://localhost:9090/graph
- View alerts: http://localhost:9090/alerts
- Check health: http://localhost:9090/-/healthy

### Grafana
**URL**: http://localhost:3000
**Credentials**: admin / admin

**Common Tasks**:
- View dashboards: http://localhost:3000/d/
- Manage datasources: http://localhost:3000/datasources
- Configure alerts: http://localhost:3000/alerting
- View logs: http://localhost:3000/explore

---

## Useful Commands

### View Prometheus Metrics

```powershell
# List all available metrics
curl -s 'http://localhost:9090/api/v1/label/__name__/values' | ConvertFrom-Json | Select -ExpandProperty data | Select -First 20

# Query specific metric (broker count)
curl -s 'http://localhost:9090/api/v1/query?query=kafka_brokers' | ConvertFrom-Json | Select -ExpandProperty data

# Get all alerts
curl -s http://localhost:9090/api/v1/alerts | ConvertFrom-Json | Select -ExpandProperty data

# Get alert rules
curl -s http://localhost:9090/api/v1/rules | ConvertFrom-Json | Select -ExpandProperty data
```

### View Grafana Status

```powershell
# Health check
curl -s http://localhost:3000/api/health | ConvertFrom-Json

# List datasources
curl -s http://localhost:3000/api/datasources | ConvertFrom-Json

# List dashboards
curl -s http://localhost:3000/api/search | ConvertFrom-Json
```

### Container Logs

```powershell
# Prometheus logs
docker logs prometheus -f

# Grafana logs
docker logs grafana -f

# Both logs side-by-side
docker-compose logs -f prometheus grafana
```

---

## Troubleshooting

### Issue: Targets Show "DOWN"

**Solution** (See Issue 6.1):
```powershell
# Verify Kafka has JMX enabled
docker exec kafka-1 netstat -tuln | grep 9999

# If not found, Kafka needs JMX configuration:
# Add to docker-compose.yml kafka services environment:
# KAFKA_JMX_PORT: 9999
# KAFKA_JMX_HOSTNAME: kafka-1
```

### Issue: Prometheus Can't Reach Targets

**Solution** (See Issue 6.1):
```powershell
# Test from Prometheus container
docker-compose exec prometheus ping kafka-1

# If fails, check network configuration
docker network ls
docker network inspect swifttrack-kafka-lab_default
```

### Issue: Grafana Shows "No Data"

**Solution** (See Issue 6.3):
```powershell
# Verify datasource connection
curl -s http://localhost:3000/api/datasources | ConvertFrom-Json

# Test Prometheus from Grafana container
docker-compose exec grafana wget -O - http://prometheus:9090/api/v1/query?query=up

# Check time range in dashboard (use "Last 1 hour", not absolute time)
```

### Issue: Empty Graphs

**Solution** (See Issue 6.4):
```powershell
# Set auto-refresh: Change dashboard time picker to "10s" refresh
# Use relative time: "Last 1 hour" instead of fixed dates
# Verify metrics exist: curl -s 'http://localhost:9090/api/v1/query?query=kafka_brokers'
```

---

## Docker Compose Changes

### Added Services

**Prometheus**:
```yaml
prometheus:
  image: prom/prometheus:latest
  ports:
    - "9090:9090"
  volumes:
    - ./prometheus:/etc/prometheus
    - prometheus-storage:/prometheus
  healthcheck: Enabled
```

**Grafana**:
```yaml
grafana:
  image: grafana/grafana:latest
  ports:
    - "3000:3000"
  volumes:
    - ./grafana/provisioning:/etc/grafana/provisioning
    - grafana-storage:/var/lib/grafana
  depends_on: prometheus
  healthcheck: Enabled
```

### Added Volumes

```yaml
volumes:
  prometheus-storage:  # For Prometheus TSDB
  grafana-storage:     # For Grafana settings
```

---

## Next Steps After Deployment

1. ✅ Open http://localhost:3000 in browser
2. ✅ Login with admin/admin (you can change password later)
3. ✅ Go to Configuration > Datasources
4. ✅ Verify "Prometheus" datasource shows green checkmark
5. ✅ Go to Dashboards > Manage
6. ✅ View "Kafka Cluster Health" dashboard
7. ✅ Verify all 4 panels show data
8. ✅ Create additional dashboards as needed

---

## Expected Metrics Flow

```
Kafka Brokers
  ↓ (JMX export on :9999)
Prometheus 
  ↓ (Scrapes every 15s)
TSDB Storage
  ↓ (Queries via API)
Grafana
  ↓ (Renders dashboards)
Browsers (http://localhost:3000)
```

---

## Documentation References

- **PHASE6_EXECUTION_FLOW.md**: 6-step execution procedure
- **ISSUES_AND_SOLUTIONS.md**: Phase 6 issues (5 comprehensive issues with solutions)
- **PHASE6_COMPLETION_REPORT.md**: Detailed completion report (to be created)
- **PHASE6_SUMMARY.md**: Quick reference guide (to be created)

---

## Support

All Phase 6 troubleshooting procedures documented in:
- **Issue 6.1**: Prometheus Scrape Failures
- **Issue 6.2**: Grafana Datasource Connection Failing
- **Issue 6.3**: Missing Metrics in Dashboards
- **Issue 6.4**: Dashboard Shows Out-of-Date Data
- **Issue 6.5**: Alerting Rules Not Triggering

See: ISSUES_AND_SOLUTIONS.md → Phase 6 Issues

---

**Status**: ✅ Ready for Deployment  
**All Files**: Created and Configured  
**Next Command**: One of the deployment options above

