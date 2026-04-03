#!/bin/bash
# Phase 6 Monitoring Stack Deployment Script
# This script helps deploy Prometheus and Grafana in docker-compose

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=========================================="
echo "Phase 6: Deploying Monitoring Stack"
echo "=========================================="
echo ""

# Check if directories exist
echo "✓ Checking configuration directories..."
if [ ! -d "$PROJECT_DIR/prometheus" ]; then
    echo "✗ prometheus directory not found"
    exit 1
fi

if [ ! -d "$PROJECT_DIR/grafana" ]; then
    echo "✗ grafana directory not found"
    exit 1
fi

if [ ! -f "$PROJECT_DIR/prometheus/prometheus.yml" ]; then
    echo "✗ prometheus.yml not found"
    exit 1
fi

if [ ! -f "$PROJECT_DIR/prometheus/alert-rules.yml" ]; then
    echo "✗ alert-rules.yml not found"
    exit 1
fi

echo "✓ All configuration files present"
echo ""

# Start the monitoring stack
echo "Starting Prometheus and Grafana services..."
docker-compose up -d prometheus grafana

# Wait for services to start
echo "Waiting 30 seconds for services to initialize..."
sleep 30

echo ""
echo "=========================================="
echo "Phase 6 Monitoring Stack Ready!"
echo "=========================================="
echo ""
echo "Access URLs:"
echo "  Prometheus:  http://localhost:9090"
echo "  Grafana:     http://localhost:3000 (admin / admin)"
echo ""
echo "Useful Commands:"
echo "  View services:        docker-compose ps"
echo "  Check Prometheus:     curl http://localhost:9090/-/healthy"
echo "  View targets:         curl -s http://localhost:9090/api/v1/targets | jq"
echo "  View alerts:          curl -s http://localhost:9090/api/v1/alerts | jq"
echo ""
echo "Next Steps:"
echo "  1. Open http://localhost:3000 in browser"
echo "  2. Login with admin / admin"
echo "  3. Check Prometheus datasource in Configuration > Datasources"
echo "  4. View Kafka Cluster Health dashboard"
echo "  5. Set up alerts and notifications"
echo ""
