@echo off
REM Phase 6 Monitoring Stack Deployment Script for Windows
REM This script helps deploy Prometheus and Grafana in docker-compose

setlocal enabledelayedexpansion

echo ==========================================
echo Phase 6: Deploying Monitoring Stack
echo ==========================================
echo.

REM Check if directories exist
echo Checking configuration directories...

if not exist "prometheus\" (
    echo ERROR: prometheus directory not found
    exit /b 1
)

if not exist "grafana\" (
    echo ERROR: grafana directory not found
    exit /b 1
)

if not exist "prometheus\prometheus.yml" (
    echo ERROR: prometheus.yml not found
    exit /b 1
)

if not exist "prometheus\alert-rules.yml" (
    echo ERROR: alert-rules.yml not found
    exit /b 1
)

echo OK: All configuration files present
echo.

REM Start the monitoring stack
echo Starting Prometheus and Grafana services...
docker-compose up -d prometheus grafana

REM Wait for services to start
echo Waiting 30 seconds for services to initialize...
timeout /t 30 /nobreak

echo.
echo ==========================================
echo Phase 6 Monitoring Stack Ready!
echo ==========================================
echo.
echo Access URLs:
echo   Prometheus:  http://localhost:9090
echo   Grafana:     http://localhost:3000 ^(admin / admin^)
echo.
echo Useful Commands:
echo   View services:        docker-compose ps
echo   Check Prometheus:     curl http://localhost:9090/-/healthy
echo   View targets:         curl -s http://localhost:9090/api/v1/targets ^| jq
echo   View alerts:          curl -s http://localhost:9090/api/v1/alerts ^| jq
echo.
echo Next Steps:
echo   1. Open http://localhost:3000 in browser
echo   2. Login with admin / admin
echo   3. Check Prometheus datasource in Configuration ^> Datasources
echo   4. View Kafka Cluster Health dashboard
echo   5. Set up alerts and notifications
echo.

endlocal
