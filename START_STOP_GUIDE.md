# SwiftTrack Kafka Lab: Start & Stop Sequence Guide

## Overview
This document describes the recommended order to gracefully start and stop all services in the SwiftTrack Kafka Lab. Following this sequence ensures data integrity, avoids orphaned processes, and maintains observability.

---

## Graceful Shutdown Sequence

**1. Stop Data Producers**
- If running the Python producer manually:
  - Stop the process (Ctrl+C in terminal)
- If running in Docker:
  - `docker stop <producer-container>`

**2. Stop Stream Processor**
- If running manually:
  - Stop the process (Ctrl+C in terminal)
- If running in Docker:
  - `docker stop <stream-processor-container>`

**3. Stop Kafka Connect**
- `docker-compose stop kafka-connect`

**4. Stop Application Consumers (if any)**
- Stop any custom consumers or sinks

**5. Stop Monitoring Stack**
- `docker-compose stop grafana prometheus`

**6. Stop Database**
- `docker-compose stop postgres`

**7. Stop Schema Registry**
- `docker-compose stop schema-registry`

**8. Stop Kafka Brokers**
- `docker-compose stop kafka-1 kafka-2 kafka-3`

**9. (Optional) Bring down all containers**
- `docker-compose down` (removes all containers, keeps volumes)
- `docker-compose down -v` (removes containers and volumes)

---

## Startup Sequence

**1. Start Kafka Brokers**
- `docker-compose up -d kafka-1 kafka-2 kafka-3`

**2. Start Schema Registry**
- `docker-compose up -d schema-registry`

**3. Start Database**
- `docker-compose up -d postgres`

**4. Start Kafka Connect**
- `docker-compose up -d kafka-connect`

**5. Start Monitoring Stack**
- `docker-compose up -d prometheus grafana`

**6. Start Stream Processor**
- If using Docker:
  - `docker run ...` (see DEMO.md for exact command)
- If running manually:
  - `python stream_processor.py`

**7. Start Data Producers**
- If using Docker:
  - `docker run ...` (see DEMO.md for exact command)
- If running manually:
  - `python producer.py`

**8. Start Application Consumers (if any)**
- Start any custom consumers or sinks

---

## Notes
- Always allow 30-60 seconds for each service to become healthy before starting the next dependent service.
- Use `docker-compose ps` to check service status.
- For full demo, see DEMO.md.
- For troubleshooting, see ISSUES_AND_SOLUTIONS.md.

---

## References
- [DEMO.md](./DEMO.md)
- [PHASE6_DEPLOYMENT_GUIDE.md](./PHASE6_DEPLOYMENT_GUIDE.md)
- [PHASE6_EXECUTION_FLOW.md](./PHASE6_EXECUTION_FLOW.md)
- [ISSUES_AND_SOLUTIONS.md](./ISSUES_AND_SOLUTIONS.md)
