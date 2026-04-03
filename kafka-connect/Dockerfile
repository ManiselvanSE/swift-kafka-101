# Kafka Connect Docker image with JDBC driver
FROM confluentinc/cp-kafka-connect:7.9.0

# Install PostgreSQL JDBC driver
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8083/connectors || exit 1
