# Phase 2: Java Kafka Producer with Avro Serialization

## Overview

This is the **shipping-producer-service** component. It simulates package scan events and sends them to Kafka with:
- **Avro Serialization** - Messages are serialized using Avro schemas managed by Schema Registry
- **Idempotence** - `enable.idempotence=true` ensures no duplicate messages on retries
- **Partitioning by Key** - `package_id` is used as the message key, guaranteeing ordered events per package
- **Fault Tolerance** - `acks=all` waits for all replicas before acknowledging

## CCDAK Exam Domains Covered

- **Domain 1.0**: Avro/Schema Registry - Defines and registers package event schema
- **Domain 2.0**: Producer Application Development - Idempotent producer, key-based partitioning
- **Producer Concepts**: Idempotence, acks=all, compression, batching, retries

## Project Structure

```
producer-app/
├── pom.xml                  # Maven configuration with dependencies
├── src/main/java/
│   └── com/swifttrack/producer/
│       └── PackageProducer.java   # Main producer implementation
└── src/test/java/
    └── (tests in Phase 5)
```

## Prerequisites

- **Java 11+** installed
- **Maven 3.6+** installed
- **Kafka running** (from Phase 1)
- **Schema Registry running** (from Phase 1)

Verify:
```bash
java -version
mvn -version
docker ps
```

## Step 1: Build the Project

```bash
cd producer-app
mvn clean package
```

This will:
1. Download dependencies from Maven Central and Confluent Maven repo
2. Generate Java classes from the Avro schema (`package-event.avsc`)
3. Compile the code
4. Create a fat JAR with all dependencies (`kafka-producer-1.0-SNAPSHOT-all.jar`)

**Expected output:**
```
[INFO] Building jar: target/kafka-producer-1.0-SNAPSHOT-all.jar
[INFO] BUILD SUCCESS
```

## Step 2: Register the Avro Schema (Optional)

The producer will auto-register the schema with Schema Registry on first use. You can verify manually:

```bash
# Check registered subjects
curl -s http://localhost:8081/subjects | jq .

# Should return something like:
# ["package-events-value"]
```

## Step 3: Create the Kafka Topic

Before running the producer, create the topic (if not exists):

```bash
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --create \
  --topic package-events \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=delete
```

**Verify:**
```bash
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list
```

## Step 4: Run the Producer

From the `producer-app` directory:

```bash
java -jar target/kafka-producer-1.0-SNAPSHOT-all.jar
```

**Expected output:**
```
[INFO] Starting PackageProducer...
[INFO] PackageProducer initialized successfully
[INFO] Simulating 5 packages with 4 events each
[INFO] Message sent: PackageID=PKG-a1b2c3d4, Status=Package Picked Up, Partition=0, Offset=0, Timestamp=1234567890...
[INFO] Message sent: PackageID=PKG-a1b2c3d4, Status=In Transit, Partition=0, Offset=1, Timestamp=1234567891...
[INFO] Message sent: PackageID=PKG-x9y8z7w6, Status=Package Picked Up, Partition=1, Offset=0, Timestamp=1234567892...
...
[INFO] PackageProducer completed successfully
[INFO] Producer closed successfully
```

## Step 5: Verify Messages with Kafka Console Consumer

In a **new terminal**, consume the messages in Avro format:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081 \
  --formatter io.confluent.kafka.formatter.AvroMessageFormatter
```

**Expected output:**
```
{"package_id":"PKG-a1b2c3d4","status":"Package Picked Up","timestamp":1712140620123,"location":"New York Distribution Center"}
{"package_id":"PKG-a1b2c3d4","status":"In Transit","timestamp":1712140620224,"location":"Los Angeles Hub"}
{"package_id":"PKG-a1b2c3d4","status":"Out for Delivery","timestamp":1712140620325,"location":"Chicago Sorting Facility"}
{"package_id":"PKG-a1b2c3d4","status":"Delivered","timestamp":1712140620426,"location":"Customer Location"}
{"package_id":"PKG-x9y8z7w6","status":"Package Picked Up","timestamp":1712140620527,"location":"New York Distribution Center"}
...
```

## Producer Configuration Explained

### Idempotence
```java
ENABLE_IDEMPOTENCE_CONFIG = true
```
- Ensures exactly-once semantics per partition per producer session
- Prevents duplicate messages on network retries
- Required for high-reliability systems (supply chain, finance, etc.)

### Acknowledgments
```java
ACKS_CONFIG = "all"
```
- Waits for all in-sync replica brokers to acknowledge before returning
- Ensures data durability even if brokers fail

### Key-Based Partitioning
```java
ProducerRecord<String, GenericRecord> record = 
  new ProducerRecord<>(TOPIC, packageId, value);
```
- Uses `package_id` as the key
- Kafka guarantees all events for the same key go to the same partition
- This ensures chronological ordering per package

### Compression
```java
COMPRESSION_TYPE_CONFIG = "snappy"
```
- Reduces network bandwidth and storage costs
- Snappy provides good balance between compression ratio and speed

### Batching
```java
BATCH_SIZE_CONFIG = 16384    // 16KB
LINGER_MS_CONFIG = 10         // Wait up to 10ms
```
- Groups messages together for efficiency
- Reduces network calls and improves throughput

## Testing Idempotence

To verify that idempotence works, you can:

1. Run the producer and let it complete normally (20 messages sent)
2. Run the producer again
3. Check topic with console consumer

The second run will send **additional** messages (not duplicates), proving idempotence prevents retries from creating duplicates.

## Troubleshooting

### Issue: "Schema Registry unreachable"
```
org.apache.kafka.common.KafkaException: Failed to get schema for payload
```
**Fix:** Ensure Schema Registry is running (`docker ps` should show schema-registry)

### Issue: "Topic doesn't exist"
```
org.apache.kafka.errors.UnknownTopicOrPartitionException
```
**Fix:** Create the topic using Step 3 above

### Issue: "Certificate validation error"
```
javax.net.ssl.SSLHandshakeException
```
**Fix:** This lab uses plaintext (non-HTTPS). If you see SSL errors, ensure you're using `localhost:9092`, not HTTPS URLs.

### Issue: "Port already in use"
```
Address already in use
```
**Fix:** Stop other Kafka instances or change the port in the producer configuration

## What's Next?

- **Phase 3**: Build a **Kafka Streams** application to process these events in real-time
- **Phase 4**: Add **Kafka Connect** to sink the data into PostgreSQL
- **Phase 5**: Write unit tests using **TopologyTestDriver**

## Additional Resources

- [Apache Kafka Producer Docs](https://kafka.apache.org/documentation/#producerconfigs)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Avro Specification](https://avro.apache.org/docs/current/)
