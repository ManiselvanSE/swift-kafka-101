# SwiftTrack Kafka Lab - Issues & Solutions (Phase 2, 3, 4 & 5)

**Document Type**: Troubleshooting & Lessons Learned  
**Date**: 2026-04-03  
**Scope**: All issues encountered and their resolutions across Phase 2, Phase 3, Phase 4, and Phase 5

---

## Table of Contents

1. [Phase 2: Python Avro Producer Issues](#phase-2-issues)
2. [Phase 3: Kafka Streams Processor Issues](#phase-3-issues)
3. [Phase 4: Kafka Connect Issues](#phase-4-issues)
4. [Phase 5: TopologyTestDriver Issues](#phase-5-issues)
5. [Phase 6: Observability Issues](#phase-6-issues)
6. [Common Patterns & Prevention](#common-patterns)
7. [Debugging Techniques Used](#debugging-techniques)

---

# Phase 2 Issues

## Issue 2.1: Java Producer Unnecessarily Complex

**Category**: Architecture Decision  
**Severity**: Medium (wasted time, unnecessary complexity)  
**Timeline**: Early Phase 2

### Problem Description
Initially created a Java-based Avro producer using Maven with pom.xml, verbose configuration, and multiple dependencies. This approach was:
- Overcomplicated for a simple message generation task
- Required Java development environment
- More setup time for minimal benefit
- Harder to debug and iterate

### Root Cause
Assumption that "enterprise Kafka development" requires Java, when the project goal was to learn Kafka concepts, not specific language implementations.

### Solution
**Decision**: Switched to Python-based producer

### Step-by-Step How to Fix

**Step 1: Recognize the problem**
```bash
# You notice Maven build taking long time
cd /streams-processor
mvn clean compile
# Output takes 5+ minutes, downloads hundreds of MB
# Realization: This is overkill for sending 20 messages
```

**Step 2: Backup existing code (just in case)**
```bash
mkdir backup
cp pom.xml backup/pom.xml
cp -r src backup/src
cp Dockerfile backup/Dockerfile-java
cp run-phase2.sh backup/run-phase2.sh
```

**Step 3: Create Python alternative**
```bash
# Create new Python producer script
cat > producer.py << 'EOF'
#!/usr/bin/env python3
import json
import random
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient

producer = Producer({'bootstrap.servers': 'kafka-1:9092'})

# Generate and send 20 messages
for i in range(20):
    message = {
        'package_id': f'PKG-{random.randbytes(4).hex()}',
        'status': random.choice(['Package Picked Up', 'In Transit', 'Delivered']),
        'timestamp': int(time.time() * 1000),
        'location': 'Hub'
    }
    producer.produce('package-events', json.dumps(message).encode())

producer.flush()
EOF
```

**Step 4: Test Python locally in Docker**
```bash
docker build -f Dockerfile.python -t phase2-producer:python .
docker run --rm --network swifttrack-kafka-lab_default phase2-producer:python
# Output: "Sent 20 messages successfully"
```

**Step 5: Remove Java artifacts**
```bash
# Delete Java files
rm -f pom.xml
rm -rf src/
rm -f Dockerfile  # Old Java docker file
rm -f run-phase2.sh

# Verify they're gone
ls -la  # Should not show pom.xml or src/
```

**Step 6: Keep Python version**
```bash
mv producer.py final-producer.py
# Now you have clean, simple Python producer
```

**Why Python Was Better**:
- Faster development iteration
- Simpler syntax for learning
- confluent-kafka library provides all needed Avro support
- Easier debugging with print statements
- Faster development-test cycles

### Lessons Learned
✅ Choose the simplest tool that accomplishes the goal  
✅ Don't optimize for perceived "enterprise standards" when learning  
✅ Python's faster iteration beats Java's robustness for prototype phases  
✅ Always question initial architectural decisions

---

## Issue 2.2: Windows Environment Incompatible with confluent-kafka

**Category**: Environment Setup  
**Severity**: High (blocked local development)  
**Timeline**: Mid Phase 2

### Problem Description
Attempted to install confluent-kafka==2.2.0 locally on Windows to test producer code before Dockerizing.

**Error Message**:
```
ERROR: Microsoft Visual C++ 14.0 or greater is required. Get it with "Microsoft C++ Build Tools": https://visualstudio.microsoft.com/visual-cpp-build-tools/
Building wheel for confluent-kafka (2.2.0) ...
error: subprocess.run() raised CalledProcessError: Command '[...] build' returned non-zero exit status 1.
```

### Root Cause
`confluent-kafka` is a compiled C++ extension (librdkafka binding). Windows doesn't have C++ build tools installed by default, and the pre-built wheels for Windows are limited.

### Solutions Attempted

**Attempt 1**: Install Visual Studio Build Tools  
❌ **Failed** - Installation process was complex and fragile, would need ~5GB disk space

**Attempt 2**: Use pre-built wheel from unofficial sources  
❌ **Failed** - Version mismatches, security concerns

**Attempt 3**: Use Docker from the start  
✅ **Success** - Eliminated the Windows environment issue entirely

### Step-by-Step How to Fix

**Step 1: Acknowledge the problem**
```bash
# You try to install locally and get the painful error
pip install confluent-kafka==2.2.0

# Output:
# ERROR: Microsoft Visual C++ 14.0 or greater is required...
# Error: subprocess.run() raised CalledProcessError...
```

**Step 2: Understand why it failed**
```
Root cause analysis:
- confluent-kafka is a compiled C extension
- Uses librdkafka C++ library underneath
- Windows doesn't have C++ compiler by default
- Pre-built wheels limited for Windows Python versions
- Installing Visual C++ build tools is heavyweight operation
```

**Step 3: Decide on Docker approach**
```bash
# Create Dockerfile that HAS build tools
cat > Dockerfile.python << 'EOF'
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .

# Linux base image HAS all build tools
# pip can compile confluent-kafka from source
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
CMD ["python", "producer.py"]
EOF
```

**Step 4: Create requirements.txt**
```bash
cat > requirements.txt << 'EOF'
confluent-kafka==2.2.0
avro==1.11.3
requests==2.31.0
EOF
```

**Step 5: Build Docker image**
```bash
# This happens inside Linux container
docker build -f Dockerfile.python -t phase2-producer:latest .

# Output shows pip compiling confluent-kafka:
# Collecting confluent-kafka==2.2.0
#   Compiling confluent-kafka-2.2.0/setup.py
#   ...successfully built confluent-kafka
```

**Step 6: Test in container instead of locally**
```bash
# Don't install locally - test in Docker
docker run --rm --network swifttrack-kafka-lab_default phase2-producer:latest

# Works perfectly! Message sent successfully.
```

**Step 7: Never try local installation again (for this project)**
```bash
# In your development workflow, ALWAYS:
# 1. Write code locally (Python is text)
# 2. Test in Docker (where it matters)
# 3. Don't install C++ extensions on Windows
```

**Step 8: Update team documentation**
```plaintext
IMPORTANT: Development Workflow
- Edit .py files locally in VS Code (fast iteration)
- Container handles all compiled dependencies
- Always test in Docker before claiming "it works"
- THIS saves the "works on my machine" debate
```

### Why Docker Solved It
- Docker container has Linux base, full build tools available
- Same environment as production
- Reproducible across machines
- No Windows-specific issues
- Easy to iterate and rebuild
- Single source of truth

### Lessons Learned
✅ For compiled dependencies, use Docker early  
✅ Don't waste time trying to make Windows work with C++ extensions  
✅ Docker solves "works on my machine" problem completely  
✅ Test all code in Docker, even during development

---

## Issue 2.3: Avro Producer Initialization Failures

**Category**: Configuration & Dependencies  
**Severity**: High (code didn't work)  
**Timeline**: Late Phase 2

### Problem Description
Created Python Avro producer using confluent-kafka's `AvroProducer` class:

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroProducer

producer = AvroProducer({
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    'schema.registry.url': 'http://schema-registry:8081'
})
```

**Error**:
```
confluent_kafka.schema_registry.SchemaRegistryClient - Failed to initialize Schema Registry client
ValueError: ...
AttributeError: 'NoneType' object has no attribute '...'
```

### Root Cause
Multiple issues stacked:
1. `AvroProducer` has strict validation of Schema Registry connectivity
2. Environment variables not properly set in Docker container
3. Schema Registry URL had network resolution issues
4. Missing required dependencies for specific confluent-kafka version

### Solutions Attempted

**Attempt 1**: Debug configuration values  
```python
print(f"SR URL: {os.getenv('SCHEMA_REGISTRY_URL')}")
print(f"Brokers: {os.getenv('BOOTSTRAP_SERVERS')}")
```
❌ **Partial Help** - Found mismatched env vars, but didn't solve root issue

**Attempt 2**: Use explicit URL instead of env var  
```python
'schema.registry.url': 'http://schema-registry:8081'
```
⚠️ **Better** - Helped but AvroProducer still had strict validation

**Attempt 3**: Switch to manual Avro handling  
✅ **Success** - Used plain Consumer/Producer with manual Avro serialization

### Step-by-Step How to Fix

**Step 1: Recognize the AvroProducer problem**
```python
# Original code that fails:
from confluent_kafka.schema_registry.avro import AvroProducer

producer = AvroProducer({
    'bootstrap.servers': 'kafka-1:9092',
    'schema.registry.url': 'http://schema-registry:8081'
})
# Error: ValueError / AttributeError
# The AvroProducer has very strict validation
```

**Step 2: Check what's available**
```python
# Inspect what you can use from confluent_kafka
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import fastavro
import io

print("✓ Producer imported")
print("✓ SchemaRegistryClient imported")
print("✓ fastavro imported")
# All good - we can build alternative
```

**Step 3: Test manual serialization approach**
```python
# Test Avro serialization WITHOUT AvroProducer
def serialize_avro(data, schema):
    """Manually serialize Python dict to Avro bytes."""
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, data)
    return bytes_writer.getvalue()

# Test it
test_data = {'name': 'test', 'timestamp': 123}
test_schema = {
    'type': 'record',
    'fields': [{'name': 'name', 'type': 'string'}, 
               {'name': 'timestamp', 'type': 'long'}]
}

result = serialize_avro(test_data, test_schema)
print(f"Serialized: {result.hex()}")  # Shows binary Avro format
# ✓ Works! Manual serialization successful
```

**Step 4: Create the Producer manually**
```python
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'kafka-1:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'compression.type': 'snappy',
})

print("✓ Producer created successfully")
# No schema registry validation, just plain producer
```

**Step 5: Register schema in Schema Registry**
```python
from confluent_kafka.schema_registry import SchemaRegistryClient

sr_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

schema_str = json.dumps(PACKAGE_EVENT_SCHEMA)
subject = 'package-events-value'

try:
    version = sr_client.register_schema(subject, 
        Schema(schema_str, schema_type='AVRO'))
    print(f"✓ Schema registered with version {version}")
except:
    # Might already exist
    existing = sr_client.get_latest_version(subject)
    print(f"✓ Schema already exists: {existing}")
```

**Step 6: Combine manual serialization + producer**
```python
def send_avro_message(data, schema, topic):
    """Send message with manual Avro serialization."""
    
    # Serialize
    serialized = serialize_avro(data, schema)
    
    # Produce
    producer.produce(
        topic=topic,
        key=data['package_id'].encode(),
        value=serialized
    )

# Test it
message = {
    'package_id': 'PKG-123',
    'status': 'Delivered',
    'timestamp': int(time.time() * 1000),
    'location': 'Hub A'
}

send_avro_message(message, PACKAGE_EVENT_SCHEMA, 'package-events')
producer.flush()

print("✓ Message sent with manual Avro serialization!")
```

**Step 7: Verify message in topic**
```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events \
  --from-beginning \
  --max-messages 1

# Output shows Avro binary format (magic byte at start)
# ✓ Confirmation: Message successfully sent and serialized
```

**Step 8: Loop for all 20 messages**
```python
for i in range(20):
    message = {
        'package_id': f'PKG-{random.randbytes(4).hex()}',
        'status': random.choice(STATUSES),
        'timestamp': int(time.time() * 1000),
        'location': random.choice(LOCATIONS)
    }
    send_avro_message(message, PACKAGE_EVENT_SCHEMA, 'package-events')
    
producer.flush()
print(f"✓ Successfully sent 20 messages")
```

### Final Solution
```python
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import fastavro
import io

producer = Producer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all'
})

def serialize_avro(data, schema):
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, data)
    return bytes_writer.getvalue()

# Produce with manual serialization
producer.produce(
    topic='package-events',
    key=data['package_id'].encode(),
    value=serialize_avro(data, SCHEMA)
)
```

**Why This Worked**:
- Removes AvroProducer's strict validation
- More explicit control over serialization
- Fewer dependencies on confluent-kafka's internal implementations
- Easier to debug byte-level issues
- More flexible for learning

### Lessons Learned
✅ Sometimes library abstractions cause more problems than they solve  
✅ Manual implementation can be simpler than "convenience" classes  
✅ Confluent's Avro wrappers have strict validation - sometimes too strict  
✅ Decompose problems: serialize separately from producing

---

## Issue 2.4: Schema Not Pre-Registered in Schema Registry

**Category**: Data Format & Schema Management  
**Severity**: Medium (schema validation failed)  
**Timeline**: Mid Phase 2

### Problem Description
Producer attempted to serialize Avro messages without pre-registering the schema in Schema Registry.

**Error**:
```
Schema not found in registry
Subject 'package-events-value' not found
```

### Root Cause
Assumed Schema Registry would auto-register schemas on first use. It doesn't - schemas must be pre-registered or explicitly registered before use.

### Solution
Created schema registration step:

```python
def register_schema_if_needed():
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    schema_str = json.dumps(PACKAGE_EVENT_SCHEMA)
    subject = 'package-events-value'
    
    try:
        # Try to get existing schema
        existing = sr_client.get_latest_version(subject)
        print(f"Schema already registered: {existing}")
    except:
        # Register new schema
        schema = Schema(schema_str, schema_type='AVRO')
        registered = sr_client.register_schema(subject, schema)
        print(f"Schema registered: {registered}")
```

**Placement**: Called at producer startup before any messages

### Lessons Learned
✅ Always register schemas before using them  
✅ Schema Registry is not auto-magical - requires explicit setup  
✅ Check if schema exists before registering (avoid duplicates)  
✅ Document what schemas your application expects

---

## Issue 2.5: Message Timestamp Generation

**Category**: Data Format  
**Severity**: Low (functional but wrong timestamps)  
**Timeline**: Late Phase 2

### Problem Description
Generated 20 messages but all had incorrect timestamps - some from 1970, some in future. This happened because:
```python
timestamp = int(time.time() * 1000)  # Correct
# But then some messages used:
timestamp = random.randint(1000000000000, 2000000000000)  # Random range
# And some used:
timestamp = 1775161076401  # Hardcoded value
```

### Root Cause
Copy-paste inconsistency - different methods used in different places without standardization.

### Solution
Centralized timestamp generation:

```python
import time

def get_timestamp_ms():
    """Get current time in milliseconds since epoch."""
    return int(time.time() * 1000)

# Use consistently everywhere
for i in range(20):
    message = {
        'package_id': f'PKG-{random.randbytes(4).hex()}',
        'status': random.choice(STATUSES),
        'timestamp': get_timestamp_ms(),  # Always from same source
        'location': random.choice(LOCATIONS)
    }
```

### Why It Matters
- Consistent timestamps allow accurate replay and debugging
- Real systems expect chronologically ordered events
- Makes troubleshooting easier (can reproduce timing issues)
- Good practice for data quality

### Lessons Learned
✅ Centralize utility functions (don't duplicate logic)  
✅ For time-sensitive data, use consistent time sources  
✅ Generator functions are better than scattered time calls  
✅ Even "test data" should follow real-world constraints

---

# Phase 3 Issues

## Issue 3.1: Processor Consumed Messages But Processed 0

**Category**: Silent Failure / Data Deserialization  
**Severity**: Critical (most blocking issue of Phase 3)  
**Timeline**: Early-mid Phase 3

### Problem Description
Processor ran successfully, initialized correctly, and consumed messages from input topic - but reported "Processed 0 messages" at the end.

**Logs**:
```
[2026-04-02 20:34:16] INFO: Processing stream...
[2026-04-02 20:34:38] INFO: Processed 0 messages in 22.2s
[2026-04-02 20:34:38] INFO: Status counts: {}
```

Consumer group showed offsets were being committed:
```
streams-processor-group package-events 0 4 4 0  # offset=4, lag=0
```

But no debug output showed messages being processed.

### Root Cause
Multiple layered issues:

**Layer 1**: Deserialization failing silently
```python
try:
    data = self.deserialize_avro(msg.value())
    if not data:  # This was True for all messages!
        continue  # Skip silently
except:
    return None  # Catch-all exception handling
```

**Layer 2**: Deserialization function had wrong logic
```python
reader_schema = json.loads(schema_obj.schema.schema_str)
# schema_obj doesn't have .schema attribute!
# Should be: schema_obj.schema_str directly
```

**Layer 3**: No logging to show what was happening
```python
try:
    data = deserialize_avro(msg.value())
    # No log here - silently fails and returns to loop
```

### Solutions Attempted

**Attempt 1**: Add logging at message level  
```python
logger.info(f"Processing message: {msg.key()}")
```
⚠️ **Partial Help** - Showed messages arrived but no message logs appeared

**Attempt 2**: Log deserialization errors  
```python
except Exception as e:
    logger.error(f"Deserialization failed: {e}")
```
✅ **Found the Bug!** - Revealed: `'Schema' object has no attribute 'schema'`

**Attempt 3**: Fix schema access path  
```python
reader_schema = json.loads(schema_obj.schema.schema_str)  # ❌ Wrong
# vs
reader_schema = json.loads(schema_obj.schema_str)  # ✅ Correct
```
⚠️ **Worked but incomplete** - Needed fallback for different versions

### Step-by-Step How to Fix (The Most Complex Issue)

**Step 1: Detect the problem**
```bash
# Run processor with 20 messages in topic
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:1.0

# Output:
# [2026-04-02 20:34:16] INFO: Processing stream...
# [2026-04-02 20:34:38] INFO: Processed 0 messages in 22.2s
# [2026-04-02 20:34:38] INFO: Status counts: {}

# ❌ Problem: Claims to process 0 messages
```

**Step 2: Verify messages exist in topic**
```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events \
  --from-beginning \
  --max-messages 1

# Output shows message data - it's there!
# PKG-d179f2c9    PKG-d179f2c9"Package Picked Up...

# ✓ Confirmed: Messages definitely exist
```

**Step 3: Check consumer group offsets**
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --describe

# Output:
# streams-processor-group package-events 0 4 4 0
# streams-processor-group package-events 1 12 12 0  
# streams-processor-group package-events 2 4 4 0

# ✓ Confirmed: Consumer DID consume messages (offsets=4,12,4)
# ✓ But processor says it processed 0
# → Problem is in deserialization/processing loop
```

**Step 4: Add logging to find where it fails**
```python
# Original code (no visibility):
while True:
    msg = self.consumer.poll(timeout=2.0)
    if msg is None:
        empty += 1
        if empty > 10:
            break
        continue
    
    # No logging here!
    data = self.deserialize_avro(msg.value())  # Returns None silently
    if not data:
        continue  # Skip silently
    
    count += 1  # Never reached!
    logger.info(f"[INPUT #{count}] ...")
```

**Step 5: Enhanced version with logging at every step**
```python
while True:
    msg = self.consumer.poll(timeout=2.0)
    
    if msg is None:
        empty += 1
        logger.debug(f"No message ({empty}/10)")  # ← Log empty polls
        if empty > 10:
            break
        continue
    
    # ← NEW: Log that we got a message
    logger.info(f"Got message from {msg.topic()}:{msg.partition()}@{msg.offset()}")
    
    empty = 0
    if msg.error():
        logger.error(f"Consumer error: {msg.error()}")  # ← Log errors
        continue
    
    # ← NEW: Log deserialization attempt
    data = self.deserialize_avro(msg.value())
    
    if not data:
        logger.warning(f"Deserialization returned None at offset {msg.offset()}")  # ← Log failures
        continue
    
    count += 1
    logger.info(f"[INPUT #{count}] {data}")  # ← Log success
```

**Step 6: Run with enhanced logging**
```bash
docker build ... swifttrack-streams-processor:debug

docker run ... swifttrack-streams-processor:debug

# Output now shows:
# [2026-04-03 05:22:20] INFO: Got message from package-events:0@0
# [2026-04-03 05:22:20] DEBUG: Deserializing message: 58 bytes
# [2026-04-03 05:22:20] ERROR: Deserialization failed: 'Schema' object has no attribute 'schema'
# ^ THIS tells us exactly where it fails!
```

**Step 7: Inspect the schema object structure**
```python
# Add introspection logging to deserialize function
def deserialize_avro(self, msg_bytes):
    try:
        if msg_bytes[0] == 0:
            schema_id = struct.unpack('>I', msg_bytes[1:5])[0]
            sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
            schema_obj = sr_client.get_schema(schema_id)
            
            # ← NEW: Inspect the object
            logger.debug(f"schema_obj type: {type(schema_obj)}")
            logger.debug(f"schema_obj attributes: {dir(schema_obj)}")
            logger.debug(f"Has 'schema_str'? {hasattr(schema_obj, 'schema_str')}")
            logger.debug(f"Has 'schema'? {hasattr(schema_obj, 'schema')}")
            
            # Old code tries: schema_obj.schema.schema_str
            # This fails because schema_obj.schema doesn't exist!
```

**Step 8: Run and see what attributes actually exist**
```bash
docker run ... swifttrack-streams-processor:debug

# New output shows:
# [2026-04-03 05:22:20] DEBUG: schema_obj type: <class 'confluent_kafka.schema_registry.schema_registry_client.Schema'>
# [2026-04-03 05:22:20] DEBUG: Has 'schema_str'? True ← THERE IT IS!
# [2026-04-03 05:22:20] DEBUG: Has 'schema'? False

# ✓ Problem found: should be schema_obj.schema_str, not schema_obj.schema.schema_str
```

**Step 9: Fix with correct attribute path**
```python
# ❌ Old (broken):
reader_schema = json.loads(schema_obj.schema.schema_str)

# ✅ New (working):
reader_schema = json.loads(schema_obj.schema_str)
```

**Step 10: Rebuild and test**
```bash
docker build ... swifttrack-streams-processor:1.0

docker run ... swifttrack-streams-processor:1.0

# Output now shows:
# [2026-04-03 05:22:20] INFO: [INPUT #1] PKG-4669442e | Package Picked Up | Chicago Sorting Facility
# [2026-04-03 05:22:20] INFO:   -> [MAP location]
# [2026-04-03 05:22:20] INFO: [INPUT #2] PKG-4669442e | In Transit | Customer Location
# ...
# [2026-04-03 05:22:22] INFO: Processed 20 messages in 62.3s

# ✓ SUCCESS: All 20 messages processed!
```

**Step 11: Add version-safe fallback for future compatibility**
```python
# Even better - handle multiple versions:
if hasattr(schema_obj, 'schema_str'):
    reader_schema = json.loads(schema_obj.schema_str)
elif hasattr(schema_obj, 'schema'):
    reader_schema = json.loads(schema_obj.schema)
else:
    reader_schema = json.loads(str(schema_obj))

# Now works with:
# - Current version
# - Future versions with different attribute names
# - Fallback to string conversion if needed
```

### Final Solution

```python
def deserialize_avro(self, msg_bytes):
    """Simple Avro deserialization."""
    if not msg_bytes:
        return None
    try:
        if msg_bytes[0] == 0:
            import struct
            import fastavro
            from io import BytesIO
            
            sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
            schema_id = struct.unpack('>I', msg_bytes[1:5])[0]
            schema_obj = sr_client.get_schema(schema_id)
            
            # Try different attribute paths based on version
            if hasattr(schema_obj, 'schema_str'):
                reader_schema = json.loads(schema_obj.schema_str)
            elif hasattr(schema_obj, 'schema'):
                reader_schema = json.loads(schema_obj.schema)
            else:
                reader_schema = json.loads(str(schema_obj))
            
            return fastavro.schemaless_reader(BytesIO(msg_bytes[5:]), reader_schema)
        else:
            return json.loads(msg_bytes.decode('utf-8'))
    except Exception as e:
        logger.debug(f"Deserialization error: {e}")
        return None
```

**Why This Works**:
- ✅ Tries multiple attribute paths for version compatibility
- ✅ Has explicit fallback for different Schema Registry versions
- ✅ Logs errors for debugging
- ✅ Handles both Avro and JSON formats

### What We Learned
✅ **Critical**: Never silently fail in stream processing without logging  
✅ **Critical**: Log every deserialization attempt and failure  
✅ **Critical**: Test with actual data format, not just happy path  
✅ Multi-version compatibility requires defensive coding  
✅ Silent failures are harder to debug than loud failures  
✅ Always ask: "What would happen if this returned None?"

---

## Issue 3.2: Missing `fastavro` Dependency

**Category**: Missing Dependency / Configuration  
**Severity**: Critical (code failed at runtime)  
**Timeline**: Mid Phase 3

### Problem Description
Processor tried to import `fastavro` but it wasn't installed.

**Error**:
```
ModuleNotFoundError: No module named 'fastavro'
```

**Root Cause**: `fastavro>=1.8.0` was not in `requirements.txt`

### Why It Was Added
- Used for fast Avro deserialization
- `avro==1.11.3` (pure Python) is very slow
- `fastavro` uses C extensions for speed

### Step-by-Step How to Fix

**Step 1: Identify the error**
```bash
docker run ... swifttrack-streams-processor:1.0

# Output shows:
# Traceback (most recent call last):
#   File "/app/stream_processor.py", line 105, in deserialize_avro
#     import fastavro
# ModuleNotFoundError: No module named 'fastavro'

# ✗ Error is clear: fastavro not installed
```

**Step 2: Check what dependencies are installed**
```bash
# Inside running container, check requirements
docker run -ti --entrypoint bash swifttrack-streams-processor:1.0

# In container:
pip list | grep avro
# Output:
# avro                    1.11.3
# ← No fastavro listed!
```

**Step 3: Examine requirements.txt**
```bash
cat streams-processor/requirements.txt

# Output:
confluentkafka==2.2.0
avro==1.11.3
requests==2.31.0
authlib>=1.2.0
cachetools>=5.0.0
httpx>=0.24.0
websocket-client>=1.5.0

# ✗ Missing: fastavro
```

**Step 4: Add fastavro to requirements**
```bash
# Edit requirements.txt
cat >> streams-processor/requirements.txt << 'EOF'
fastavro>=1.8.0
EOF

# Verify it's added
cat streams-processor/requirements.txt
# Should now include fastavro
```

**Step 5: Check available fastavro versions**
```bash
# Find compatible version
pip search fastavro  # or visit pypi.org

# Latest stable versions include:
# - fastavro==1.9.7 (latest stable as of 2026-04)
# - fastavro>=1.8.0 (good for our needs)

# We'll use >=1.8.0 for flexibility
```

**Step 6: Rebuild Docker image**
```bash
cd "e:\Kafka\Projects\Swift Track\SwiftTrack-Kafka-Lab"

docker build --no-cache -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.1 .

# Output shows:
# => [6/6] RUN pip install --no-cache-dir -r requirements.txt
# Collecting fastavro>=1.8.0
#   Downloading fastavro-1.9.7-cp311-cp311-linux_x86_64.whl (1.2 MB)
# Installing collected packages: ..., fastavro, ...
# Successfully installed fastavro-1.9.7

# ✓ fastavro now installed in image
```

**Step 7: Verify fastavro is available**
```bash
# Test that fastavro works
docker run -ti --entrypoint python swifttrack-streams-processor:1.1 << 'EOF'
import fastavro
print(f"fastavro version: {fastavro.__version__}")
print("✓ fastavro imported successfully!")
EOF

# Output:
# fastavro version: 1.9.7
# ✓ fastavro imported successfully!
```

**Step 8: Test full processor with fastavro**
```bash
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:1.1

# Output shows:
# [2026-04-03 05:22:20] INFO: [INPUT #1] PKG-4669442e | Package Picked Up | ...
# [2026-04-03 05:22:20] DEBUG: Deserialized: {...}
# ✓ Now works with fastavro!
```

**Step 9: Document for next time**
```markdown
# Pattern for adding dependencies:

1. When you add new code that imports something:
   - Example: `import fastavro`
   
2. Immediately check if it's in requirements.txt:
   - `grep fastavro requirements.txt`
   
3. If not found, add it:
   - `echo "fastavro>=1.8.0" >> requirements.txt`
   
4. Rebuild Docker:
   - `docker build ... -t image:newversion .`
   
5. Test new image:
   - `docker run ...`
```

### Solution
Added to requirements.txt:
```
fastavro>=1.8.0
```

Rebuilt Docker image:
```bash
docker build --no-cache -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.1 .
```

### Lessons Learned
✅ If code imports it, it must be in requirements.txt  
✅ Use `pip freeze > requirements.txt` after testing locally first  
✅ Don't rely on transitive dependencies  
✅ Always test Docker build with fresh image (no cache)

---

## Issue 3.3: Schema Object Attribute Mismatch

**Category**: API Misunderstanding  
**Severity**: Critical (code broke silently)  
**Timeline**: Mid Phase 3

### Problem Description
Code attempted to access schema using wrong attribute path:

```python
# ❌ This doesn't work:
reader_schema = json.loads(schema_obj.schema.schema_str)

# ✅ Correct:
reader_schema = json.loads(schema_obj.schema_str)
```

The Schema Registry client returns a Schema object with `schema_str` attribute directly, not nested under `.schema`.

### Root Cause
Assumption based on nested structure naming - thought `.schema` would be another wrapper object.

### How It Was Found
Created debug processor with extensive logging:

```python
logger.debug(f"Schema object type: {type(schema_obj)}")
logger.debug(f"Schema object attributes: {dir(schema_obj)}")
logger.debug(f"Has schema_str? {hasattr(schema_obj, 'schema_str')}")
```

**Output**:
```
Schema object type: <class 'confluent_kafka.schema_registry.schema_registry_client.Schema'>
Schema object attributes: [... 'schema_str', ...]
Has schema_str? True
```

### Step-by-Step How to Fix

**Step 1: Encounter the error**
```bash
docker run ... swifttrack-streams-processor:1.1

# Error output:
# [2026-04-03 05:22:20] ERROR: Deserialization failed: 'Schema' object has no attribute 'schema'
# Traceback:
#   File "stream_processor.py", line 110, in deserialize_avro
#     reader_schema = json.loads(schema_obj.schema.schema_str)
#                                             ^^^^^^
# AttributeError: 'Schema' object has no attribute 'schema'

# ✗ The code tries: schema_obj.schema.schema_str
# But schema_obj doesn't have a .schema attribute!
```

**Step 2: Understand what you're working with**
```python
# The problematic code:
schema_obj = sr_client.get_schema(schema_id)

# You think the structure is:
# schema_obj.schema
#   └─ schema_str (a string property)

# But actually it's:
# schema_obj
#   └─ schema_str (a property directly)

# The naming is confusing!
```

**Step 3: Create a test script to introspect**
```python
# test_schema_structure.py
from confluent_kafka.schema_registry import SchemaRegistryClient

sr_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

# Get a real schema object from registry
schema_obj = sr_client.get_schema(1)  # schema_id = 1 from our messages

# Inspect it thoroughly
print(f"Type: {type(schema_obj)}")
print(f"Module: {schema_obj.__module__}")
print(f"Class: {schema_obj.__class__.__name__}")

# List all attributes
print(f"\nAll attributes:")
for attr in dir(schema_obj):
    if not attr.startswith('_'):
        print(f"  - {attr}")
        try:
            value = getattr(schema_obj, attr)
            print(f"    Value: {value!r}")
        except:
            print(f"    (method or not readable)")

# Check specific paths
print(f"\nChecking specific attributes:")
print(f"Has 'schema_str'? {hasattr(schema_obj, 'schema_str')}")
print(f"Has 'schema'? {hasattr(schema_obj, 'schema')}")
print(f"Has 'schema.schema_str'? Can't have - no .schema attribute")
```

**Step 4: Run the introspection script**
```bash
cd streams-processor
python test_schema_structure.py

# Output:
# Type: <class 'confluent_kafka.schema_registry.schema_registry_client.Schema'>
# Module: confluent_kafka.schema_registry.schema_registry_client
# Class: Schema
#
# All attributes:
#  - schema_str
#    Value: '{"type": "record", "name": "PackageEvent", ...}'
#  - schema_id
#    Value: 1
#  - subject
#    Value: 'package-events-value'
#  - version
#    Value: 1
#
# Checking specific attributes:
# Has 'schema_str'? True  ← HERE'S THE ANSWER!
# Has 'schema'? False     ← NOT HERE
# Has 'schema.schema_str'? Can't have - no .schema attribute
```

**Step 5: Fix the attribute path**
```python
# ❌ WRONG (causes AttributeError):
reader_schema = json.loads(schema_obj.schema.schema_str)

# ✅ CORRECT (use schema_str directly):
reader_schema = json.loads(schema_obj.schema_str)
```

**Step 6: Test the fix in isolation**
```python
# test_deserialization.py
import json
import fastavro
from io import BytesIO
from confluent_kafka.schema_registry import SchemaRegistryClient

sr_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

# Get schema object
schema_id = 1
schema_obj = sr_client.get_schema(schema_id)

# ✅ ACCESS IT CORRECTLY:
schema_str = schema_obj.schema_str  # Direct access, not nested!
reader_schema = json.loads(schema_str)

print(f"Schema loaded successfully: {reader_schema}")

# Now try to deserialize a message
test_message_bytes = b'\x00\x00\x00\x00\x01...'  # Real Avro message

try:
    result = fastavro.schemaless_reader(
        BytesIO(test_message_bytes[5:]),
        reader_schema
    )
    print(f"✓ Message deserialized: {result}")
except Exception as e:
    print(f"✗ Deserialization failed: {e}")
```

**Step 7: Check for version compatibility**
```python
# Some versions might have different attribute names
# Be defensive:

# Try different possible attribute paths
if hasattr(schema_obj, 'schema_str'):
    schema_str = schema_obj.schema_str
    print("✓ Using schema_obj.schema_str")
elif hasattr(schema_obj, 'schema'):
    schema_str = schema_obj.schema
    print("✓ Using schema_obj.schema")
else:
    schema_str = str(schema_obj)
    print("✓ Using fallback str(schema_obj)")

reader_schema = json.loads(schema_str)
```

**Step 8: Update the processor with defensive code**
```python
def deserialize_avro(self, msg_bytes):
    """Simple Avro deserialization."""
    if not msg_bytes:
        return None
    try:
        if msg_bytes[0] == 0:
            import struct
            import fastavro
            from io import BytesIO
            
            sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
            schema_id = struct.unpack('>I', msg_bytes[1:5])[0]
            schema_obj = sr_client.get_schema(schema_id)
            
            # ✅ FIXED: Try multiple paths for version compatibility
            if hasattr(schema_obj, 'schema_str'):
                reader_schema = json.loads(schema_obj.schema_str)
            elif hasattr(schema_obj, 'schema'):
                reader_schema = json.loads(schema_obj.schema)
            else:
                reader_schema = json.loads(str(schema_obj))
            
            return fastavro.schemaless_reader(BytesIO(msg_bytes[5:]), reader_schema)
        else:
            return json.loads(msg_bytes.decode('utf-8'))
    except Exception as e:
        logger.debug(f"Deserialization error: {e}")
        return None
```

**Step 9: Rebuild and test**
```bash
docker build --no-cache -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:2.0 .

docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:2.0

# Output now shows:
# [2026-04-03 05:24:37] INFO: [INPUT #1] PKG-4669442e | ...
# ✓ SUCCESS: Messages processed!
```

**Step 10: Document the learning for future API use**
```markdown
# API Inspection Checklist

When using unfamiliar APIs:
1. Never assume nested structure (obj.attr.attr2)
2. Always inspect what's actually available:
   - `type(obj)` → see the class
   - `dir(obj)` → see all attributes
   - `hasattr(obj, 'name')` → check before accessing
3. Use defensive coding with fallbacks
4. Test with real objects, not assumptions
```

### Solution Implemented
Instead of fixing one place, implemented defensive multi-fallback:

```python
if hasattr(schema_obj, 'schema_str'):
    schema_str = schema_obj.schema_str
elif hasattr(schema_obj, 'schema'):
    schema_str = schema_obj.schema
else:
    schema_str = str(schema_obj)
```

**Advantage**: Works with different versions of confluent-kafka

### Lessons Learned
✅ When API feels wrong, inspect the object (`dir()`, `type()`)  
✅ Don't guess - use `hasattr()` to check before accessing  
✅ Defensive coding handles version differences gracefully  
✅ Debug logging with type information is invaluable  
✅ Python's `dir()` function can reveal API structure instantly

---

## Issue 3.4: Consumer Group Started from Latest, Not Earliest

**Category**: Consumer Configuration  
**Severity**: High (no messages consumed)  
**Timeline**: Early Phase 3

### Problem Description
Consumer was initialized but consumed 0 messages even though 20 existed in topic.

**Configuration**:
```python
self.consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',  # ← Explicitly set to earliest
    'enable.auto.commit': False,
})
```

**But**: Consumer group showed no offsets initially, then appeared starting from end.

### Root Cause
First consumer in group was not properly initializing with `earliest` setting. Partition assignment happened before offset reset could occur.

### Step-by-Step How to Fix

**Step 1: Recognize the problem**
```bash
# You have 20 messages in topic
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic package-events \
  --from-beginning \
  --max-messages 2
# Shows 2 messages successfully

# But your processor consumes 0:
docker run ... swifttrack-streams-processor:1.0
# [2026-04-02 20:34:16] INFO: Processing stream...
# [2026-04-02 20:34:38] INFO: Processed 0 messages in 22.2s

# ✗ Problem: Processor isn't reading available messages
```

**Step 2: Check consumer group status**
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --describe

# Output might show:
# (empty output - group not yet created)
# OR
# streams-processor-group package-events 0 - - 
# streams-processor-group package-events 1 -  -
# streams-processor-group package-events 2 -  -

# ✓ Group exists but offsets show: CURRENT-OFFSET=-, LAG=-
# This means: group created but no offset yet set
```

**Step 3: Examine the processor consumer config**
```python
# Current code in processor:
self.consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',  # This SHOULD work
    'enable.auto.commit': False,
})
self.consumer.subscribe([INPUT_TOPIC])

# The 'earliest' setting should make it start from beginning
# But if group already has offsets (even at end), 
# 'earliest' doesn't apply!
```

**Step 4: Add logging to see what's happening**
```python
# Add callback to see partition assignment
def on_assign(consumer, partitions):
    logger.info(f"Partitions assigned: {[(p.topic, p.partition) for p in partitions]}")
    for p in partitions:
        logger.info(f"  {p.topic}:{p.partition} offset={p.offset}")

def on_revoke(consumer, partitions):
    logger.info(f"Partitions revoked: {[(p.topic, p.partition) for p in partitions]}")

self.consumer.subscribe([INPUT_TOPIC], on_assign=on_assign, on_revoke=on_revoke)
```

**Step 5: Run with logging to see what offsets are assigned**
```bash
docker run ... swifttrack-streams-processor:debug

# Output shows:
# [2026-04-03 05:22:20] INFO: Partitions assigned: [('package-events', 0), ('package-events', 1), ('package-events', 2)]
# [2026-04-03 05:22:20] INFO: package-events:0 offset=-1
# [2026-04-03 05:22:20] INFO: package-events:1 offset=-1
# [2026-04-03 05:22:20] INFO: package-events:2 offset=-1

# ✓ Good news: offset=-1 means not yet set (will use auto.offset.reset)
# ✓ Should start from beginning with 'earliest'
```

**Step 6: Debug message polling**
```python
# Add detailed polling logs
while True:
    msg = self.consumer.poll(timeout=2.0)
    
    if msg is None:
        empty += 1
        logger.debug(f"poll() returned None: {empty}/30 empty polls")
        if empty > 30:
            break
        continue
    
    logger.info(f"✓ Got message from {msg.topic()}:{msg.partition()}@{msg.offset()}")
    # ... process message
```

**Step 7: Check if group already has committed offsets**
```bash
# BEFORE running processor:
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --all-groups \
  --describe | grep streams-processor-group

# Output shows:
# streams-processor-group package-events 0 12 12 0
# streams-processor-group package-events 1 12 12 0  ← GROUP ALREADY HAS OFFSETS!
# streams-processor-group package-events 2 8 8 0

# ✗ Problem: Group was created in previous run and committed offsets!
# When group already has offsets, 'earliest' is ignored!
```

**Step 8: Solution 1 - Reset the consumer group**
```bash
# Method: Reset group to the beginning
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --reset-offsets \
  --to-earliest \
  --execute \
  --topic package-events

# Output:
# TOPIC PARTITION NEW-OFFSET
# package-events 0 0
# package-events 1 0
# package-events 2 0

# ✓ Group offsets reset to 0

# Verify reset worked:
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --describe

# Output now shows offset=0 for all partitions
```

**Step 9: Solution 2 - Use a new group name**
```python
# Alternative: Instead of resetting, use different group
CONSUMER_GROUP = 'streams-processor-group-v2'

# Benefits:
# - Fresh start without reset command
# - Can compare with old group if needed
# - Easy to iterate: v2, v3, v4, etc for testing
```

**Step 10: Solution 3 - Add manual seek in on_assign**
```python
def on_assign(consumer, partitions):
    logger.info(f"Partitions assigned: {len(partitions)}")
    # Explicitly seek to beginning
    for partition in partitions:
        partition.offset = 0  # Seek to start
        consumer.fseek(partition)
    logger.info("Seeked all partitions to offset 0")

self.consumer.subscribe([INPUT_TOPIC], on_assign=on_assign)
```

**Step 11: Implement all three safeguards together**
```python
# Final robust version:

CONSUMER_GROUP = 'streams-processor-group'

def on_assign(consumer, partitions):
    logger.info(f"Assigned partitions: {[(p.topic, p.partition) for p in partitions]}")
    # Could manually seek here if needed

def on_revoke(consumer, partitions):
    logger.info(f"Revoked partitions: {[(p.topic, p.partition) for p in partitions]}")

self.consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',  # Safeguard 1: explicit setting
    'enable.auto.commit': False,  # Manual commit for control
    'session.timeout.ms': 30000,
})

# Safeguard 2: Callbacks for visibility
self.consumer.subscribe([INPUT_TOPIC], on_assign=on_assign, on_revoke=on_revoke)
```

**Step 12: Run with clean group**
```bash
# Option A: Reset existing group
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --reset-offsets --to-earliest --execute \
  --topic package-events

# Option B: Use new group name
export CONSUMER_GROUP=streams-processor-group-v2

# Then run processor:
docker run ... swifttrack-streams-processor:2.0

# Output now shows:
# [2026-04-03 05:24:37] INFO: Partitions assigned: [('package-events', 0), ('package-events', 1), ('package-events', 2)]
# [2026-04-03 05:24:37] INFO: [INPUT #1] PKG-4669442e | Package Picked Up | ...
# [2026-04-03 05:24:37] INFO: [INPUT #2] PKG-4669442e | In Transit | ...
# ...
# [2026-04-03 05:24:40] INFO: Processed 20 messages in 62.3s

# ✓ SUCCESS: All 20 messages consumed and processed!
```

### Final Solution
Combine all three safeguards:

```python
# Reset group before running
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
  --group streams-processor-group --reset-offsets --to-earliest --execute \
  --topic package-events

# Then run with callbacks for visibility
def on_assign(consumer, partitions):
    logger.info(f"Partitions assigned: {[(p.topic, p.partition) for p in partitions]}")

self.consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})

self.consumer.subscribe([INPUT_TOPIC], on_assign=on_assign, on_revoke=on_revoke)
```

### Lessons Learned
✅ Consumer group state persists - reset needed when changing logic  
✅ `auto.offset.reset` only applies to new groups or reset groups  
✅ Partition assignment happens asynchronously - use callbacks  
✅ "earliest" doesn't work if group already has offsets  
✅ Always log partition assignment for visibility

---

## Issue 3.5: Empty Poll Timeout Too Short

**Category**: Stream Processing Timing  
**Severity**: Low (minor user experience issue)  
**Timeline**: Mid Phase 3

### Problem Description
Processor timed out after only ~20 seconds of waiting, even though lag detection showed more messages might arrive.

**Original Code**:
```python
empty = 0
while True:
    msg = self.consumer.poll(timeout=2.0)
    if msg is None:
        empty += 1
        if empty > 10:  # Only 20 seconds of waiting
            break
```

**Result**: Processed messages but exited before consuming all available messages in some cases.

### Root Cause
`empty > 10` means 10 × 2 seconds = 20 seconds maximum wait. With schema lookups and processing delays, processor might not reach all messages.

### Solution
Increased timeout threshold:

```python
if empty > 30:  # 60 seconds of waiting instead of 20
    logger.info(f"Processor timeout after {time.time()-start:.1f}s")
    break
```

**Also added better logging**:
```python
logger.debug(f"No message received ({empty}/30 empty polls)")
```

### Why 60 Seconds?
- 20 messages × 2-3 seconds per message = ~50-60 seconds max
- Includes schema registry lookups
- Includes Avro deserialization
- Safe margin for edge cases

### Lessons Learned
✅ Timeout values should be data-driven (# messages × time-per-message)  
✅ Don't hardcode timeout logic without understanding data volume  
✅ Stream processing needs generous timeouts for first-time setup  
✅ Log empty poll counts for debugging

---

## Issue 3.6: Partition Distribution Uneven

**Category**: Data Distribution / Partitioning  
**Severity**: Low (affects performance, not correctness)  
**Timeline**: Late Phase 3

### Problem Description
Consumer group offsets showed uneven distribution:
```
Partition 0: offset 4
Partition 1: offset 12  ← Heavy traffic
Partition 2: offset 4
```

Partition 1 had 3x more messages than others. This was correct (producer randomly distributed 20 messages), but showed:
- Partition key distribution could be better
- Round-robin not guaranteed with hash partitioning

### Root Cause
Producer used random package_id as key:
```python
producer.produce(
    topic='package-events',
    key=f'PKG-{random.randbytes(4).hex()}'.encode(),  # Random
)
```

Hash of random keys naturally distributed unevenly.

### Was It a Problem?
**No** - Uneven distribution is expected and correct:
- Kafka partitions by hash of key
- Random keys → random hash distribution
- Processor handles multi-partition consumption correctly

### What We Could Do (But Didn't)
Would only matter if we needed:
- Round-robin distribution
- Specific key ordering
- Partition-level locality

### Lessons Learned
✅ Uneven partition distribution is normal  
✅ Kafka handles multi-partition consumption transparently  
✅ Only optimize distribution if it's causing real issues  
✅ Random data → random distribution (expected and fine)

---

## Issue 3.7: Avro Deserialization Performance (Schema Registry Calls)

**Category**: Performance / Optimization  
**Severity**: Low (works but slow)  
**Timeline**: Late Phase 3

### Problem Description
Processor made ~100+ HTTP calls to Schema Registry (one per message deserialization).

**Logs**:
```
[2026-04-03 05:22:20] DEBUG: Starting new HTTP connection (1): schema-registry:8081
[2026-04-03 05:22:20] DEBUG: http://schema-registry:8081 "GET /schemas/ids/1 HTTP/1.1" 200 156
```

Each deserialization:
1. Extract schema ID from message
2. Make HTTP GET to Schema Registry
3. Parse response
4. Use schema for deserialization

### Root Cause
No schema caching - fetch schema for every single message.

### Why Not Optimized Yet
- Phase 3 is about learning, not performance
- Caching adds complexity
- 20 messages with HTTP calls still completes in <100ms per call

### How to Fix (For Future Phases)
```python
# Add schema cache
self.schema_cache = {}

def get_schema_cached(self, schema_id):
    if schema_id not in self.schema_cache:
        sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.schema_cache[schema_id] = sr_client.get_schema(schema_id)
    return self.schema_cache[schema_id]
```

### Lessons Learned
✅ Identify performance bottlenecks (schema registry calls)  
✅ Not everything needs optimization immediately  
✅ Caching schemas reduces network calls by 100x  
✅ For 20 messages, performance difference is negligible  
✅ Document optimization opportunities for future phases

---

# Phase 4 Issues

## Issue 4.1: Kafka Connect Container Build Failures

**Category**: Infrastructure / Docker  
**Severity**: Critical (blocks entire phase)  
**Timeline**: Phase 4 startup

### Problem Description

Kafka Connect Docker image fails to build with various errors:

**Error Option A: confluentinc-hub timeout**
```
Step 4/4 : RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
ERROR: failed to download confluentinc/kafka-connect-jdbc:10.7.4
```

**Error Option B: Network issues**
```
Get "https://api.github.com": dial tcp: lookup api.github.com: no such host
```

**Error Option C: Base image missing**
```
Error response from daemon: pull access denied for confluentinc/cp-kafka-connect
```

### Root Cause

1. **Network isolation**: Docker build inside container can't reach external services
2. **Mirror/registry issues**: Default confluentinc hub may be throttled or offline
3. **Missing base image**: confluentinc/cp-kafka-connect:7.9.0 not cached locally

### How to Fix: Step-by-Step

**Step 1: Pre-pull the base image**
```bash
# Pull before building
docker pull confluentinc/cp-kafka-connect:7.9.0

# Verify it's available
docker images | grep cp-kafka-connect
```

**Step 2: Create simplified Dockerfile (no hub install)**
```dockerfile
# kafka-connect/Dockerfile (modified)
FROM confluentinc/cp-kafka-connect:7.9.0

# The image already has PostgreSQL driver built-in
# No need for confluentinc-hub install

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8083/connectors || exit 1
```

**Step 3: Verify PostgreSQL driver exists in image**
```bash
# Build the image
cd kafka-connect && docker build -t swifttrack-kafka-connect:1.0 . && cd ..

# Check if PostgreSQL driver is included
docker run --rm swifttrack-kafka-connect:1.0 \
  find /usr/share -name "*postgresql*" -o -name "kafka-connect-jdbc*"

# Expected output:
# /usr/share/java/kafka/libs/postgresql-42.6.0.jar
# /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/...
```

**Step 4: If PostgreSQL driver missing, install manually**
```dockerfile
# kafka-connect/Dockerfile (with explicit driver installation)
FROM confluentinc/cp-kafka-connect:7.9.0

# Download PostgreSQL driver directly
RUN curl -o /usr/share/java/kafka/libs/postgresql-driver.jar \
  https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Or use apt-get
RUN apt-get update && apt-get install -y libpq5 && rm -rf /var/lib/apt/lists/*

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8083/connectors || exit 1
```

**Step 5: Build and test**
```bash
cd kafka-connect && docker build -t swifttrack-kafka-connect:1.0 . && cd ..

# Test the image starts
docker run --rm -d --name test-connect \
  -e CONNECT_BOOTSTRAP_SERVERS=kafka:9092 \
  swifttrack-kafka-connect:1.0

# Wait 5 seconds
sleep 5

# Check if it's running
docker logs test-connect | head -20

# Cleanup
docker rm -f test-connect
```

**Step 6: Update docker-compose to use pre-built image**
```yaml
# In docker-compose.yml
kafka-connect:
  image: swifttrack-kafka-connect:1.0  # Use pre-built image
  # OR build locally:
  # build:
  #   context: ./kafka-connect
  #   dockerfile: Dockerfile
  ...
```

**Step 7: Start all services and verify Connect is ready**
```bash
docker-compose up -d

# Wait for Connect to be healthy
for i in {1..30}; do
  if docker exec kafka-connect curl -s http://localhost:8083/connectors > /dev/null; then
    echo "Kafka Connect is ready"
    break
  fi
  echo "Waiting for Kafka Connect... ($i/30)"
  sleep 2
done
```

### Lessons Learned

✅ Pre-pull base images before building  
✅ confluentinc-hub install can fail in isolated networks  
✅ PostgreSQL driver may already be included in cp-kafka-connect image  
✅ Use explicit driver URLs if hub install fails  
✅ Always test rebuilt Docker images before publishing  
✅ Document which drivers are pre-installed vs. optional  

---

## Issue 4.2: PostgreSQL Tables Not Created or Wrong Schema

**Category**: Database / Initialization  
**Severity**: Critical (data can't be inserted)  
**Timeline**: Phase 4 connector startup

### Problem Description

**Symptom Option A: Tables don't exist**
```
ERROR: Relation "package_events_delivered" does not exist
```

**Symptom Option B: Wrong column types**
```
ERROR: column "event_time_ms" is of type bigint but expression is of type text
```

**Symptom Option C: Missing constraints**
```
ERROR: Duplicate key value violates unique constraint
```

### Root Cause

1. PostgreSQL initializes before init-postgres.sql executes
2. init-postgres.sql not mounted properly in docker-compose
3. Schema script has errors and rolls back silently
4. Connector tries to insert before DDL completes

### How to Fix: Step-by-Step

**Step 1: Verify init script is mounted**
```bash
# Check docker-compose.yml
grep -A 5 "postgres:" docker-compose.yml | grep "volumes:"

# Expected:
# volumes:
#   - ./kafka-connect/init-postgres.sql:/docker-entrypoint-initdb.d/01-init.sql
```

**Step 2: Check if volume mount exists**
```bash
# List mounted volumes in postgres container
docker exec postgres mount | grep "init-postgres"

# Expected output:
# /.../init-postgres.sql on /docker-entrypoint-initdb.d/01-init.sql ...
```

**Step 3: Verify init script executed**
```bash
# Check PostgreSQL logs for "init" references
docker logs postgres | grep -i "init\|sql"

# Expected:
# [postgres] /docker-entrypoint-initdb.d/01-init.sql
# [postgres] Processing /docker-entrypoint-initdb.d/01-init.sql
```

**Step 4: Test init script directly**
```bash
# Extract the script from running container
docker exec postgres cat /docker-entrypoint-initdb.d/01-init.sql | head -20

# Or check if it exists:
docker exec postgres ls -la /docker-entrypoint-initdb.d/
```

**Step 5: Re-initialize PostgreSQL with new script**

Option A: Delete and recreate (nukes all data):
```bash
# Stop PostgreSQL
docker-compose down

# Remove persistent volume
docker volume rm swifttrack-kafka-lab_pgdata

# Fix docker-compose.yml or init script
# ...

# Restart
docker-compose up -d postgres

# Wait for initialization
sleep 15

# Verify tables exist
docker exec -it postgres psql -U swifttrack -d swifttrack -c "\dt"
```

Option B: Manual table creation (if volume mount failed):
```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U swifttrack -d swifttrack

# Run the init script manually (copy from init-postgres.sql):
# CREATE TABLE IF NOT EXISTS package_events_delivered (...);
# CREATE TABLE IF NOT EXISTS package_events_by_location (...);
# ... etc

# Then exit
\q
```

**Step 6: Verify table creation**
```bash
docker exec postgres psql -U swifttrack -d swifttrack << 'EOF'
SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;
EOF

# Expected output:
# kafka_connect_offsets
# package_count_by_status
# package_delivery_hops
# package_events_by_location
# package_events_delivered
```

**Step 7: Check column definitions**
```bash
docker exec postgres psql -U swifttrack -d swifttrack << 'EOF'
\d package_events_delivered
EOF

# Expected:
#                  Table "public.package_events_delivered"
#        Column       |            Type             | Collation | Nullable
# --------------------+-----------------------------+-----------+------------------
#  id                 | integer                     |           | not null
#  package_id         | character varying(50)       |           | not null
#  status             | character varying(50)       |           | not null
#  location           | character varying(100)      |           | not null
#  event_time_ms      | bigint                      |           |
#  created_at         | timestamp without time zone |           | default now()
```

**Step 8: Verify unique constraints**
```bash
docker exec postgres psql -U swifttrack -d swifttrack << 'EOF'
SELECT tablename, indexname FROM pg_indexes 
WHERE schemaname = 'public' AND tablename LIKE 'package%'
ORDER BY tablename, indexname;
EOF
```

### Lessons Learned

✅ PostgreSQL init scripts execute in /docker-entrypoint-initdb.d/ only on first run  
✅ Volume mounts must be correct for init to find the SQL file  
✅ Always verify table creation before starting connectors  
✅ Use `\dt` and `\d tablename` to inspect schema  
✅ Column types must match Kafka schema (bigint for timestamps)  
✅ Primary keys and unique constraints must match connector config  

---

## Issue 4.3: Kafka Connect Connectors Stay in FAILED State

**Category**: Connector Configuration / Runtime  
**Severity**: Critical (data doesn't sink)  
**Timeline**: After connector submission

### Problem Description

**Symptom: Connector shows FAILED state**
```bash
$ curl -s http://localhost:8083/connectors/package-events-delivered-sink/status

{
  "name": "package-events-delivered-sink",
  "connector": {
    "state": "FAILED",
    "worker_id": "kafka-connect:8083",
    "error_code": -1,
    "error": "org.apache.kafka.connect.errors.ConnectException: Paste of this error logs makes message too long"
  },
  "tasks": [],
  "type": "sink"
}
```

### Root Cause: Multiple Possibilities

**Option A: PostgreSQL not reachable**
```
org.postgresql.util.PSQLException: Connection to postgres:5432 refused. 
Check that the hostname and port are correct and that the post accepts TCP/IP connections.
```

**Option B: Connector configuration has wrong table name**
```
ERROR: relation "package_events_deliverd" does not exist
(Note: missing 'e' in 'delivered')
```

**Option C: Schema mismatch (Avro schema fields don't match table columns)**
```
ERROR: column "unknown_field" does not exist
```

**Option D: Missing JDBC driver**
```
java.lang.ClassNotFoundException: org.postgresql.Driver
```

**Option E: Connector task failed to initialize**
```
java.lang.RuntimeException: Failed to initialize schema objects
```

### How to Fix: Step-by-Step

**Step 1: Check connector logs**
```bash
# Get detailed error messages
docker logs kafka-connect 2>&1 | grep -A 10 "package-events-delivered"

# Or use Kafka Connect API to get task status
curl -s http://localhost:8083/connectors/package-events-delivered-sink/status | jq '.tasks'
```

**Step 2: Verify PostgreSQL connectivity from Connect container**
```bash
# Test connection from inside Connect container
docker exec kafka-connect nc -zv postgres 5432

# Should output: Connection to postgres 5432 port [tcp/postgresql] succeeded!

# Or use psql if installed:
docker exec kafka-connect \
  psql -h postgres -U swifttrack -d swifttrack -c "SELECT 1"
```

**Step 3: If connection fails, check PostgreSQL health**
```bash
# Verify PostgreSQL is running
docker ps | grep postgres

# Check PostgreSQL logs
docker logs postgres | tail -20

# Verify it's accepting connections
docker exec postgres pg_isready -U swifttrack

# Should output: accepting connections
```

**Step 4: Verify connector configuration syntax**
```bash
# Get current connector config
curl -s http://localhost:8083/connectors/package-events-delivered-sink | jq '.config'

# Check key fields:
# - "Topics" should be valid topic names
# - "table.name.format" should match PostgreSQL table
# - "pk.fields" should be valid column names
# - "connection.url" should be correct

# Example expected:
# {
#   "connection.url": "jdbc:postgresql://postgres:5432/swifttrack",
#   "table.name.format": "package_events_delivered",
#   "topics": "package-events-delivered",
#   "pk.fields": "package_id"
# }
```

**Step 5: Delete and resubmit connector with corrected config**
```bash
# Delete broken connector
curl -X DELETE http://localhost:8083/connectors/package-events-delivered-sink

# Wait for cleanup
sleep 5

# Verify it's deleted
curl -s http://localhost:8083/connectors | grep package-events-delivered

# Re-submit with correct config
curl -X POST \
  -H "Content-Type: application/json" \
  -d @kafka-connect/connectors/package-delivered-sink.json \
  http://localhost:8083/connectors

# Check new status
sleep 5
curl -s http://localhost:8083/connectors/package-events-delivered-sink/status | jq '.connector.state'
```

**Step 6: If still failing, check for schema mismatches**
```bash
# Get Kafka message schema
docker run --rm --network swifttrack-kafka-lab_default \
  python:3.11-slim \
  bash -c '
    pip install confluent-kafka > /dev/null
    python -c "
from confluent_kafka.schema_registry import SchemaRegistryClient
sr_client = SchemaRegistryClient({\"url\": \"http://schema-registry:8081\"})
schema = sr_client.get_schema(1)
import json
print(json.loads(schema.schema_str))
    "
  '

# Compare with PostgreSQL table structure:
docker exec postgres psql -U swifttrack -d swifttrack -c "\d package_events_delivered"

# Ensure:
# - All Avro fields exist as columns
# - Field types are compatible (string → varchar, long → bigint)
# - Primary key field matches pk.fields in connector config
```

**Step 7: Enable debug logging in connector**
```bash
# Update docker-compose.yml:
environment:
  ...
  CONNECT_LOG_LEVEL: DEBUG

# Rebuild and restart
docker-compose down
docker-compose up -d kafka-connect

# Wait for startup
sleep 10

# Re-submit connector
bash kafka-connect/init-connectors.sh

# Check verbose logs
docker logs kafka-connect | grep -A 20 "package-events-delivered"
```

**Step 8: Verify connector is RUNNING (not FAILED)**
```bash
curl -s http://localhost:8083/connectors/package-events-delivered-sink/status | jq '.'

# Expected output:
# {
#   "name": "package-events-delivered-sink",
#   "connector": {
#     "state": "RUNNING",  ← THIS
#     "worker_id": "kafka-connect:8083"
#   },
#   "tasks": [
#     {
#       "id": 0,
#       "state": "RUNNING",  ← AND THIS
#       "worker_id": "kafka-connect:8083"
#     }
#   ],
#   "type": "sink"
# }
```

### Lessons Learned

✅ Always check connector logs first: `docker logs kafka-connect`  
✅ Test database connectivity from Connect container  
✅ Verify table names match exactly (PostgreSQL is case-sensitive)  
✅ Schema compatibility must be verified before submission  
✅ Use Kafka Connect REST API to get detailed error info  
✅ Delete and resubmit is often faster than debugging in place  
✅ Enable DEBUG logging for complex issues  

---

## Issue 4.4: Data Not Appearing in PostgreSQL Tables (Silent Failure)

**Category**: Data Flow / Debugging  
**Severity**: High (looks working but no data)  
**Timeline**: After connectors show RUNNING

### Problem Description

**Symptom: Everything looks OK but no data**
```bash
# Connectors are RUNNING
curl -s http://localhost:8083/connectors | jq '.[] | select(.status.connector.state=="RUNNING")'
# Output shows all RUNNING

# Output topics have data
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic package-events-delivered --from-beginning --max-messages 1
# Output: receives message successfully

# But tables are empty
docker exec postgres psql -U swifttrack -d swifttrack -c "SELECT COUNT(*) FROM package_events_delivered"
# Output: 0
```

### Root Cause: Multiple Possibilities

**Option A: Consumer group hasn't consumed yet**
- Connector created but messages were published before its group was created
- Group offset is at the end, not beginning

**Option B: Messages in topic but connector group at latest offset**
- Phase 3 processor ran before connector started
- Connector's auto.offset.reset='latest' (default)
- It's waiting for NEW messages, not reading old ones

**Option C: Connector reading but encountering errors silently**
- Deserialization errors logged but not visible
- Connector continues processing 0 records

**Option D: Upsert logic working but with wrong primary key**
- Records are updating same row repeatedly
- Only 1 total row instead of 20

### How to Fix: Step-by-Step

**Step 1: Check connector consumer group**
```bash
# List all consumer groups
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 --list

# Look for: connect-cluster (this is the default Connect group)
# Should see: connect-cluster
```

**Step 2: Check group offsets for output topics**
```bash
# Describe the connect-cluster group
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group connect-cluster \
  --describe

# Look at offsets for package-events-delivered:
# TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID
# package-events-delivered 0 0 20 20 connector-0
# ↑ If CURRENT-OFFSET is 0 and LOG-END-OFFSET is 20, it's behind
# ↑ If both are 20, it's caught up (good)
# ↑ If LAG is large, connector didn't consume yet
```

**Step 3: Reset connector consumer group (clear offset)**
```bash
# Option A: Delete and resubmit connector (resets group)
curl -X DELETE http://localhost:8083/connectors/package-events-delivered-sink
sleep 5
curl -X POST \
  -H "Content-Type: application/json" \
  -d @kafka-connect/connectors/package-delivered-sink.json \
  http://localhost:8083/connectors

# Wait for consumption to start
sleep 10

# Check consumer group again
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group connect-cluster \
  --describe

# Offsets should advance within seconds
```

**Step 4: Verify Phase 3 processor has produced data**
```bash
# Check if output topics have messages
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic package-events-delivered

# Should show:
# Topic: package-events-delivered Partitions: 1 Replication factor: 1
# Leader: 1 Replicas: [1] Isr: [1]

# Check message count
docker exec kafka-1 bash -c \
  'kafka-run-class kafka.tools.JournalConsumer \
    --bootstrap-server kafka-1:9092 \
    --topic package-events-delivered | wc -l'

# If 0 messages: Phase 3 processor hasn't produced yet
# Need to run Phase 3 processor first
```

**Step 5: If Phase 3 hasn't produced yet, re-run it**
```bash
# First reset Phase 3 consumer group
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --reset-offsets --to-earliest --execute \
  --topic package-events

# Run Phase 3 processor
docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-processor:2.0

# This will produce to all 4 output topics
# Now check if messages appear in PostgreSQL
```

**Step 6: Monitor connector consumption in real-time**
```bash
# Start a watch to see offsets changing
watch -n 2 'docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group connect-cluster \
  --describe | grep package-events-delivered'

# Should see CURRENT-OFFSET incrementing every few seconds
# If it stops incrementing, connector is stuck
```

**Step 7: Check for deserialization errors in connector logs**
```bash
# Look for ERROR messages related to deserialization
docker logs kafka-connect 2>&1 | grep -i "error\|exception" | head -20

# Look for specific patterns:
# - "Cannot find field" → schema mismatch
# - "No such attribute" → schema attribute error
# - "Type casting" → column type mismatch

# If errors found, check connector config value.converter settings
curl -s http://localhost:8083/connectors/package-events-delivered-sink | \
  jq '.config | {value_converter: .["value.converter"], schema_registry: .["value.converter.schema.registry.url"]}'
```

**Step 8: Enable Uber logging in PostgreSQL**
```bash
# Check actual INSERT statements being attempted
docker logs postgres 2>&1 | grep "INSERT\|UPDATE" | head -10

# If nothing, connector isn't actually executing SQL

# Verify table is accepting writes
docker exec postgres psql -U swifttrack -d swifttrack << 'EOF'
INSERT INTO package_events_delivered (package_id, status, location, event_time_ms)
VALUES ('test-pkg', 'Delivered', 'NYC', 1234567890);

SELECT * FROM package_events_delivered WHERE package_id = 'test-pkg';
EOF

# If manual insert fails: table has permission issues or schema problems
# If manual insert succeeds: connector is the problem
```

**Step 9: Check if records are updating (upsert working)**
```bash
# Run processor multiple times and check row count
docker exec postgres psql -U swifttrack -d swifttrack \
  -c "SELECT COUNT(*) FROM package_events_delivered"
# Should stay at 20, not increase to 40, 60, etc.
```

### Lessons Learned

✅ Connector group offsets must be at 0 to read from beginning  
✅ Phase 3 processor must run AFTER connectors are ready  
✅ Check message count in topics before assuming connector is stuck  
✅ Consumer group reset is the fastest fix  
✅ Monitor offsets in real-time to see if consumption is happening  
✅ Manual SQL inserts help isolate connector vs. database issues  
✅ Deserialization errors need to be seen, not suppressed  
✅ Upsert logic prevents duplicates, not visible in row count  

---

# Phase 5 Issues

## Issue 5.1: Test Failures with Mock Objects (Incorrect Mock Setup)

**Category**: Unit Testing / Mocking  
**Severity**: High (tests failing intermittently)  
**Timeline**: When running TopologyTestDriver tests

### Problem Description

**Symptom: Tests fail unpredictably with mock object errors**
```bash
# Running tests
python -m unittest discover -s tests -p "test_*.py" -v

# Output shows random failures:
FAIL: test_filter_operational_packages (tests.test_topology.TestStreamTopology)
  AssertionError: Mock object missing attribute 'value'
  
FAIL: test_aggregate_by_location (tests.test_topology.TestStreamTopology)
  Mock object not properly initialized for state store
  
FAIL: test_map_to_delivered_event (tests.test_topology.TestPackageEvent)
  Expected 'Delivered' but got <MagicMock object>
```

### Root Cause: Multiple Possibilities

**Option A: Mock object not configured with return_value**
- Mock created but not set up to return actual values
- Test expects data but gets MagicMock object reference

**Option B: State store mock incompletely configured**
- TopologyTestDriver requires state store to be fully initialized
- Mock doesn't implement all methods state store expects

**Option C: Timestamp mock issues**
- Kafka Record includes timestamp
- Test doesn't mock timestamp properly
- Downstream operations fail with timestamp-related errors

**Option D: Serialization/deserialization mocks incorrect**
- Schema registry mock returns wrong format
- Avro schema mocking doesn't match actual Avro format

### How to Fix: Step-by-Step

**Step 1: Identify which mock object is failing**
```bash
# Run test with verbose output and traceback
python -m unittest \
  tests.test_topology.TestStreamTopology.test_filter_operational_packages \
  -v -k

# Look at full traceback to see exactly which mock is problematic
# Common patterns:
# "Mock object has no attribute 'value'" → missing return_value
# "AttributeError: <MagicMock> is not subscriptable" → missing side_effect
```

**Step 2: Verify mock is created correctly**
```python
# Example of INCORRECT mock setup:
from unittest.mock import MagicMock

mock_record = MagicMock()
# Problem: mock_record.value is now another MagicMock, not actual data
# Returns: <MagicMock object ...>

# Example of CORRECT mock setup:
from unittest.mock import MagicMock

mock_record = MagicMock()
mock_record.value = json.dumps({
    "package_id": "pkg-001",
    "status": "Delivered",
    "location": "NYC"
}).encode()
mock_record.timestamp = 1234567890
mock_record.offset = 0

# Test can now access:
# json.loads(mock_record.value.decode()) → actual data
# mock_record.timestamp → actual timestamp
```

**Step 3: Fix TopologyTestDriver input creation**
```python
# INCORRECT way (generic):
test_input = [
    ('key1', MagicMock()),
    ('key2', MagicMock()),
]

# CORRECT way (with real Avro data):
import fastavro
from io import BytesIO

schema = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "package_id", "type": "string"},
        {"name": "status", "type": "string"},
    ]
}

event_data = {
    "package_id": "pkg-001",
    "status": "Delivered"
}

# Serialize to Avro bytes
writer = fastavro.writer(BytesIO(), schema, [event_data])
avro_bytes = writer.getvalue()

# Create proper input
test_input = [
    ('pkg-001', avro_bytes),  # Real Avro data, not mock
]

# Now TopologyTestDriver can deserialize it correctly
```

**Step 4: Fix state store mock**
```python
# INCORRECT way:
from unittest.mock import MagicMock

mock_state_store = MagicMock()
# Missing methods that state store needs:
# - get()
# - put()
# - all()

# CORRECT way:
from unittest.mock import MagicMock

mock_state_store = MagicMock()
mock_state_store.get.return_value = None  # Initial state
mock_state_store.put.return_value = None  # Put returns None
mock_state_store.all.return_value = iter([])  # Empty iterator initially

# Or even better, use dict for actual state:
class DictStateStore:
    def __init__(self):
        self.data = {}
    
    def get(self, key):
        return self.data.get(key)
    
    def put(self, key, value):
        self.data[key] = value
    
    def all(self):
        return iter(self.data.items())

# Use in test:
test_store = DictStateStore()
# Now fully functional state store
```

**Step 5: Fix Avro deserialization in tests**
```python
# INCORRECT - mocking too high level:
from unittest.mock import patch

with patch('fastavro.reader') as mock_reader:
    mock_reader.return_value = MagicMock()
    # Problem: complex to set up correctly
    
# CORRECT - use real Avro serialization:
import fastavro
from io import BytesIO

schema = {
    "type": "record",
    "name": "PackageEvent",
    "namespace": "com.swifttrack",
    "fields": [
        {"name": "package_id", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "event_time_ms", "type": "long"}
    ]
}

# Create test event
event = {
    "package_id": "pkg-001",
    "status": "Delivered",
    "location": "NYC",
    "event_time_ms": 1234567890
}

# Serialize to bytes
bio = BytesIO()
fastavro.writer(bio, schema, [event])
avro_bytes = bio.getvalue()

# Pass to processor and verify it deserializes correctly
# No mocks needed - use real Avro format
```

**Step 6: Verify mock object attributes**
```bash
# Create a test to inspect mock objects:
python << 'EOF'
from unittest.mock import MagicMock

# Check what attributes are available
mock = MagicMock()
print(dir(mock))  # Shows all methods

# Verify expected methods exist
assert hasattr(mock, 'return_value')  # MagicMock has this
assert hasattr(mock, 'side_effect')  # MagicMock has this
assert hasattr(mock, 'assert_called_with')  # MagicMock has this

# Check actual vs mock:
mock.value = "actual_value"
print(mock.value)  # "actual_value"

mock_no_setup = MagicMock()
print(mock_no_setup.value)  # <MagicMock name='value' id='...'>
EOF
```

**Step 7: Run tests with mock debugging enabled**
```bash
# Add debug logging to see what mocks are doing
python -m unittest discover -s tests -p "test_*.py" -v 2>&1 | \
  grep -E "(MagicMock|assert_called|return_value)" | head -20

# Or add to test code:
import logging
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger()
mock_obj = MagicMock()
logger.debug(f"Mock object: {mock_obj}")
logger.debug(f"Mock has value: {hasattr(mock_obj, 'value')}")
logger.debug(f"Value content: {mock_obj.value}")
```

**Step 8: Replace mocks with real objects where possible**
```python
# Strategy: Use real objects instead of mocks
# This simplifies testing and avoids mock-related issues

from tests.test_topology import TestPackageEvent
import json

# Instead of:
# event_data = MagicMock()

# Use:
event_data = {
    "package_id": "pkg-001",
    "status": "Delivered",
    "location": "NYC",
    "event_time_ms": 1234567890
}

# Serialize to JSON like actual Producer would
event_bytes = json.dumps(event_data).encode()

# Now test with real data
test_event = TestPackageEvent()
result = test_event.process_event(event_bytes)  # Real processing, no mocks
```

### Lessons Learned

✅ Avoid mocks unless absolutely necessary - use real objects  
✅ MagicMock defaults to returning MagicMock, not actual data  
✅ TopologyTestDriver works best with real Avro format, not mocks  
✅ State store needs actual implementation (dict or custom class)  
✅ Use real serialization (fastavro) instead of mocking it  
✅ Mock setup is complex - minimize mock usage in Kafka tests  
✅ When mocking is necessary, explicitly set return_value  
✅ Timestamps in Kafka records are critical - must mock them  

---

## Issue 5.2: Performance Tests Timeout (Too Much Event Data)

**Category**: Performance Testing  
**Severity**: High (tests hang indefinitely)  
**Timeline**: When running performance baseline tests (Step 6)

### Problem Description

**Symptom: Performance tests hang and timeout**
```bash
# Running performance test
timeout 30 python -m unittest \
  tests.test_topology.TestPerformanceBaseline.test_filter_throughput \
  -v

# Output: hangs for 30 seconds, then:
TIMEOUT: Test did not complete within 30 seconds
```

### Root Cause: Multiple Possibilities

**Option A: Too many events in test dataset**
- Performance test creates all 10,000 events before starting timer
- Event creation takes longer than execution
- Timer measures event creation + processing, not just processing

**Option B: Topology operations too complex for test**
- Test includes too many operations
- Each operation contends for CPU
- Net result slower than expected

**Option C: State store not sized for test data**
- State store tries to keep all 10,000 events
- In-memory state store becomes memory-bottleneck
- Garbage collection pauses cause timeouts

**Option D: Aggregation state maintains too much history**
- State store retains old aggregations
- Memory grows with each unique key
- Cascading slowdown

### How to Fix: Step-by-Step

**Step 1: Identify the bottleneck**
```bash
# Add timing instrumentation
python << 'EOF'
import time
import unittest
from tests.test_topology import TestPerformanceBaseline

test = TestPerformanceBaseline()

# Time just the event creation
start = time.time()
events = test.create_test_events(10000)
create_time = time.time() - start
print(f"Event creation: {create_time:.2f}s")

# Time the actual processing
start = time.time()
result = test.process_events(events)
process_time = time.time() - start
print(f"Event processing: {process_time:.2f}s")

# If create_time > 1 second, that's the problem
# If process_time > 1 second, topology is slow
EOF
```

**Step 2: Batch test events to avoid memory spike**
```python
# INCORRECT way - all events upfront:
def test_filter_throughput(self):
    start = time.time()
    
    # This creates all 10,000 before timing starts!
    all_events = [create_event(i) for i in range(10000)]
    
    # Now process them
    for event in all_events:
        result = self.topology.process(event)
    
    elapsed = time.time() - start
    throughput = 10000 / elapsed
    self.assertGreater(throughput, 1000)  # Expecting >1000 ops/sec

# CORRECT way - batch creation:
def test_filter_throughput(self):
    start = time.time()
    
    processed_count = 0
    batch_size = 100
    
    # Process in batches to stay memory-efficient
    for batch_num in range(100):  # 100 batches of 100 events
        # Create small batch
        batch = [create_event(i + batch_num*100) for i in range(batch_size)]
        
        # Process immediately
        for event in batch:
            result = self.topology.process(event)
            processed_count += 1
    
    elapsed = time.time() - start
    throughput = processed_count / elapsed
    self.assertGreater(throughput, 1000)  # Expecting >1000 ops/sec
```

**Step 3: Reduce test dataset size for quick feedback**
```python
# INCORRECT way - fixed large dataset:
def test_map_throughput(self):
    events = [create_test_event(i) for i in range(10000)]
    
    start = time.time()
    for event in events:
        self.topology.map_operation(event)
    elapsed = time.time() - start
    
    self.assertGreater(10000 / elapsed, 1000)  # Very strict

# CORRECT way - tiered testing:
def test_map_throughput(self):
    # Quick smoke test with small dataset
    small_test_count = 100
    events = [create_test_event(i) for i in range(small_test_count)]
    
    start = time.time()
    for event in events:
        self.topology.map_operation(event)
    elapsed = time.time() - start
    
    # Calculate throughput and be reasonable with expectations
    throughput = small_test_count / elapsed
    # For 100 events, expecting >500 ops/sec is reasonable
    # For 10,000 events, expecting >1000 ops/sec
    self.assertGreater(throughput, 500)

# Separate fixture for large-scale testing:
def test_map_throughput_large_scale(self):
    # Only run if performance testing is enabled
    if not os.getenv('RUN_PERFORMANCE_TESTS'):
        self.skipTest("Set RUN_PERFORMANCE_TESTS=1 to run")
    
    large_test_count = 10000
    events = [create_test_event(i) for i in range(large_test_count)]
    
    start = time.time()
    for event in events:
        self.topology.map_operation(event)
    elapsed = time.time() - start
    
    throughput = large_test_count / elapsed
    self.assertGreater(throughput, 1000)
```

**Step 4: Move event creation outside timing**
```python
# INCORRECT way - creation inside timing:
def test_aggregate_throughput(self):
    start = time.time()  # Start timer too early
    
    # This takes 0.5 seconds!
    events = [create_complex_event(i) for i in range(1000)]
    
    # Now measure from here
    for event in events:
        self.topology.aggregate(event)
    
    elapsed = time.time() - start
    # Result: inflated elapsed, artificially low throughput

# CORRECT way - creation separate:
def test_aggregate_throughput(self):
    # Pre-create events
    events = [create_complex_event(i) for i in range(1000)]
    
    # NOW start timing
    start = time.time()
    
    # Measure only the processing
    for event in events:
        self.topology.aggregate(event)
    
    elapsed = time.time() - start
    throughput = 1000 / elapsed
    # Now accurate measurement
    self.assertGreater(throughput, 1000)
```

**Step 5: Clear state store between test runs**
```python
# INCORRECT way - state grows between tests:
class TestPerformanceBaseline(unittest.TestCase):
    def setUp(self):
        # State store retains data from previous test!
        self.driver = TopologyTestDriver(build_topology())
    
    def test_aggregate_run1(self):
        events = [create_event(i) for i in range(1000)]
        for e in events:
            self.driver.process(e)
        # State store now has 1000 entries
    
    def test_aggregate_run2(self):
        events = [create_event(i) for i in range(1000)]
        for e in events:
            # State store already has 1000 from previous test!
            # Now adding another 1000 = 2000 total
            self.driver.process(e)
        # Slower due to larger state store

# CORRECT way - clean state:
class TestPerformanceBaseline(unittest.TestCase):
    def setUp(self):
        # Fresh driver for each test
        self.driver = TopologyTestDriver(build_topology())
    
    def tearDown(self):
        # Clean up state
        self.driver.close()
    
    def test_aggregate_run1(self):
        events = [create_event(i) for i in range(1000)]
        for e in events:
            self.driver.process(e)
        # State store has 1000 entries
    
    def test_aggregate_run2(self):
        # Fresh driver = fresh state store
        events = [create_event(i) for i in range(1000)]
        for e in events:
            self.driver.process(e)
        # State store has only 1000 entries (clean)
```

**Step 6: Set appropriate timeout per test**
```python
import unittest
import time

class TestPerformanceBaseline(unittest.TestCase):
    @unittest.skip("Use timeout instead")
    def test_slow_operation(self):
        # This test is expected to be slow
        pass
    
    def test_filter_throughput(self):
        # Quick test - should finish in <1 second
        # Use timeout to fail fast if it hangs
        
        events = [create_event(i) for i in range(100)]
        start = time.time()
        
        for e in events:
            self.driver.process(e)
        
        elapsed = time.time() - start
        # Timeout: must complete in less than 5 seconds
        self.assertLess(elapsed, 5.0)
        
        # Performance check: must achieve throughput
        throughput = 100 / elapsed
        self.assertGreater(throughput, 500)
```

**Step 7: Use generators for large datasets**
```python
# INCORRECT way - stores all in memory:
def test_filter_throughput(self):
    # Creates 10,000 objects in memory
    all_events = [create_event(i) for i in range(10000)]
    
    start = time.time()
    for event in all_events:
        self.driver.process(event)
    elapsed = time.time() - start

# CORRECT way - generator:
def test_filter_throughput(self):
    def event_generator(count):
        """Generate events on-demand, not all at once"""
        for i in range(count):
            yield create_event(i)
    
    start = time.time()
    processed = 0
    
    for event in event_generator(10000):
        self.driver.process(event)
        processed += 1
    
    elapsed = time.time() - start
    throughput = processed / elapsed
    # Memory-efficient and actual measurement
```

**Step 8: Skip performance tests by default**
```bash
# In test file
import os
import unittest

class TestPerformanceBaseline(unittest.TestCase):
    @unittest.skipIf(
        not os.getenv('RUN_PERF_TESTS'),
        "Skipped - set RUN_PERF_TESTS=1 to run"
    )
    def test_large_scale_throughput(self):
        # This test can be slow, skip by default
        pass
    
    def test_quick_sanity_check(self):
        # Always run this, quick
        pass

# Run only quick tests during normal execution
python -m unittest discover -s tests -p "test_*.py" -v

# Run performance tests separately
RUN_PERF_TESTS=1 python -m unittest discover -s tests -p "test_*.py" -v
```

### Lessons Learned

✅ Measure only the relevant operation, not event creation  
✅ Batch events to avoid memory spikes  
✅ Pre-create test data outside timing  
✅ Clear state store between tests  
✅ Use generators for large datasets to save memory  
✅ Set reasonable timeout expectations  
✅ Separate quick tests from slow tests  
✅ Performance tests should be optional/skippable  

---

## Issue 5.3: Edge Cases Not Covered (Missing Test Scenarios)

**Category**: Test Coverage / Quality  
**Severity**: Medium (gaps in functionality validation)  
**Timeline**: After basic tests pass

### Problem Description

**Symptom: Basic tests pass but edge cases fail in production**
```bash
# Unit tests all pass
python -m unittest discover -s tests -p "test_*.py" -v
# Output: 50 tests passed

# But in production:
# - Long location names cause truncation
# - Empty location names crash the aggregation
# - Duplicate package IDs overwrite each other incorrectly
# - Out-of-order events break state consistency
```

### Root Cause: Multiple Possibilities

**Option A: Test dataset too simplistic**
- Tests use perfect scenario: valid data, normal timestamps, expected values
- Real data includes edge cases: nulls, empties, extremes

**Option B: Happy path only**
- Tests don't exercise error conditions
- Production encounters errors rarely tested

**Option C: Insufficient parameterized testing**
- Same test for many scenarios manually
- Easy to miss combinations

### How to Fix: Step-by-Step

**Step 1: Identify missing edge cases**
```python
# Review list of possible edge cases:
edge_cases = [
    # Empty/null values
    ("", "Empty location", {"location": ""}),
    (None, "Null location", {"location": None}),
    
    # Very long values
    ("x" * 1000, "Very long location", {"location": "x" * 1000}),
    
    # Special characters
    ("NYC@123!#$", "Special chars", {"location": "NYC@123!#$"}),
    ("倫敦", "Unicode", {"location": "倫敦"}),
    
    # Timestamps
    (0, "Zero timestamp", {"event_time_ms": 0}),
    (-1, "Negative timestamp", {"event_time_ms": -1}),
    (999999999999, "Far future", {"event_time_ms": 999999999999}),
    
    # Duplicates
    ("same-pkg", "Duplicate package ID", {"package_id": "same-pkg"}),
    
    # Order
    ("out-of-order", "Events with decreasing timestamp", 
     [{"package_id": "pkg", "event_time_ms": 100},
      {"package_id": "pkg", "event_time_ms": 50}]),  # Time goes backward
]
```

**Step 2: Add edge case test methods**
```python
class TestEdgeCases(unittest.TestCase):
    
    def test_empty_location(self):
        """Event with empty location string"""
        event = create_event(package_id="pkg-001", location="")
        result = self.driver.process(event)
        
        # Should handle gracefully (not crash)
        self.assertIsNotNone(result)  # Should return something
        # Should not corrupt state
        self.assertNotEqual(result.location, None)
    
    def test_very_long_location(self):
        """Location name exceeds expected length"""
        long_location = "City" * 1000  # 4000 characters
        event = create_event(package_id="pkg-001", location=long_location)
        result = self.driver.process(event)
        
        # Should handle without error
        self.assertIsNotNone(result)
        # Check if truncation happens as expected
        self.assertLessEqual(len(result.location), 1024)
    
    def test_special_characters_in_location(self):
        """Location with special characters"""
        special_locations = [
            "New York & City",
            "São Paulo",
            "北京",
            "Moscow/Russia",
        ]
        
        for location in special_locations:
            event = create_event(package_id="pkg-001", location=location)
            result = self.driver.process(event)
            self.assertIsNotNone(result)
            self.assertEqual(result.location, location)
    
    def test_zero_timestamp(self):
        """Event with timestamp of 0"""
        event = create_event(
            package_id="pkg-001",
            event_time_ms=0  # Epoch start
        )
        result = self.driver.process(event)
        self.assertIsNotNone(result)
        self.assertEqual(result.event_time_ms, 0)
    
    def test_future_timestamp(self):
        """Event with far-future timestamp"""
        future_time = int(time.time() * 1000) + (365 * 24 * 60 * 60 * 1000)
        event = create_event(package_id="pkg-001", event_time_ms=future_time)
        result = self.driver.process(event)
        self.assertIsNotNone(result)
```

**Step 3: Use parameterized testing for combinations**
```python
# Install parameterized library
# pip install parameterized

from parameterized import parameterized
import unittest

class TestEdgeCasesParameterized(unittest.TestCase):
    
    @parameterized.expand([
        ("normal", "New York", True),
        ("empty", "", False),
        ("very_long", "X" * 1000, True),
        ("special_chars", "NYC@123", True),
        ("unicode", "北京", True),
    ])
    def test_location_handling(self, name, location, should_succeed):
        """Parameterized test for location edge cases"""
        event = create_event(package_id="pkg-001", location=location)
        
        if should_succeed:
            result = self.driver.process(event)
            self.assertIsNotNone(result)
        else:
            # Expected to fail gracefully
            with self.assertRaises(ValueError):
                result = self.driver.process(event)
    
    @parameterized.expand([
        ("normal", "Delivered", True),
        ("lowercase", "delivered", True),
        ("uppercase", "DELIVERED", True),
        ("invalid", "Unknown", False),
        ("empty", "", False),
    ])
    def test_status_validation(self, name, status, should_pass):
        """Parameterized test for status values"""
        event = create_event(package_id="pkg-001", status=status)
        
        if should_pass:
            result = self.driver.process(event)
            self.assertIsNotNone(result)
        else:
            with self.assertRaises(ValueError):
                self.driver.process(event)
```

**Step 4: Test event ordering scenarios**
```python
class TestEventOrdering(unittest.TestCase):
    
    def test_out_of_order_events(self):
        """Events arrive in reverse chronological order"""
        events = [
            create_event(package_id="pkg-001", event_time_ms=300),
            create_event(package_id="pkg-001", event_time_ms=100),
            create_event(package_id="pkg-001", event_time_ms=200),
        ]
        
        # Process out of order
        results = [self.driver.process(e) for e in events]
        
        # State should still be consistent
        # (depends on whether order matters in your processing)
        self.assertEqual(len(results), 3)
    
    def test_duplicate_events(self):
        """Same event appears twice"""
        event = create_event(package_id="pkg-001", location="NYC")
        
        # Process same event twice
        result1 = self.driver.process(event)
        result2 = self.driver.process(event)
        
        # Should either deduplicate or count both (depends on logic)
        # But shouldn't crash
        self.assertIsNotNone(result1)
        self.assertIsNotNone(result2)
    
    def test_rapid_state_transitions(self):
        """Multiple events for same package in rapid succession"""
        events = [
            create_event(package_id="pkg-001", status="Pending"),
            create_event(package_id="pkg-001", status="Processing"),
            create_event(package_id="pkg-001", status="In Transit"),
            create_event(package_id="pkg-001", status="Delivered"),
        ]
        
        # Process all in sequence
        for event in events:
            result = self.driver.process(event)
            self.assertIsNotNone(result)
        
        # Final state should be "Delivered"
        final = self.driver.get_state("pkg-001")
        self.assertEqual(final.status, "Delivered")
```

**Step 5: Test boundary values**
```python
class TestBoundaryValues(unittest.TestCase):
    
    def test_package_id_boundary_lengths(self):
        """Package IDs at length boundaries"""
        lengths = [1, 10, 100, 255, 256, 1000]
        
        for length in lengths:
            pkg_id = "pkg-" + "x" * (length - 4)
            event = create_event(package_id=pkg_id)
            result = self.driver.process(event)
            self.assertIsNotNone(result)
    
    def test_numeric_boundaries(self):
        """Numeric fields at extremes"""
        test_values = [
            0,  # Minimum
            1,  # Just above minimum
            2**31 - 1,  # Max 32-bit int
            2**63 - 1,  # Max 64-bit int
        ]
        
        for value in test_values:
            event = create_event(package_id="pkg-001", event_time_ms=value)
            result = self.driver.process(event)
            self.assertIsNotNone(result)
```

**Step 6: Test error recovery**
```python
class TestErrorRecovery(unittest.TestCase):
    
    def test_recovery_after_invalid_event(self):
        """Processor continues after bad event"""
        # Good event
        event1 = create_event(package_id="pkg-001")
        result1 = self.driver.process(event1)
        self.assertIsNotNone(result1)
        
        # Bad event (should fail gracefully)
        try:
            bad_event = create_event(package_id="", location="")  # Invalid
            result_bad = self.driver.process(bad_event)
        except ValueError:
            pass  # Expected
        
        # Good event again - should still work
        event2 = create_event(package_id="pkg-002")
        result2 = self.driver.process(event2)
        self.assertIsNotNone(result2)
        
        # Processor should be in healthy state
        self.assertEqual(result2.package_id, "pkg-002")
```

**Step 7: Generate comprehensive test coverage report**
```bash
# Install coverage tool
pip install coverage

# Run tests with coverage
coverage run -m unittest discover -s tests -p "test_*.py"

# Generate report
coverage report --include=streams_processor/* --omit=*test*

# Expected output:
# Name                                    Stmts   Miss  Cover
# ─────────────────────────────────────────────────────────
# streams_processor/processor.py            120     5    96%
# streams_processor/transformations.py      85      3    96%
# streams_processor/aggregators.py         150     12    92%
# ─────────────────────────────────────────────────────────
# TOTAL                                     355     20    94%

# Look for low coverage areas and add tests
```

**Step 8: Create edge case test suite configuration**
```python
# tests/test_edge_cases_config.py

EDGE_CASE_SCENARIOS = {
    "empty_values": {
        "location": "",
        "package_id": "pkg-001",
    },
    "special_chars": {
        "location": "NYC@#$%^&*()",
        "package_id": "pkg-001",
    },
    "boundary_timestamps": [
        0,  # Epoch
        2**53-1,  # Max safe integer in JavaScript/JSON
        -1,  # Before epoch (if allowed)
    ],
    "unicode_locations": [
        "São Paulo",
        "北京",
        "Москва",
        "القاهرة",
    ],
}

# Use in tests:
for scenario_name, scenario_data in EDGE_CASE_SCENARIOS.items():
    # Generate test for each scenario
```

### Lessons Learned

✅ Edge cases are often where bugs hide  
✅ Use parameterized tests for multiple scenarios  
✅ Test boundary values explicitly  
✅ Order of events matters - test permutations  
✅ Empty/null handling must be deliberate  
✅ Unicode and special characters need specific tests  
✅ Error recovery is as important as happy path  
✅ Coverage reports reveal gaps  

---

## Issue 5.4: Integration Test Flakiness (Intermittent Failures)

**Category**: Testing / State Management  
**Severity**: High (unreliable test results)  
**Timeline**: When running full integration tests repeatedly

### Problem Description

**Symptom: Same test passes sometimes, fails other times**
```bash
# Test passes
python -m unittest tests.test_topology.TestTopologyIntegration.test_full_flow -v
# Output: OK

# Run again - different result
python -m unittest tests.test_topology.TestTopologyIntegration.test_full_flow -v
# Output: FAIL: AssertionError: Expected 20 events, got 17

# Run third time - back to passing
python -m unittest tests.test_topology.TestTopologyIntegration.test_full_flow -v
# Output: OK
```

### Root Cause: Multiple Possibilities

**Option A: Shared state not cleaned between tests**
- Previous test's state carries over
- Interferes with current test
- Results depend on test execution order

**Option B: TopologyTestDriver state not reset**
- State store retains values from previous processing
- Each new test start with partial state
- Results differ based on order

**Option C: Race condition in async operations**
- If using any threading/async
- Timing issues cause intermittent failures
- Hard to reproduce

**Option D: Non-deterministic test data**
- Test uses random data/timestamps
- Different seeds produce different results
- Inconsistent assertions

### How to Fix: Step-by-Step

**Step 1: Add proper setUp and tearDown**
```python
# INCORRECT way - no cleanup:
class TestTopologyIntegration(unittest.TestCase):
    def test_full_flow_1(self):
        self.driver = TopologyTestDriver()
        events = [create_event(i) for i in range(20)]
        for e in events:
            self.driver.process(e)
        # State persists to next test

    def test_full_flow_2(self):
        # Assumes clean state, but has state from test_full_flow_1
        self.driver = TopologyTestDriver()
        events = [create_event(i) for i in range(20)]
        for e in events:
            self.driver.process(e)

# CORRECT way - clean state per test:
class TestTopologyIntegration(unittest.TestCase):
    def setUp(self):
        """Run before EACH test"""
        self.driver = TopologyTestDriver()
        self.test_events = []
    
    def tearDown(self):
        """Run after EACH test"""
        if self.driver:
            self.driver.close()
        self.test_events = []
        # Now state is completely clean
    
    def test_full_flow_1(self):
        events = [create_event(i) for i in range(20)]
        for e in events:
            self.driver.process(e)
        # State destroyed in tearDown
    
    def test_full_flow_2(self):
        # Fresh driver, clean state guaranteed
        events = [create_event(i) for i in range(20)]
        for e in events:
            self.driver.process(e)
```

**Step 2: Use deterministic test data**
```python
# INCORRECT way - random data:
import random

def test_varying_results(self):
    # Each run uses different data
    seeds = [random.randint(1, 1000) for _ in range(20)]
    
    for seed in seeds:
        event = create_event(timestamp=seed)
        result = self.driver.process(event)
        self.assertIsNotNone(result)

# Results vary based on random values
# Impossible to debug failures

# CORRECT way - fixed data:
def test_consistent_results(self):
    # Always use same data
    fixed_seeds = list(range(1, 21))  # 1 to 20, always
    
    for seed in fixed_seeds:
        event = create_event(timestamp=seed)
        result = self.driver.process(event)
        self.assertIsNotNone(result)

# Results are identical every run
# Easy to debug

# Or use seeded randomness:
def test_seeded_randomness(self):
    random.seed(42)  # Fixed seed
    
    for i in range(20):
        event = create_event(timestamp=random.randint(1, 1000))
        result = self.driver.process(event)
        self.assertIsNotNone(result)

# Same random sequence every time
```

**Step 3: Isolate state store between tests**
```python
# INCORRECT way - shared state:
class TestTopologyIntegration(unittest.TestCase):
    driver = TopologyTestDriver()  # Class-level - shared!
    
    def test_aggregate_1(self):
        # Uses shared driver
        self.driver.process(create_event(id="pkg1"))
    
    def test_aggregate_2(self):
        # Same driver - has "pkg1" from previous test
        self.driver.process(create_event(id="pkg2"))
        # Might expect 1 entry, actually has 2

# CORRECT way - instance state:
class TestTopologyIntegration(unittest.TestCase):
    def setUp(self):
        self.driver = TopologyTestDriver()  # Instance-level - fresh per test
    
    def tearDown(self):
        self.driver.close()  # Clean up fully
    
    def test_aggregate_1(self):
        self.driver.process(create_event(id="pkg1"))
        # Guaranteed only "pkg1" in state
    
    def test_aggregate_2(self):
        self.driver.process(create_event(id="pkg2"))
        # Guaranteed only "pkg2" in state
```

**Step 4: Add explicit state assertions**
```python
# INCORRECT - implicit assumptions:
def test_aggregate_correctly(self):
    event1 = create_event(id="pkg1", location="NYC")
    self.driver.process(event1)
    
    event2 = create_event(id="pkg1", location="LA")
    self.driver.process(event2)
    
    # Assuming state was cleared, but no explicit check
    # Might pass or fail depending on previous state

# CORRECT - explicit state checks:
def test_aggregate_correctly(self):
    # Verify clean state first
    self.assertEqual(len(self.driver.get_state_store().all()), 0)
    
    event1 = create_event(id="pkg1", location="NYC")
    self.driver.process(event1)
    
    # Check state after first event
    state_after_1 = dict(self.driver.get_state_store().all())
    self.assertEqual(len(state_after_1), 1)
    self.assertEqual(state_after_1["pkg1"].location, "NYC")
    
    event2 = create_event(id="pkg1", location="LA")
    self.driver.process(event2)
    
    # Check state after second event
    state_after_2 = dict(self.driver.get_state_store().all())
    self.assertEqual(len(state_after_2), 1)
    self.assertEqual(state_after_2["pkg1"].location, "LA")
```

**Step 5: Test execution order independence**
```python
# Create tests that work in any order
class TestOrderIndependent(unittest.TestCase):
    def setUp(self):
        self.driver = TopologyTestDriver()
    
    def tearDown(self):
        self.driver.close()
    
    def test_a_filter(self):
        """Each test is completely independent"""
        event = create_event(status="Delivered")
        result = self.driver.process(event)
        self.assertIsNotNone(result)
    
    def test_b_map(self):
        """Can run before or after test_a, same result"""
        event = create_event(package_id="pkg-001")
        result = self.driver.process(event)
        self.assertEqual(result.package_id, "pkg-001")
    
    def test_c_aggregate(self):
        """No dependencies on order of execution"""
        event = create_event(location="NYC")
        result = self.driver.process(event)
        self.assertEqual(result.location, "NYC")

# Run in different orders
python -m unittest tests.test_topology.TestOrderIndependent -v
# Results same regardless of test order
```

**Step 6: Add test logging for debugging failures**
```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestTopologyIntegration(unittest.TestCase):
    def setUp(self):
        self.driver = TopologyTestDriver()
        logger.info(f"setUp: Created fresh driver {id(self.driver)}")
    
    def tearDown(self):
        logger.info(f"tearDown: Closing driver {id(self.driver)}")
        self.driver.close()
    
    def test_flow(self):
        logger.debug("test_flow: Starting")
        logger.debug(f"test_flow: State store: {self.driver.get_state_store()}")
        
        event = create_event()
        logger.debug(f"test_flow: Processing event {event.package_id}")
        
        result = self.driver.process(event)
        logger.debug(f"test_flow: Result {result}")
        logger.debug(f"test_flow: Final state: {self.driver.get_state_store()}")
        
        self.assertIsNotNone(result)

# Run with verbose logging
python -m unittest tests.test_topology.TestTopologyIntegration -v 2>&1 | grep -E "(DEBUG|INFO|ERROR)"
```

**Step 7: Run tests multiple times to catch flakiness**
```bash
# Run tests 10 times in succession
for i in {1..10}; do
    echo "Run $i:"
    python -m unittest discover -s tests -p "test_*.py" -q
    if [ $? -ne 0 ]; then
        echo "FAILED on run $i"
        exit 1
    fi
done
echo "All 10 runs passed!"

# Or use a test runner that supports this
python -m pytest tests/ --count=10  # Requires pytest-repeat
```

**Step 8: Use test fixtures for complex setup**
```python
class TestWithFixtures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Run once before ALL tests in class"""
        logger.info("setUpClass: Creating shared resources")
        cls.schema = load_avro_schema()
        cls.test_data = load_test_data()
    
    @classmethod
    def tearDownClass(cls):
        """Run once after ALL tests in class"""
        logger.info("tearDownClass: Cleaning up shared resources")
        # Clean up expensive resources
    
    def setUp(self):
        """Run before EACH test"""
        # Create fresh driver
        self.driver = TopologyTestDriver()
        # Load schema from class-level resource
        self.schema = self.schema
    
    def tearDown(self):
        """Run after EACH test"""
        # Clean up driver
        self.driver.close()
    
    def test_with_shared_schema(self):
        # Uses schema from setUpClass (efficient)
        event = create_event_with_schema(self.schema)
        result = self.driver.process(event)
        self.assertIsNotNone(result)
```

### Lessons Learned

✅ setUp/tearDown is critical for test isolation  
✅ Use instance variables, not class variables, for test state  
✅ Close resources explicitly in tearDown  
✅ Use fixed/seeded data, not random, for reproducibility  
✅ State cleanliness between tests determines flakiness  
✅ Run tests multiple times to catch intermittent failures  
✅ Log state explicitly to debug flaky tests  
✅ Test execution order should not affect results  

---

## Issue 5.5: Coverage Gaps (Some Code Paths Untested)

**Category**: Test Coverage / Quality  
**Severity**: Medium (untested code paths)  
**Timeline**: After running coverage report

### Problem Description

**Symptom: Coverage report shows missing tests**
```bash
# Run coverage
coverage run -m unittest discover -s tests
coverage report

# Output shows gaps:
Name                              Stmts   Miss  Cover
─────────────────────────────────
streams_processor/processor.py      120     18    85%
streams_processor/transformations   85      12    86%
# Missing: error cases, fallback logic, edge paths
```

### Root Cause: Multiple Possibilities

**Option A: Error handling paths untested**
- Tests exercise happy path
- Error handlers and fallbacks skipped
- Catch blocks never execute in tests

**Option B: Conditional branches not exercised**
- if/else statements with both paths not tested
- Some conditions always match in test data

**Option C: Optional code not triggered**
- Logging statements not executed
- Retry logic not engaged
- Fallback implementations not tested

### How to Fix: Step-by-Step

**Step 1: Generate detailed coverage report**
```bash
# Generate HTML coverage report
coverage run -m unittest discover -s tests -p "test_*.py"
coverage html --include=streams_processor/*

# Output: htmlcov/index.html
# Open in browser to see exactly which lines aren't covered
# Red lines = not covered
# Yellow lines = partially covered
```

**Step 2: Identify all missing branches**
```bash
# Use branch coverage, not just line coverage
coverage run --branch -m unittest discover -s tests
coverage report --include=streams_processor/*

# Output shows missing branches:
# streams_processor/processor.py:42 if status == "Delivered": (missing: False branch)
# streams_processor/processor.py:50 if timestamp > now: (missing: True branch)
```

**Step 3: Add tests for error conditions**
```python
# INCOMPLETE coverage:
def test_filter_valid_status(self):
    """Only tests happy path"""
    event = create_event(status="Delivered")
    result = self.processor.filter(event)
    self.assertIsNotNone(result)

# COMPLETE coverage:
def test_filter_valid_status(self):
    """All branches tested"""
    # Branch 1: status matches
    event = create_event(status="Delivered")
    result = self.processor.filter(event)
    self.assertIsNotNone(result)

def test_filter_invalid_status(self):
    """Branch 2: status doesn't match"""
    event = create_event(status="Unknown")
    # Filter should skip this or raise error
    result = self.processor.filter(event)
    self.assertIsNone(result)  # or check for exception
```

**Step 4: Test all conditional branches**
```python
def test_timestamp_validation_branch_coverage(self):
    """Test both branches of timestamp comparison"""
    future = int(time.time() * 1000) + 1000000
    past = int(time.time() * 1000) - 1000000
    
    # Branch 1: timestamp in future
    event_future = create_event(event_time_ms=future)
    result_future = self.processor.validate_timestamp(event_future)
    self.assertTrue(result_future)  # Or expected outcome
    
    # Branch 2: timestamp in past
    event_past = create_event(event_time_ms=past)
    result_past = self.processor.validate_timestamp(event_past)
    self.assertTrue(result_past)  # Or expected outcome
```

**Step 5: Test exception paths**
```python
# INCOMPLETE - no exception testing:
def test_process_event(self):
    event = create_event()
    result = self.processor.process(event)
    self.assertIsNotNone(result)

# COMPLETE - exception testing:
def test_process_event_normal(self):
    """Happy path"""
    event = create_event()
    result = self.processor.process(event)
    self.assertIsNotNone(result)

def test_process_event_invalid_schema(self):
    """Exception path: bad schema"""
    bad_event = {"invalid": "structure"}
    with self.assertRaises(ValueError):
        self.processor.process(bad_event)

def test_process_event_null_value(self):
    """Exception path: null event"""
    with self.assertRaises(TypeError):
        self.processor.process(None)
```

**Step 6: Test retry logic and fallbacks**
```python
# Test that retries happen
def test_retry_on_failure(self):
    """Test fallback/retry behavior"""
    from unittest.mock import Mock, patch, call
    
    # Mock the underlying operation
    with patch.object(self.processor, 'send_to_kafka') as mock_send:
        # Fail once, succeed on retry
        mock_send.side_effect = [Exception("Connection error"), {"status": "success"}]
        
        result = self.processor.process_with_retry(
            event,
            max_retries=2
        )
        
        # Verify retry happened
        self.assertEqual(mock_send.call_count, 2)
        self.assertEqual(result["status"], "success")
```

**Step 7: Test logging for completeness**
```python
# Even log statements count toward coverage
class TestLoggingCoverage(unittest.TestCase):
    def test_debug_logging(self):
        """Log statements are executed"""
        with self.assertLogs(level='DEBUG') as log:
            event = create_event()
            self.processor.process_verbose(event)
        
        # Verify logging happened
        self.assertTrue(any('Processing' in m for m in log.output))

    def test_error_logging(self):
        """Error logging is triggered"""
        with self.assertLogs(level='ERROR') as log:
            # Cause error that gets logged
            try:
                bad_event = None
                self.processor.process(bad_event)
            except TypeError:
                pass
        
        # Verify error was logged
        self.assertTrue(any('ERROR' in m for m in log.output))
```

**Step 8: Add missing test methods systematically**
```python
# Based on coverage report, add tests for each gap
# Example: Coverage reports line 42 is missing

# Source code (streams_processor/processor.py#42):
def calculate_delivery_time(event):
    if event.status == "Delivered":
        return event.event_time_ms  # Line 42 - not covered
    else:
        return None

# Add test:
def test_calculate_delivery_time_delivered(self):
    """Covers line 42: delivered status"""
    event = create_event(status="Delivered", event_time_ms=1000)
    result = self.processor.calculate_delivery_time(event)
    self.assertEqual(result, 1000)

def test_calculate_delivery_time_pending(self):
    """Covers else branch"""
    event = create_event(status="Pending")
    result = self.processor.calculate_delivery_time(event)
    self.assertIsNone(result)
```

### Lessons Learned

✅ Coverage reports show exactly which lines need tests  
✅ Branch coverage is more important than line coverage  
✅ Test both sides of every if/else  
✅ Exception paths must be tested explicitly  
✅ Set coverage targets (e.g., 90%) and enforce them  
✅ Use coverage tools to guide test writing  
✅ Missing coverage often reveals unreachable code  

---

# Phase 6 Issues

## Issue 6.1: Prometheus Scrape Failures (Targets Down/Unreachable)

**Category**: Observability / Metrics Collection  
**Severity**: High (no metrics collected)  
**Timeline**: After Prometheus container starts

### Problem Description

**Symptom: Prometheus shows targets DOWN**
```bash
# Check Prometheus endpoint
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets'

# Output shows:
# [{"labels": {...}, "state": "down", "reason": "Get \"http://kafka-1:9999/metrics\": 
#   dial tcp: lookup kafka-1: no such host"}]

# All targets unreachable
# Prometheus dashboard shows RED status for all metrics sources
```

### Root Cause: Multiple Possibilities

**Option A: Container networking issue**
- Prometheus can't resolve Kafka container hostnames
- Docker-compose network not properly configured
- Containers on different networks

**Option B: Metrics endpoints not exposed**
- Kafka brokers not exporting metrics on expected ports
- JMX exporter not running in Kafka containers
- Kafka-exporter not configured

**Option C: Port mappings incorrect**
- Prometheus config references wrong ports
- Metrics endpoints on different ports than configured
- Port forwarding not set up

**Option D: Prometheus config scrape targets wrong**
- Target addresses in prometheus.yml don't match docker-compose
- Using localhost instead of service names
- Port numbers off by one

### How to Fix: Step-by-Step

**Step 1: Verify container network connectivity**
```bash
# Check if containers can reach each other
docker-compose exec prometheus ping kafka-1
# If "no such host": containers on different networks

# Verify all services are on same network
docker network ls
docker network inspect swifttrack-kafka-lab_default

# Should see all containers listed
docker-compose ps
# All services should show their network
```

**Step 2: Check Kafka metrics endpoint accessibility**
```bash
# Test from Prometheus container directly
docker-compose exec prometheus /bin/sh

# Inside container, try to reach Kafka metrics
curl -s http://kafka-1:9999/metrics | head -20
# If error: Kafka not exposing metrics on port 9999

# Try common Kafka ports
curl -s http://kafka-1:9092/metrics  # Broker port
curl -s http://kafka-1:9999/metrics  # JMX exporter port
curl -s http://kafka-1:8888/metrics  # Alternative exporter
```

**Step 3: Enable JMX metrics export in Kafka**
```bash
# In docker-compose.yml, Kafka service needs:
environment:
  KAFKA_JMX_PORT: 9999
  KAFKA_JMX_HOSTNAME: kafka-1  # Internal docker hostname
  KAFKA_JMX_OPTS: -Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false

# Restart Kafka
docker-compose restart kafka-1 kafka-2 kafka-3

# Verify JMX is running
docker exec kafka-1 netstat -tuln | grep 9999
# Should show: LISTEN 0.0.0.0:9999
```

**Step 4: Add JMX exporter sidecar**
```bash
# Option: Run jmx_exporter container alongside Kafka
# In docker-compose.yml:

jmx-exporter:
  image: sscaling/jmx-exporter:latest
  ports:
    - "5556:5556"
  environment:
    SERVICE_PORT: 5556
    KAFKA_BROKER_HOST: kafka-1
    KAFKA_BROKER_PORT: 9999
  depends_on:
    - kafka-1
  networks:
    - default
```

**Step 5: Update Prometheus scrape config**
```yaml
# In prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets:
        - 'kafka-1:9999'  # JMX exporter port
        - 'kafka-2:9999'
        - 'kafka-3:9999'
    
  - job_name: 'kafka-exporter'
    static_configs:
      - targets:
        - 'kafka-exporter:9308'  # Kafka exporter port
  
  - job_name: 'schema-registry'
    static_configs:
      - targets:
        - 'schema-registry:8081'
  
  - job_name: 'kafka-connect'
    static_configs:
      - targets:
        - 'kafka-connect:8083'
  
  - job_name: 'postgres'
    static_configs:
      - targets:
        - 'postgres-exporter:9187'
```

**Step 6: Rebuild and restart Prometheus**
```bash
# Rebuild with new prometheus.yml
docker-compose build prometheus

# Restart
docker-compose down prometheus
docker-compose up -d prometheus

# Wait for startup (30 seconds)
sleep 30

# Check targets again
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[0] | {labels, state}'
```

**Step 7: Verify metrics are flowing**
```bash
# Query Prometheus for any metric
curl -s 'http://localhost:9090/api/v1/query?query=up' | jq '.data.result'

# Expected output:
# [{"metric": {"job": "kafka", "instance": "kafka-1:9999"}, "value": [timestamp, "1"]}]
# Value "1" = target UP
# Value "0" = target DOWN

# If all 0s or empty, targets still down
```

**Step 8: Debug with Prometheus logs**
```bash
# Check Prometheus startup logs
docker logs prometheus 2>&1 | grep -i "error\|scrape\|target"

# Look for patterns:
# "cannot unmarshal ProtoBuf" → metric format wrong
# "scrape failed" → endpoint unreachable
# "context deadline exceeded" → timeout
# "connection refused" → port wrong
```

### Lessons Learned

✅ Docker-compose networking requires service names, not localhost  
✅ JMX export is separate from Kafka broker - must be configured  
✅ Port numbers must match between docker-compose and prometheus.yml  
✅ Scrape config requires exact container names from docker-compose  
✅ Test connectivity from inside Prometheus container  
✅ JMX exporter takes time to start - wait before checking  
✅ Multiple exporters may be needed (JMX, kafka-exporter, postgres-exporter)  
✅ Prometheus targets tab shows exactly why scrape fails  

---

## Issue 6.2: Grafana Datasource Connection Failing

**Category**: Observability / Dashboard Setup  
**Severity**: High (no data in dashboards)  
**Timeline**: After Grafana container starts

### Problem Description

**Symptom: Grafana can't query Prometheus**
```bash
# Grafana shows error when trying to view dashboards:
# "Error reading Prometheus: Bad Gateway"
# or
# "No data returned"

# Datasource shows red "X" in configuration
curl http://localhost:3000/api/datasources
# Response shows: {"message": "Datasource is not working"}
```

### Root Cause: Multiple Possibilities

**Option A: Grafana can't reach Prometheus container**
- Grafana and Prometheus on different networks
- Prometheus service name wrong in datasource URL
- Prometheus not fully started when Grafana connects

**Option B: Datasource URL incorrect**
- Using localhost when inside container
- Wrong port (9090 vs 9091, etc.)
- Wrong protocol (http vs https)

**Option C: Prometheus not ready**
- Prometheus still initializing
- Prometheus crashed or unhealthy
- Prometheus port not exposed

### How to Fix: Step-by-Step

**Step 1: Verify Prometheus is running and healthy**
```bash
# Check Prometheus container status
docker-compose ps prometheus
# Should show: Up (healthy)

# Check Prometheus health endpoint
curl -s http://localhost:9090/-/healthy
# Expected: "Prometheus Server is Healthy"

# If fails, check logs
docker logs prometheus 2>&1 | tail -20
```

**Step 2: Test Prometheus from Grafana container**
```bash
# Enter Grafana container
docker-compose exec grafana /bin/sh

# Try to reach Prometheus
curl -s http://prometheus:9090/api/v1/query?query=up | head -50

# If success: connection works, config is wrong
# If fails: networking issue
```

**Step 3: Use correct datasource URL in Grafana**
```bash
# Inside Grafana container, use service name:
# NOT: http://localhost:9090  (localhost = grafana container itself)
# NOT: http://127.0.0.1:9090  (loopback = grafana only)
# YES: http://prometheus:9090  (service name from docker-compose)

# Configure via API:
curl -X POST http://localhost:3000/api/datasources \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Prometheus",
    "type": "prometheus",
    "url": "http://prometheus:9090",
    "access": "proxy",
    "isDefault": true
  }'
```

**Step 4: Check docker-compose network setup**
```bash
# All services must be on same network
docker-compose config | grep -A 20 networks:

# Verify all services have same network alias
docker inspect swifttrack-kafka-lab_default | jq '.Containers | keys'

# Should show: grafana, prometheus, kafka-1, kafka-2, kafka-3, etc.
```

**Step 5: Wait for Prometheus to be fully ready**
```bash
# Prometheus needs time to initialize
# Add healthcheck to docker-compose:

prometheus:
  # ... existing config ...
  healthcheck:
    test: ["CMD", "wget", "--quiet", "--tries=1", "--spider", "http://localhost:9090/-/healthy"]
    interval: 5s
    timeout: 3s
    retries: 10
    start_period: 30s

# Wait for health
docker-compose up -d
sleep 45  # Wait for startup
```

**Step 6: Test datasource with simple query**
```bash
# In Grafana UI:
# 1. Go to Configuration > Datasources
# 2. Click "Prometheus"
# 3. Scroll to bottom, click "Save & Test"
# 4. Should show: "Data source is working"

# Or via API:
curl -s -X GET http://localhost:3000/api/datasources/1 \
  -H "Authorization: Bearer $(curl -s -X POST http://localhost:3000/api/auth/login \
  -d '{"user": "admin", "password": "admin"}' | jq '.token')"
```

**Step 7: Rebuild Grafana provisioning**
```bash
# Create provisioning directory
mkdir -p grafana/provisioning/datasources

# Create datasource file:
cat > grafana/provisioning/datasources/prometheus.yml << 'EOF'
apiVersion: 1
providers:
  - name: 'Prometheus'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /etc/grafana/provisioning/datasources

datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    access: proxy
    isDefault: true
    editable: true
EOF

# Mount in docker-compose
grafana:
  volumes:
    - ./grafana/provisioning:/etc/grafana/provisioning
```

**Step 8: Check Grafana logs for datasource errors**
```bash
# View Grafana startup logs
docker logs grafana 2>&1 | grep -i "datasource\|prometheus\|error"

# Common errors:
# "Connection refused" → Prometheus not running
# "Name resolution failed" → Wrong service name
# "Bad Gateway" → Prometheus port wrong
# "Unauthorized" → Authentication issue
```

### Lessons Learned

✅ Inside containers, use service name (prometheus), not localhost  
✅ Datasource URL must be http://service-name:port  
✅ Prometheus must fully start before Grafana tests connectivity  
✅ "Save & Test" button in Grafana UI shows exact error  
✅ Docker-compose networking requires same network for all services  
✅ Healthchecks help ensure startup order  
✅ Provisioning files make datasource setup repeatable  

---

## Issue 6.3: Missing Metrics in Dashboards (Empty Graphs)

**Category**: Observability / Metrics Collection  
**Severity**: High (metrics collected but not displayed)  
**Timeline**: After Prometheus and Grafana configured

### Problem Description

**Symptom: Dashboards show no data**
```bash
# Dashboard loads but all graphs are empty
# "No data" message in every panel

# Manually querying Prometheus works:
curl 'http://localhost:9090/api/v1/query?query=up'
# Returns: [{"metric": {"job": "kafka"}, "value": [timestamp, "1"]}]

# But Grafana shows empty graph
# Prometheus data source test passes
# But "Run Query" returns nothing
```

### Root Cause: Multiple Possibilities

**Option A: Metric names wrong in Grafana queries**
- Dashboard using kafka_broker_* but actual metric is kafka_brokers_*
- Label names don't match actual data
- Typos in metric names

**Option B: Time range not covering data**
- Dashboard set to last 5 minutes but metrics started 1 hour ago
- Prometheus data retention periods
- Timezone mismatch

**Option C: Metrics not being scrapped**
- Prometheus scrape config correct but targets DOWN
- Scrape interval too long
- Metrics endpoints returning wrong format

**Option D: JMX metrics not exported**
- Kafka running but JMX disabled
- Metrics endpoint not actually returning data
- Only test metrics available, not real Kafka metrics

### How to Fix: Step-by-Step

**Step 1: Verify metric names in Prometheus**
```bash
# List all available metrics
curl -s 'http://localhost:9090/api/v1/label/__name__/values' | jq '.data[]' | head -20

# Look for metrics like:
# kafka_brokers (number of brokers)
# kafka_controller_active_controller_count
# kafka_log_log_size_bytes
# kafka_network_fetch_consumer_total_bytes

# If no kafka_ metrics: exporters not working
```

**Step 2: Test metric directly in Prometheus UI**
```bash
# Open http://localhost:9090
# Go to Graph tab
# In Expression box, type: kafka_brokers
# Click Execute
# Should show graph with data points

# If no results: metric doesn't exist
# Try: up (should exist for all targets)
# Try: node_up (if node exporter configured)
```

**Step 3: Fix metric names in Grafana dashboard**
```bash
# Edit dashboard panel
# Query section should use exact metric names

WRONG:
kafka_broker_info  # Might not exist

CORRECT:
kafka_brokers  # Actual metric name

# Use Prometheus autocomplete
# Start typing metric name in Grafana
# Press Ctrl+Space to see available metrics
```

**Step 4: Expand time range to show data**
```bash
# If data was only collected recently
# Change time range from "Last 5 minutes" to "Last 24 hours"

# In Grafana:
# Top right corner: change time picker
# Or: use relative time "Last 6 hours"

# Check when metrics started:
# Prometheus > Status > Targets
# See "Started at" timestamp for each target
```

**Step 5: Verify metrics are actually collected**
```bash
# Check Prometheus targets
curl -s http://localhost:9090/api/v1/targets | \
  jq '.data.activeTargets[] | {job: .labels.job, state: .state, lastScrape: .lastScrapeTime}'

# Should show:
# - All targets with state="up"
# - lastScrapeTime recent (within last 15 seconds)

# If state="down": fix Issue 6.1
# If lastScrapeTime old: scrape interval too long
```

**Step 6: Check metric format and cardinality**
```bash
# Some metrics might have too many labels
# causing high cardinality (too many unique series)

# Query that could return too much data:
kafka_broker_info  # Might have 1000s of series

# Better query with filtering:
kafka_broker_info{instance="kafka-1:9999"}  # Specific broker

# Test in Prometheus UI:
# Try: rate(kafka_network_fetch_consumer_total_bytes[5m])
# If no data: try smaller time window [1m]
```

**Step 7: Check Grafana query time range**
```bash
# Some Grafana panels have their own time range
# That might not match dashboard time range

# In Grafana Panel:
# Click gear icon (Panel settings)
# Check: Time range override settings
# Should be: Use dashboard time range (not custom)

# Or set query with relative range:
# FROM: now() - 1h
# TO: now()
```

**Step 8: Export and inspect raw metric data**
```bash
# Download Prometheus metrics in raw format
curl -s http://localhost:9090/api/v1/query?query=up | jq '.' > metrics.json

# Check structure:
jq '.data.result[0]' metrics.json
# Should show:
# {"metric": {...labels...}, "value": [timestamp, "1"]}

# If metric array empty: no data for that metric
```

### Lessons Learned

✅ Metric names must be exact - check Prometheus UI first  
✅ Time range must cover when metrics were collected  
✅ Targets must be UP and recent to have data  
✅ Use metric autocomplete in Grafana to find correct names  
✅ High cardinality metrics need label filtering  
✅ Test queries in Prometheus UI before using in Grafana  
✅ Check "Last Scrape" time to see if metrics are fresh  

---

## Issue 6.4: Dashboard Panels Show Out-of-Date Data (Stale Cache)

**Category**: Observability / Data Freshness  
**Severity**: Medium (data old but present)  
**Timeline**: After metrics running for a while

### Problem Description

**Symptom: Dashboard data lags or is outdated**
```bash
# Prometheus has latest data:
curl -s 'http://localhost:9090/api/v1/query?query=up' | \
  jq '.data.result[0].value'
# Output: [1712099455, "1"]  (current timestamp)

# But Grafana shows old timestamp
# "Last updated: 5 minutes ago"
# Panel data hasn't refreshed

# Manual refresh makes it show latest
# But auto-refresh isn't working
```

### Root Cause: Multiple Possibilities

**Option A: Grafana auto-refresh interval too long**
- Set to 5 minutes when it should be 10 seconds
- Or disabled completely

**Option B: Query result caching in Grafana**
- Query results cached but cache not invalidating
- Browser cache preventing updates

**Option C: Prometheus query caching**
- Prometheus caching query results
- Old cached results returned to Grafana

**Option D: Time range in dashboard fixed**
- Time picker set to specific time period
- Not following current time

### How to Fix: Step-by-Step

**Step 1: Set appropriate auto-refresh interval**
```bash
# Grafana top right: click refresh icon
# Set auto-refresh to 10s (for active monitoring)
# Or 30s (for less frequent updates)

# Via API:
curl -X PATCH http://localhost:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d '{"dashboard": {"refresh": "10s"}}'
```

**Step 2: Disable query result caching**
```bash
# In Grafana config (grafana.ini):
[database]
log_queries = false

[grafana]
# Disable any caching
cache_ttl = 0

# Restart Grafana
docker-compose restart grafana
```

**Step 3: Disable Prometheus query caching**
```bash
# In prometheus.yml:
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  
  # Disable caching
  external_labels:
    cache_ttl: 0

# Or disable in command flags:
command:
  - '--storage.tsdb.max-block-duration=2h'
  - '--storage.tsdb.min-block-duration=2h'
  - '--query.max-cache-size=0'

docker-compose restart prometheus
```

**Step 4: Change time range to follow current time**
```bash
# In Grafana dashboard:
# Top right time picker
# Select "Last 1 hour" (relative)
# NOT "Jan 1 00:00 to Jan 1 01:00" (absolute)

# Relative time ranges auto-update as time passes
```

**Step 5: Enable request tracing to debug**
```bash
# Check how old the data Grafana received is:
# Grafana > Inspect tab in panel
# Look at "Request" timestamp vs "Response" timestamp
# Should be very close

# If Response timestamp old:
# - Prometheus caching
# - Browser cache
# - Network delay
```

**Step 6: Clear browser cache**
```bash
# Browsers can cache API responses
# Chrome: Ctrl+Shift+Delete (or Cmd+Shift+Delete on Mac)
# Firefox: Ctrl+Shift+Delete
# Clear: Cookies and cached files

# Or use incognito/private mode
# Open http://localhost:3000 in new incognito window
```

**Step 7: Check Prometheus data retention**
```bash
# Prometheus keeps data for limited time
# Check retention:
curl -s http://localhost:9090/api/v1/status/flags | \
  jq '.data | select(."storage.tsdb.retention.time")'  

# If retention is 2h: old data automatically deleted
# Grafana dashboard might show empty for old time ranges
```

**Step 8: Monitor query performance to see caching**
```bash
# Prometheus > Status > Runtime & Build info
# Look at: Query Stats

# Each query shows:
# - Instant queries (cache efficient)
# - Range queries (full data range)
# - Caching status

# Fast queries might be cached
# Slow queries might be full scans
```

### Lessons Learned

✅ Set auto-refresh to 10-30 seconds for live dashboards  
✅ Use relative time ranges ("Last hour") not absolute  
✅ Disable caching if freshness is critical  
✅ Browser cache can interfere - clear cache if suspicious  
✅ Prometheus log files show query timing  
✅ Monitor query response times to detect caching issues  
✅ Different panels can have different refresh rates  

---

## Issue 6.5: Alerting Rules Not Triggering (Alerts Silent)

**Category**: Observability / Alerting  
**Severity**: High (no alerts when problems occur)  
**Timeline**: After alert rules configured

### Problem Description

**Symptom: Alerts configured but never fire**
```bash
# Alert rule defined:
# "Alert if kafka_brokers < 2" (expect 3 brokers)

# Manually verify condition:
curl -s 'http://localhost:9090/api/v1/query?query=kafka_brokers' | \
  jq '.data.result[0].value'
# Output: [timestamp, "3"]  (3 brokers - OK)

# If stop one broker:
# kafka-1 goes down
# kafka_brokers becomes 2
# But alert still doesn't fire

# Prometheus shows alert state: PENDING (not FIRING)
# Alert not sent to Grafana
```

### Root Cause: Multiple Possibilities

**Option A: Alert threshold condition never met**
- Metric value never hits configured threshold
- Condition logic wrong (< instead of >
- Label filtering causes no data to match

**Option B: Alert requires hold period (FOR clause)**
- Alert condition must be true for N minutes
- Only briefly true, then recovers
- Never sustained long enough to fire

**Option C: Alert notification not configured**
- Rule exists but no notification channel set
- Alert fires in Prometheus but Grafana doesn't know
- Notification channel unreachable

**Option D: Alert rule syntax error**
- Rule doesn't parse correctly
- Invalid metric name
- Bad label matcher syntax

### How to Fix: Step-by-Step

**Step 1: Verify alert rule syntax**
```bash
# Check if Prometheus loaded alert rules correctly
curl -s http://localhost:9090/api/v1/rules | \
  jq '.data.groups[0].rules[] | {name: .name, state: .state}'

# Should show rule list with state: ok
# If empty: rules not loaded
# If error: syntax problem
```

**Step 2: Test alert condition manually**
```bash
# Query the metric directly
curl -s 'http://localhost:9090/api/v1/query?query=kafka_brokers' | \
  jq '.data.result[] | {metric: .metric, value: .value}'

# Check if value would trigger alert
# Alert rule: kafka_brokers < 2
# Current value: 3
# Shouldn't trigger (correct)

# To test: manually stop a broker to make value 2
docker-compose stop kafka-1
sleep 10

# Re-query:
curl -s 'http://localhost:9090/api/v1/query?query=kafka_brokers' | \
  jq '.data.result[] | {metric: .metric, value: .value}'
# Should now show: 2

# Wait for FOR duration (usually 1-5 minutes)
# Check Prometheus Alerts tab
docker-compose start kafka-1
```

**Step 3: Check alert rule FOR duration**
```yaml
# In prometheus.yml alert rules:
groups:
  - name: kafka-rules
    rules:
      - alert: KafkaUnavailable
        expr: kafka_brokers < 2
        for: 5m  # ← Must be true for 5 minutes
        
# If for: 5m and condition only true for 2m
# Alert won't fire

# Reduce for: duration for testing:
for: 1m  # Will fire after 1 minute of condition being true
```

**Step 4: Check for label filtering issues**
```yaml
# Alert with too restrictive labels might match nothing:
alert: KafkaError
expr: kafka_errors{instance="kafka-999:9999"}  # Instance doesn't exist!

# Check what labels actually exist:
curl -s http://localhost:9090/api/v1/label/instance/values | \
  jq '.data[] | select(contains("kafka"))'

# Use actual instance names:
expr: kafka_errors{instance=~"kafka-[123]:9999"}
```

**Step 5: Configure alert notification channel**
```bash
# Alert rules alone don't send notifications
# Need notification channel configured

# In Grafana, add notification channel:
Grafana UI:
  Alerting > Notification channels > New channel
  Select type: Webhook / Email / Slack / PagerDuty
  Configure endpoint
  Save

# Or via API:
curl -X POST http://localhost:3000/api/alert-notifications \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Alert to Console",
    "type": "webhook",
    "settings": {
      "url": "http://alertmanager:9093/api/v1/alerts"
    }
  }'
```

**Step 6: Set notification channel on alert rule**
```yaml
# In grafana alert (different from Prometheus alert):
# Dashboard > Panel Edit > Alert
# Set: Notification channel = the channel you created

# Or in prometheus.yml for Prometheus-native alerting:
global:
  resolve_timeout: 5m
  
route:
  receiver: default
  
receivers:
  - name: default
    webhook_configs:
      - url: 'http://localhost:9000/alerts'
```

**Step 7: Test alert by forcing condition**
```bash
# Manually make alert trigger:
# 1. Stop a broker:
docker-compose stop kafka-1

# 2. Wait for FOR duration (e.g., 1 minute):
sleep 65

# 3. Check Prometheus Alerts tab:
curl -s http://localhost:9090/api/v1/alerts | \
  jq '.data.alerts[] | {state: .state, name: .labels.alertname}'

# Should show: state="firing" for your alert

# 4. Restart broker:
docker-compose start kafka-1

# 5. Alert should change to "resolved"
```

**Step 8: Check Alertmanager if Prometheus-native alerts**
```bash
# Prometheus sends alerts to Alertmanager
# Alertmanager handles routing, grouping, notification

# Check if Alertmanager running:
docker-compose ps alertmanager

# Check Alertmanager config:
curl -s http://localhost:9093/api/v1/status | jq '.data.config'

# View current alerts in Alertmanager:
curl -s http://localhost:9093/api/v1/alerts | jq '.data[0] | {status: .status, labels: .labels}'
```

### Lessons Learned

✅ Test alert condition manually first  
✅ Check FOR duration - short for testing, longer for production  
✅ Verify metric labels match rule expectations  
✅ Notification channel must be configured separately  
✅ Prometheus alerts and Grafana alerts are different systems  
✅ Label filtering commonly causes no-match situations  
✅ Use Prometheus Alerts tab to debug rule execution  
✅ Test by deliberately triggering condition (stop a service)  

---

# Common Patterns Across All Issues

## Pattern 1: Right Tool for the Job

**Appeared in Issues**: 2.1, 2.2, 2.3

### Step-by-Step Recognition and Application

**Issue 2.1 Example: Java was overkill**

Step 1: Recognize the problem
```
- Initial choice: Use Confluent's AvroProducer (Java)
- Reality: Overkill for simple producer
- Symptom: Complex build process, dependency management nightmare
```

Step 2: Evaluate alternatives
```
- Python confluent-kafka: Simpler, lighter weight
- Python fastavro: Lower-level, more control
- Plain Python json: Too simple, no schema enforcement
```

Step 3: Make minimum viable choice
```
- Chose Python confluent-kafka
- Reason: Good balance of simplicity and schema support
```

Step 4: Execute transition
```bash
# Keep original Phase1_AvroProducer.java as reference
cp Phase1_AvroProducer.java Phase1_AvroProducer.java.backup

# Create new Python version
cat > producer.py << 'EOF'
from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'kafka:9092'})
producer.produce('topic', json.dumps(payload).encode())
producer.flush()
EOF
```

Step 5: Verify it works
```bash
python producer.py
# Success! Messages appear in topic
```

**Issue 2.2 Example: Containers solve environment problems**

Step 1: Recognize the constraint
```
- Problem: Need C++ compiler for confluent-kafka on Windows
- Reality: Not feasible in local dev environment
- Symptom: "ERROR: Microsoft Visual C++ 14.0 is required"
```

Step 2: Evaluate solutions
```
- Solution A: Install Visual Studio (1GB+, licensing questions)
- Solution B: Use WSL (extra complexity, still compilation issues)
- Solution C: Use Docker (problem solved once, replicable everywhere)
```

Step 3: Choose Docker
```
- Reason: Compile once, runs everywhere
- Setup: Single Dockerfile with all dependencies
- Team benefit: Developers don't need C++ compiler
```

**Issue 2.3 Example: Manual > Abstraction when abstraction is wrong**

Step 1: Recognize the mismatch
```
- Tool: confluent-kafka's AvroProducer
- Expectation: Automatic Avro serialization
- Reality: Overly strict validation, too many assumptions
```

Step 2: Find lower level
```
- Instead: Use confluent-kafka's base Producer
- Manual: Handle serialization with fastavro
```

Step 3: Implement simple version
```python
# Instead of:
producer = AvroProducer(...)  # Too complex

# Do this:
producer = Producer(...)
message_bytes = fastavro.schemaless_writer(BytesIO(...), schema, data).getvalue()
producer.produce('topic', message_bytes)
```

**Learning**: If a tool is causing more problems than it solves, go one level lower and handle it manually. Simpler tools are often better.

---

## Pattern 2: Dependencies Must Be Complete

**Appeared in Issues**: 2.2, 3.2, 3.3

### Step-by-Step Solution Template

**Issue 2.2: System Dependencies**

Step 1: Identify the error
```
ERROR: Microsoft Visual C++ 14.0 is required. Get it with "Microsoft C++ Build Tools"
```

Step 2: Understand what's needed
```
confluent-kafka package depends on:
  - C library librdkafka
  - C++ compiler to build bindings
```

Step 3: Solution strategy
```
Option A: Install compiler (not ideal, Windows-specific)
Option B: Pre-compile in container (better)
```

Step 4: Implement Docker solution
```dockerfile
FROM python:3.11-slim
# Build system already includes compilers
RUN apt-get update && apt-get install -y build-essential

COPY requirements.txt .
RUN pip install -r requirements.txt
# confluent-kafka compiles successfully in container
```

Step 5: Verify compilation
```bash
docker build ...
# Should compile without "Microsoft Visual C++" errors
```

**Issue 3.2: Python Package Dependencies**

Step 1: Identify error at runtime
```python
import fastavro
# ModuleNotFoundError: No module named 'fastavro'
```

Step 2: Check what's installed
```bash
pip list | grep -i avro
# (nothing)
```

Step 3: Understand what's needed
```
- Application code: from fastavro import schemaless_reader
- Dockerfile: pip install -r requirements.txt
- requirements.txt: (missing fastavro entry!)
```

Step 4: Add to requirements
```bash
# Inside container:
cat requirements.txt
# confluent-kafka==2.2.0
# (fastavro is not here!)

# Fix:
echo "fastavro>=1.8.0" >> requirements.txt
```

Step 5: Rebuild and verify
```bash
docker build -t processor:1.1 .
docker run ... processor:1.1
# Should work now
```

**Issue 3.3: Version Compatibility Dependencies**

Step 1: Identify the API issue
```python
schema_obj = sr_client.get_schema(1)
reader_schema = json.loads(schema_obj.schema.schema_str)
# AttributeError: 'Schema' object has no attribute 'schema'
```

Step 2: Understand version differences
```
Different confluent-kafka versions expose different APIs:
- v1.x: schema_obj.schema.schema_str (nested)
- v2.x: schema_obj.schema_str (direct)
```

Step 3: Check installed version
```bash
pip show confluent-kafka
# Version: 2.2.0
# ← Uses direct API!
```

Step 4: Handle version differences
```python
# Solution: Check what exists at runtime
if hasattr(schema_obj, 'schema_str'):
    schema_str = schema_obj.schema_str
elif hasattr(schema_obj, 'schema'):
    schema_str = schema_obj.schema.schema_str
else:
    schema_str = str(schema_obj)
```

Step 5: Document dependency
```bash
# requirements.txt
confluent-kafka>=2.2.0  # ← Specify version that supports direct API
fastavro>=1.8.0
```

**Learning**: 
1. Every import statement needs a requirements.txt entry
2. Verify each package exists: `docker exec container pip list | grep package`
3. Version differences matter - use `hasattr()` for compatibility
4. System dependencies (gcc, g++) need system packages in Dockerfile

---

## Pattern 3: Silent Failures Are Dangerous

**Appeared in Issues**: 3.1, 3.2, 3.3, 3.4

### Step-by-Step Detection and Prevention

**Issue 3.1: Silent None return**

Step 1: Recognize the symptom
```
- Processor running: ✓
- Messages in topic: ✓
- Messages being consumed: ✓
- Messages being processed: ✗ ("Processed 0 messages")
```

Step 2: Trace the producer
```python
# Deserialization code:
def deserialize(self, msg_bytes):
    try:
        result = fastavro.schemaless_reader(...)
        return result
    except:
        return None  # ← SILENT FAILURE!

# In main loop:
msg_data = self.deserialize(msg_bytes)
if msg_data:
    process(msg_data)
else:
    pass  # ← Nothing logged, silently skipped!
```

Step 3: Add comprehensive logging
```python
def deserialize(self, msg_bytes):
    logger.debug(f"Attempting to deserialize {len(msg_bytes)} bytes")
    try:
        result = fastavro.schemaless_reader(...)
        logger.debug(f"Deserialized successfully: {result}")
        return result
    except Exception as e:
        logger.error(f"Deserialization failed: {e}", exc_info=True)
        logger.debug(f"Message bytes: {msg_bytes[:50].hex()}")
        return None

# In main loop:
msg_data = self.deserialize(msg_bytes)
if msg_data:
    logger.info(f"Processing message: {msg_data}")
    process(msg_data)
else:
    logger.warning(f"Skipping message due to deserialization failure")
```

Step 4: Run and capture logs
```bash
docker run ... 2>&1 | tee processor.log
# Now see exactly where it failed
# [ERROR] Deserialization failed: 'Schema' object has no attribute 'schema'
# ← Root cause found!
```

**Issue 3.2: Missing module silently skipped**

Step 1: Recognize the symptom
```
- Processor runs without errors
- Outputs "Processed 0 messages"
- No ImportError or exception
```

Step 2: Discover the culprit
```python
# Inside processor:
try:
    import fastavro
    reader = fastavro.schemaless_reader(...)
except Exception as e:
    logger.error(f"Error: {e}")
    return handle_fallback()  # ← Falls back silently

# Later:
if reader:
    process(reader)
# else: nothing!
```

Step 3: Add import-time checks
```python
# At module level:
required_modules = ['confluent_kafka', 'fastavro']
for module_name in required_modules:
    try:
        __import__(module_name)
        logger.info(f"✓ Module {module_name} available")
    except ImportError:
        logger.error(f"✗ REQUIRED module {module_name} NOT installed")
        logger.error(f"  Add to requirements.txt: pip freeze | grep {module_name}")
        raise  # ← Fail fast on startup!
```

Step 4: Fail fast on startup
```bash
docker run ... processor:1.1
# [ERROR] ✗ REQUIRED module 'fastavro' NOT installed
# Error: ImportError: No module named 'fastavro'
# Container exits immediately with error
# ← Now error is obvious, not hidden!
```

**Issue 3.3: AttributeError caught and ignored**

Step 1: Recognize guarded exception handling
```python
def get_schema_str(schema_obj):
    try:
        return schema_obj.schema.schema_str
    except (AttributeError, TypeError):
        return None  # ← Silently returns None!

# Later:
schema_str = get_schema_str(schema_obj)
if not schema_str:
    logger.warning("Could not get schema")
    # But what went wrong? Unknown!
```

Step 2: Log the exception, don't hide it
```python
def get_schema_str(schema_obj):
    try:
        return schema_obj.schema.schema_str
    except (AttributeError, TypeError) as e:
        logger.error(f"Failed to access schema: {e}", exc_info=True)
        logger.debug(f"Schema object type: {type(schema_obj)}")
        logger.debug(f"Has 'schema'? {hasattr(schema_obj, 'schema')}")
        logger.debug(f"Has 'schema_str'? {hasattr(schema_obj, 'schema_str')}")
        return None

# Or raise and fail fast:
def get_schema_str(schema_obj):
    try:
        return schema_obj.schema.schema_str
    except (AttributeError, TypeError) as e:
        raise ValueError(
            f"Schema object {type(schema_obj)} doesn't have expected structure. "
            f"Available: {[attr for attr in dir(schema_obj) if not attr.startswith('_')]}"
        ) from e
```

Step 3: Run with detailed logging
```bash
docker run ... processor:1.2
# [ERROR] Failed to access schema: 'Schema' object has no attribute 'schema'
# [DEBUG] Schema object type: <class 'confluent_kafka.schema_registry.schema_registry_client.Schema'>
# [DEBUG] Has 'schema'? False
# [DEBUG] Has 'schema_str'? True
# ← Now you know exactly what's wrong AND what to do!
```

**Issue 3.4: Configuration silently ignored**

Step 1: Recognize when settings aren't taking effect
```python
consumer_config = {
    'auto.offset.reset': 'earliest',  # Set to earliest
    'group.id': 'streams-processor-group'
}
consumer = Consumer(consumer_config)
# But processor starts from latest, not earliest!
```

Step 2: Understand the real behavior
```
Kafka Consumer Offset Priority:
1. Committed offset (highest priority) ← Usually wins!
2. auto.offset.reset (fallback if no committed offset)

If group has EVER consumed from this topic:
- Offsets are COMMITTED to ZooKeeper/internal topic
- auto.offset.reset is IGNORED
- Consumer starts from COMMITTED offset, not earliest
```

Step 3: Add startup diagnostics
```python
consumer.subscribe(['package-events'])

# Immediately check assigned partitions
import time
time.sleep(0.5)  # Give partition assignment time
partitions = consumer.assignment()
logger.info(f"Assigned partitions: {partitions}")

# Check current committed offsets
for partition in partitions:
    committed = consumer.committed([partition])[0]
    if committed and committed.offset >= 0:
        logger.warning(f"Partition {partition.partition} has committed offset {committed.offset}")
        logger.warning(f"  auto.offset.reset='earliest' will be IGNORED")
        logger.warning(f"  Consumer will start from offset {committed.offset}")
    else:
        logger.info(f"Partition {partition.partition} has NO committed offset")
        logger.info(f"  Will use auto.offset.reset='earliest'")
```

Step 4: Run and see the diagnostics
```bash
docker run ... processor:1.2
# [WARNING] Partition 0 has committed offset 4
# [WARNING] auto.offset.reset='earliest' will be IGNORED
# [WARNING] Consumer will start from offset 4
# ← NOW you understand why it's not using 'earliest'!
```

Step 5: Fix with explicit reset
```bash
# Reset the group
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --reset-offsets \
  --to-earliest \
  --execute

# Verify reset worked
docker exec kafka-1 kafka-consumer-groups \
  --describe \
  --group streams-processor-group
# Now shows offset 0 for all partitions

# Run processor again
docker run ... processor:2.0
# [INFO] Partition 0 has NO committed offset
# [INFO] Will use auto.offset.reset='earliest'
# [INFO] [INPUT #1] ...  ← Now it actually processes from beginning!
```

**Learning**:
1. Never silently return None - log what happened
2. Never catch exceptions without logging them - log the error AND available options
3. Validate configuration took effect - log what was applied, not just what was requested
4. Fail fast on startup - don't hide problems until runtime

---

## Summary: Pattern Applications Across Issues

| Pattern | Issue 2.1 | Issue 2.2 | Issue 2.3 | Issue 3.1 | Issue 3.2 | Issue 3.3 | Issue 3.4 |
|---------|-----------|-----------|-----------|-----------|-----------|-----------|-----------|
| **Right Tool** | Java→Python | Windows→Docker | AvroProducer→Manual | N/A | N/A | N/A | N/A |
| **Dependencies** | - | C++ Compiler | - | - | fastavro | confluent-kafka version | - |
| **Silent Failures** | - | - | - | return None | missing module | exception caught | config ignored |

---

# Debugging Techniques Used

## Technique 1: Debug Version with Logging

Created separate debug processor (`stream_processor_debug.py`) with:
- DEBUG level logging
- Hex dumps of message bytes
- Schema attribute checking
- Type information

### Step-by-Step Implementation

**Step 1: Identify what needs debugging**
```python
# Original code with no visibility:
def deserialize_avro(self, msg_bytes):
    try:
        schema_obj = sr_client.get_schema(schema_id)
        reader_schema = json.loads(schema_obj.schema.schema_str)
        return fastavro.schemaless_reader(...)
    except:
        return None  # Silent failure!
```

**Step 2: Create debug version with comprehensive logging**
```python
def deserialize_avro(self, msg_bytes, schema_id=None):
    """Deserialize Avro message with extensive logging."""
    if not msg_bytes:
        return None, None
    
    logger.debug(f"Deserializing message: {len(msg_bytes)} bytes")
    logger.debug(f"First 20 bytes (hex): {msg_bytes[:20].hex()}")
    
    try:
        if msg_bytes[0] == 0:  # Avro magic byte
            import struct
            schema_id = struct.unpack('>I', msg_bytes[1:5])[0]
            logger.debug(f"Extracted schema_id: {schema_id}")
            
            sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
            logger.debug(f"Fetching schema {schema_id} from registry...")
            
            schema_obj = sr_client.get_schema(schema_id)
            
            # ← NEW: Inspect the object
            logger.debug(f"Schema object type: {type(schema_obj)}")
            logger.debug(f"Schema object class: {schema_obj.__class__.__name__}")
            logger.debug(f"Has 'schema_str'? {hasattr(schema_obj, 'schema_str')}")
            logger.debug(f"Has 'schema'? {hasattr(schema_obj, 'schema')}")
            
            # ← NEW: Multiple attempts with logging
            if hasattr(schema_obj, 'schema_str'):
                schema_str = schema_obj.schema_str
                logger.debug(f"Using schema_obj.schema_str")
            elif hasattr(schema_obj, 'schema'):
                schema_str = schema_obj.schema
                logger.debug(f"Using schema_obj.schema")
            else:
                schema_str = str(schema_obj)
                logger.debug(f"Using str(schema_obj)")
            
            reader_schema = json.loads(schema_str)
            logger.debug(f"Parsed schema: {reader_schema.get('name', 'unknown')}")
            
            result = fastavro.schemaless_reader(BytesIO(msg_bytes[5:]), reader_schema)
            logger.debug(f"Deserialization successful: {result}")
            return result, schema_id
        else:
            logger.debug("No Avro magic byte, trying JSON")
            data = json.loads(msg_bytes.decode('utf-8'))
            logger.debug(f"JSON deserialized: {data}")
            return data, None
    except Exception as e:
        logger.error(f"Deserialization failed: {e}", exc_info=True)
        logger.debug(f"Message bytes (hex): {msg_bytes[:50].hex()}")
        return None, None
```

**Step 3: Create separate Dockerfile for debug version**
```dockerfile
# Dockerfile.debug
FROM python:3.11-slim

WORKDIR /app
COPY streams-processor/requirements.txt .
COPY streams-processor/stream_processor_debug.py .
COPY schemas/ /app/schemas/

RUN pip install --no-cache-dir -r requirements.txt

# Run debug version instead
CMD ["python", "stream_processor_debug.py"]
```

**Step 4: Build and run debug version**
```bash
docker build -f Dockerfile.debug -t swifttrack-streams-debug:1.0 .

docker run --rm --network swifttrack-kafka-lab_default \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  swifttrack-streams-debug:1.0

# Output now shows detailed logs:
# [DEBUG] Deserializing message: 58 bytes
# [DEBUG] First 20 bytes (hex): 000000000118504b472d6163...
# [DEBUG] Extracted schema_id: 1
# [DEBUG] Fetching schema 1 from registry...
# [DEBUG] Schema object type: <class 'confluent_kafka.schema_registry.schema_registry_client.Schema'>
# [DEBUG] Has 'schema_str'? True
# [DEBUG] Using schema_obj.schema_str
# [ERROR] Deserialization failed: 'Schema' object has no attribute 'schema'
# ^ THIS immediately shows the problem!
```

**Result**: **Found the bug in minutes** instead of hours of blind guessing

---

## Technique 2: Incremental Docker Builds

Built multiple versions of Docker image, testing each independently:

### Step-by-Step Implementation

**Step 1: Version v1.0 - Initial attempt**
```bash
docker build -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.0 .

docker run ... swifttrack-streams-processor:1.0
# Result: Processed 0 messages
```

**Step 2: Version v1.1 - Add missing dependency**
```bash
# Identified missing fastavro in requirements
echo "fastavro>=1.8.0" >> requirements.txt

docker build -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.1 .

docker run ... swifttrack-streams-processor:1.1
# Result: Still 0 messages (different problem)
```

**Step 3: Version v1.2 - Increase timeout**
```bash
# Fixed timeout value
sed -i 's/if empty > 10:/if empty > 30:/' stream_processor.py

docker build -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:1.2 .

docker run ... swifttrack-streams-processor:1.2
# Result: Still 0 messages (still root issue not fixed)
```

**Step 4: Debug version - Add logging everywhere**
```bash
cp stream_processor.py stream_processor_debug.py
# Add extensive DEBUG logs

docker build -f Dockerfile.debug \
  -t swifttrack-streams-debug:1.0 .

docker run ... swifttrack-streams-debug:1.0
# Output shows: AttributeError: 'Schema' object has no attribute 'schema'
# ✓ ROOT CAUSE FOUND!
```

**Step 5: Version v2.0 - Fix schema access**
```bash
# Fix the schema attribute access
sed -i 's/schema_obj.schema.schema_str/schema_obj.schema_str/' stream_processor.py
# Add hasattr() checks for compatibility

docker build -f streams-processor/Dockerfile \
  -t swifttrack-streams-processor:2.0 .

docker run ... swifttrack-streams-processor:2.0
# Output:
# [INFO] [INPUT #1] PKG-4669442e | Package Picked Up | ...
# [INFO] [INPUT #2] PKG-4669442e | In Transit | ...
# ...
# [INFO] Processed 20 messages in 62.3s
# ✓ SUCCESS!
```

**Why Incremental Builds Helped**:
- Each version isolated one change
- Could compare behavior: v1.1 vs v1.2 showed fastavro wasn't the root issue
- Debug version had its own Dockerfile (replicable)
- Could rollback if a change made things worse

---

## Technique 3: Consumer Group Inspection

Used Kafka CLI to inspect consumer group extensively:

### Step-by-Step Implementation

**Step 1: Check if group exists**
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --list | grep streams-processor

# Output shows group exists or not
```

**Step 2: Describe group to see details**
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --describe

# Output example:
# GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
# streams-processor-group package-events 0 - - - - / -
# streams-processor-group package-events 1 - - - - / -
# streams-processor-group package-events 2 - - - - / -

# - = not set yet (good for fresh start)
# 0 = at offset 0 (will start from beginning)
# 4 = at offset 4 (will skip first 4 messages)
```

**Step 3: Check offsets after processor runs**
```bash
# Run processor for a moment
timeout 10 docker run ... swifttrack-streams-processor:2.0

# Check offsets after it runs
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --describe

# Output now shows:
# streams-processor-group package-events 0 4 4 0
# streams-processor-group package-events 1 12 12 0
# streams-processor-group package-events 2 4 4 0

# Interpretation:
# CURRENT-OFFSET 4,12,4 = processor consumed 4+12+4=20 messages total  ✓
# LAG 0 = caught up to all messages  ✓
```

**Step 4: Reset group for clean slate**
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --reset-offsets \
  --to-earliest \
  --execute \
  --topic package-events

# Verify reset:
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --group streams-processor-group \
  --describe

# Now shows offset 0 for all partitions again
# Ready for fresh start
```

**Result**: Could see exactly what the consumer was doing without modifying code

---

## Technique 4: Raw Message Inspection

Consumed messages without deserialization to see actual format:

### Step-by-Step Implementation

**Step 1: Consume raw messages**
```bash
docker exec -i kafka-1 bash -c \
  'timeout 5 kafka-console-consumer \
    --bootstrap-server kafka-1:9092 \
    --topic package-events \
    --from-beginning \
    --max-messages 1'

# Output shows raw Avro binary:
# PKG-d179f2c9    PKG-d179f2c9"Package Picked Up...
# (Not human readable - it's binary!)
```

**Step 2: Get hex dump of message**
```bash
docker exec -i kafka-1 bash -c \
  'timeout 5 kafka-run-class kafka.tools.JournalConsumer \
    --bootstrap-server kafka-1:9092 \
    --topic package-events \
    --property print.key=true \
    --max-messages 1 | hexdump -C' 2>/dev/null

# Output (hex dump):
# 00000000  00 00 00 00 01 18 50 4b  47 2d 64 31 37 39 66 32
# 00000010  63 39 22 50 61 63 6b 61  67 65 20 50 69 63 6b 65
# 00000020  64 20 55 70 ...

# Interpretation:
# 00 00 00 00 = magic bytes
# 01 = schema id (1)
# 18...99... = Avro binary data

# ✓ Confirms: Messages ARE in the topic, data IS Avro formatted
```

**Step 3: Compare expected vs actual**
```python
# Expected Avro format:
# Bytes 0: Magic byte (0x00)
# Bytes 1-4: Schema ID (big-endian int)
# Bytes 5+: Avro payload

# Actual from inspection:
# Bytes 0-3: 00 00 00 00 ✓ Magic + padding
# Bytes 4: 01 = Schema ID ✓
# Bytes 5+: Actual Avro data ✓

# ✓ Format matches expectations!
```

**Result**: Proved data existed and was correctly formatted before assuming deserialization was broken

---

## Technique 5: Type Introspection

Used Python's built-in introspection to understand unknown objects:

### Step-by-Step Implementation

**Step 1: Create inspection script**
```python
# inspect_schema_object.py
from confluent_kafka.schema_registry import SchemaRegistryClient

sr_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
schema_obj = sr_client.get_schema(1)

print("=" * 60)
print("TYPE INFORMATION")
print("=" * 60)
print(f"Type: {type(schema_obj)}")
print(f"Module: {schema_obj.__module__}")
print(f"Class name: {schema_obj.__class__.__name__}")

print("\n" + "=" * 60)
print("ALL ATTRIBUTES")
print("=" * 60)
for attr_name in dir(schema_obj):
    if not attr_name.startswith('_'):
        print(f"\n{attr_name}:")
        try:
            attr_value = getattr(schema_obj, attr_name)
            if callable(attr_value):
                print(f"  (method) {attr_value.__name__}")
            else:
                # Show first 100 chars if string
                if isinstance(attr_value, str):
                    value_str = attr_value[:100] + ("..." if len(attr_value) > 100 else "")
                else:
                    value_str = repr(attr_value)[:100]
                print(f"  {value_str}")
        except Exception as e:
            print(f"  (error accessing: {e})")

print("\n" + "=" * 60)
print("SPECIFIC CHECKS")
print("=" * 60)
print(f"Has 'schema_str'? {hasattr(schema_obj, 'schema_str')}")
print(f"Has 'schema'? {hasattr(schema_obj, 'schema')}")
print(f"Has 'schema_id'? {hasattr(schema_obj, 'schema_id')}")
print(f"Has 'subject'? {hasattr(schema_obj, 'subject')}")
```

**Step 2: Run inspection inside Docker**
```bash
docker run --rm --network swifttrack-kafka-lab_default \
  -v $(pwd):/workspace \
  python:3.11-slim \
  /bin/bash -c \
  'pip install confluent-kafka > /dev/null && python /workspace/inspect_schema_object.py'

# Output:
# ============================================================
# TYPE INFORMATION
# ============================================================
# Type: <class 'confluent_kafka.schema_registry.schema_registry_client.Schema'>
# Module: confluent_kafka.schema_registry.schema_registry_client
# Class name: Schema
#
# ============================================================
# ALL ATTRIBUTES
# ============================================================
#
# schema_id:
#   1
#
# schema_str:
#   {"type": "record", "name": "PackageEvent", "namespace": "com.swifttrack.schemas", "fields": [{"name": "package_id", "type": "string"}, ...
#
# subject:
#   package-events-value
#
# version:
#   1
#
# ============================================================
# SPECIFIC CHECKS
# ============================================================
# Has 'schema_str'? True  ← DIRECT ANSWER
# Has 'schema'? False     ← NOT NESTED
# Has 'schema_id'? True
# Has 'subject'? True
```

**Result**: Instantly showed the API structure without reading 50 pages of documentation

---

## Summary: Debugging Tools Used

| Technique | When to Use | Result |
|-----------|------------|--------|
| **Debug Version** | Silent failures | Found exact error location |
| **Incremental Builds** | Isolating which change broke things | Identified fastavro wasn't root cause |
| **Consumer Group CLI** | Message not being consumed | Confirmed 20 messages WERE consumed |
| **Raw Message Inspection** | Data format unknown | Confirmed Avro correctness |
| **Type Introspection** | API structure unknown | Revealed attribute names instantly |



---

# Summary Statistics

## Phase 2
| Issue | Severity | Time to Fix | Root Cause |
|-------|----------|-------------|-----------|
| Java Producer | Medium | 30 min | Wrong tool choice |
| Windows Build | High | 1 hour | Missing C++ build tools |
| AvroProducer Init | High | 45 min | API strictness |
| Schema Registration | Medium | 20 min | Missing prereq |
| Timestamp Generation | Low | 15 min | Inconsistency |

**Phase 2 Total Issues**: 5  
**Phase 2 Total Fix Time**: ~2.5 hours  

## Phase 3
| Issue | Severity | Time to Fix | Root Cause |
|-------|----------|-------------|-----------|
| Zero Message Processing | Critical | 2 hours | Multiple layers (deserial + logging) |
| Missing fastavro | Critical | 15 min | Requirement not documented |
| Schema Attribute Path | Critical | 1 hour | API misunderstanding |
| Consumer Offset | High | 45 min | Group state persistence |
| Timeout Too Short | Low | 10 min | Timing calculation |
| Partition Distribution | Low | 0 min | Not a problem |
| Registry Performance | Low | 0 min | Optimization not needed yet |

**Phase 3 Total Issues**: 7 (3 critical, 1 high, 3 low)  
**Phase 3 Total Fix Time**: ~4.5 hours  

---

# Key Takeaways

✅ **Always log everything in stream processing** - Silent failures are your enemy  
✅ **Use Docker from day 1** - Eliminates environment issues  
✅ **Inspect objects before using them** - Don't guess API structure  
✅ **Create debug versions** - Logging can isolate issues quickly  
✅ **Test with real data** - Not just happy paths  
✅ **Document assumptions** - "This should auto-register" vs "Must pre-register"  
✅ **Version handling is critical** - Use hasattr() checks for compatibility  
✅ **Consumer group state persists** - Reset when changing logic  

---

**Document Status**: Complete  
**Last Updated**: 2026-04-03  
**Ready for**: Phase 4 troubleshooting reference

