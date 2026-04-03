#!/usr/bin/env python3
"""
Phase 3: Kafka Streams Processor - Debug Version
Simple message consumption with extensive logging.
"""

import json
import logging
import os
import time
from collections import defaultdict

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.schema_registry_client import Schema

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka-1:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
INPUT_TOPIC = 'package-events'
CONSUMER_GROUP = 'streams-processor-debug-group'

OUTPUT_DELIVERED = 'package-events-delivered'
OUTPUT_BY_LOCATION = 'package-events-by-location'
OUTPUT_STATUS_COUNT = 'package-count-by-status'
OUTPUT_DELIVERY_HOPS = 'package-delivery-hops'


def register_schemas():
    """Register output schemas."""
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    schemas = {
        'package-events-delivered': {
            "type": "record", "name": "DeliveredEvent", "namespace": "com.swifttrack.schemas",
            "fields": [
                {"name": "package_id", "type": "string"},
                {"name": "delivered_timestamp", "type": "long"},
                {"name": "delivery_location", "type": "string"}
            ]
        },
        'package-events-by-location': {
            "type": "record", "name": "LocationEvent", "namespace": "com.swifttrack.schemas",
            "fields": [
                {"name": "package_id", "type": "string"},
                {"name": "location", "type": "string"},
                {"name": "event_timestamp", "type": "long"}
            ]
        },
        'package-count-by-status': {
            "type": "record", "name": "StatusCount", "namespace": "com.swifttrack.schemas",
            "fields": [
                {"name": "status", "type": "string"},
                {"name": "count", "type": "int"},
                {"name": "window_timestamp", "type": "long"}
            ]
        },
        'package-delivery-hops': {
            "type": "record", "name": "DeliveryHops", "namespace": "com.swifttrack.schemas",
            "fields": [
                {"name": "package_id", "type": "string"},
                {"name": "hop_count", "type": "int"},
                {"name": "status_history", "type": {"type": "array", "items": "string"}},
                {"name": "duration_ms", "type": "long"}
            ]
        }
    }
    
    for topic, schema_def in schemas.items():
        subject = f'{topic}-value'
        try:
            schema = sr_client.get_latest_version(subject)
            logger.info(f"Using existing schema for {topic}")
        except:
            logger.info(f"Registering new schema for {topic}")
            schema = Schema(json.dumps(schema_def), schema_type='AVRO')
            sr_client.register_schema(subject, schema)


def deserialize_avro(msg_bytes, schema_id=None):
    """Deserialize Avro message."""
    if not msg_bytes:
        return None, None
    
    logger.debug(f"Deserializing message: {len(msg_bytes)} bytes, first 20: {msg_bytes[:20]}")
    
    try:
        # Check for Avro format (magic byte 0)
        if msg_bytes[0] == 0:
            import struct
            import fastavro
            from io import BytesIO
            
            schema_id = struct.unpack('>I', msg_bytes[1:5])[0]
            logger.debug(f"Avro magic byte detected, schema_id={schema_id}")
            
            sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
            schema_obj = sr_client.get_schema(schema_id)
            
            # Get the schema string - it might be different attributes
            if hasattr(schema_obj, 'schema_str'):
                reader_schema = json.loads(schema_obj.schema_str)
            elif hasattr(schema_obj, 'schema'):
                reader_schema = json.loads(schema_obj.schema)
            else:
                reader_schema = json.loads(str(schema_obj))
            
            logger.debug(f"Schema: {reader_schema}")
            
            data = fastavro.schemaless_reader(BytesIO(msg_bytes[5:]), reader_schema)
            logger.debug(f"Deserialized: {data}")
            return data, schema_id
        else:
            logger.debug("Attempting JSON deserialization")
            data = json.loads(msg_bytes.decode('utf-8'))
            logger.debug(f"JSON Deserialized: {data}")
            return data, None
    except Exception as e:
        logger.error(f"Deserialization failed: {e}", exc_info=True)
        logger.debug(f"Message hex: {msg_bytes[:50].hex()}")
        return None, None


class StreamProcessor:
    """Stream processor for package events."""

    def __init__(self):
        """Initialize."""
        register_schemas()
        
        def on_assign(consumer, partitions):
            logger.info(f"Partitions assigned: {[(p.topic, p.partition) for p in partitions]}")
        
        def on_revoke(consumer, partitions):
            logger.info(f"Partitions revoked: {[(p.topic, p.partition) for p in partitions]}")
        
        self.consumer = Consumer({
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'api.version.request.timeout.ms': 10000,
        })
        self.consumer.subscribe([INPUT_TOPIC], on_assign=on_assign, on_revoke=on_revoke)
        
        self.producer = Producer({
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'enable.idempotence': True,
            'acks': 'all',
            'compression.type': 'snappy',
        })
        
        self.status_counts = defaultdict(int)
        self.package_states = {}
        logger.info("StreamProcessor initialized")

    def process(self):
        """Process stream."""
        count = 0
        empty = 0
        start = time.time()

        logger.info("Processing stream...")
        
        try:
            while True:
                logger.debug(f"Polling... (empty={empty})")
                msg = self.consumer.poll(timeout=2.0)
                
                if msg is None:
                    empty += 1
                    logger.debug(f"No message ({empty}/30)")
                    if empty > 30:
                        logger.info(f"Timeout after {time.time()-start:.1f}s")
                        break
                    continue

                empty = 0
                logger.debug(f"Got message from {msg.topic()}:{msg.partition()}@{msg.offset()}")
                
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    data, schema_id = deserialize_avro(msg.value())
                    if not data:
                        logger.warning(f"Failed to deserialize message at offset {msg.offset()}")
                        continue
                    
                    pkg_id = data.get('package_id', '')
                    status = data.get('status', '')
                    ts = data.get('timestamp', 0)
                    location = data.get('location', '')
                    count += 1

                    logger.info(f"[INPUT #{count}] pkg={pkg_id} status={status} loc={location}")

                    # Filter: Delivered
                    if status == 'Delivered':
                        self.producer.produce(
                            OUTPUT_DELIVERED,
                            key=pkg_id,
                            value=json.dumps({
                                'package_id': pkg_id,
                                'delivered_timestamp': ts,
                                'delivery_location': location
                            }).encode()
                        )
                        logger.debug(f"  -> [FILTER delivered]")

                    # Map: By location
                    self.producer.produce(
                        OUTPUT_BY_LOCATION,
                        key=pkg_id,
                        value=json.dumps({
                            'package_id': pkg_id,
                            'location': location,
                            'event_timestamp': ts
                        }).encode()
                    )
                    logger.debug(f"  -> [MAP location]")

                    # Aggregate: Count by status
                    self.status_counts[status] += 1
                    self.producer.produce(
                        OUTPUT_STATUS_COUNT,
                        key=status,
                        value=json.dumps({
                            'status': status,
                            'count': self.status_counts[status],
                            'window_timestamp': int(time.time() * 1000)
                        }).encode()
                    )
                    logger.debug(f"  -> [AGG status]")

                    # Window: Delivery hops  
                    if status == 'Delivered':
                        if pkg_id not in self.package_states:
                            self.package_states[pkg_id] = {'start': time.time(), 'hops': []}
                        self.package_states[pkg_id]['hops'].append(status)
                        
                        duration_ms = int((time.time() - self.package_states[pkg_id]['start']) * 1000)
                        self.producer.produce(
                            OUTPUT_DELIVERY_HOPS,
                            key=pkg_id,
                            value=json.dumps({
                                'package_id': pkg_id,
                                'hop_count': len(self.package_states[pkg_id]['hops']),
                                'status_history': self.package_states[pkg_id]['hops'],
                                'duration_ms': duration_ms
                            }).encode()
                        )
                        logger.debug(f"  -> [WINDOW hops]")

                    self.consumer.commit()

                except Exception as e:
                    logger.error(f"Processing error: {e}", exc_info=True)
                    continue

        finally:
            elapsed = time.time() - start
            self.producer.flush()
            self.consumer.close()
            
            logger.info(f"Processed {count} messages in {elapsed:.1f}s")
            logger.info(f"Status counts: {dict(self.status_counts)}")
            logger.info("Done")


if __name__ == '__main__':
    logger.info("Starting Phase 3: Kafka Streams Processor (Debug)...")
    processor = StreamProcessor()
    processor.process()
