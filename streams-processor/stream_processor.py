#!/usr/bin/env python3
"""
Phase 3: Kafka Streams Processor
Simplified stream processing for package events.
"""

import json
import logging
import os
import time
from collections import defaultdict
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.schema_registry_client import Schema

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka-1:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
INPUT_TOPIC = 'raw-products'
CONSUMER_GROUP = 'streams-processor-group'

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

    def deserialize_avro(self, msg_bytes):
        """Simple Avro deserialization."""
        if not msg_bytes:
            return None
        try:
            # Check for Avro format (magic byte 0)
            if msg_bytes[0] == 0:
                import struct
                import fastavro
                from io import BytesIO
                from confluent_kafka.schema_registry import SchemaRegistryClient
                
                sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
                schema_id = struct.unpack('>I', msg_bytes[1:5])[0]
                schema_obj = sr_client.get_schema(schema_id)
                
                # Get the schema string - try different attributes
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

    def process(self):
        """Process stream."""
        count = 0
        empty = 0
        start = time.time()

        logger.info("Processing stream...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=2.0)
                
                if msg is None:
                    empty += 1
                    logger.debug(f"No message received ({empty}/30 empty polls)")
                    if empty > 30:  # Wait 60 seconds instead of 20
                        logger.info(f"Processor timeout after {time.time()-start:.1f}s")
                        break
                    continue

                empty = 0
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    data = self.deserialize_avro(msg.value())
                    if not data:
                        logger.debug(f"Failed to deserialize message: {msg.value()[:50]}")
                        continue
                    
                    pkg_id = data.get('package_id', '')
                    status = data.get('status', '')
                    ts = data.get('timestamp', 0)
                    location = data.get('location', '')
                    count += 1

                    logger.info(f"[INPUT #{count}] {pkg_id} | {status} | {location}")

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
                        logger.info(f"  -> [FILTER delivered]")

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
                    logger.info(f"  -> [MAP location]")

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

                    # Window: Delivery hops
                    if pkg_id not in self.package_states:
                        self.package_states[pkg_id] = {
                            'start': ts, 'last': ts, 'hops': 0, 'statuses': []
                        }
                    
                    state = self.package_states[pkg_id]
                    state['last'] = ts
                    state['statuses'].append(status)
                    state['hops'] += 1
                    
                    if status == 'Delivered':
                        self.producer.produce(
                            OUTPUT_DELIVERY_HOPS,
                            key=pkg_id,
                            value=json.dumps({
                                'package_id': pkg_id,
                                'hop_count': state['hops'] - 1,
                                'status_history': state['statuses'],
                                'duration_ms': state['last'] - state['start']
                            }).encode()
                        )
                        logger.info(f"  -> [WINDOW delivered]")

                    self.consumer.commit(asynchronous=False)

                except Exception as e:
                    logger.debug(f"Error: {e}")
                    continue

        except KeyboardInterrupt:
            pass
        finally:
            elapsed = time.time() - start
            logger.info(f"Processed {count} messages in {elapsed:.1f}s")
            logger.info(f"Status counts: {dict(self.status_counts)}")
            self.producer.flush()
            self.consumer.close()
            logger.info("Done")


def main():
    """Main."""
    logger.info("Starting Phase 3: Kafka Streams Processor...")
    processor = StreamProcessor()
    processor.process()


if __name__ == '__main__':
    main()
