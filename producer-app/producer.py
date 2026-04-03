#!/usr/bin/env python3
"""
PackageProducer - Python Kafka producer with Avro serialization.

Features:
- Avro serialization via Schema Registry
- Idempotent producer (no duplicates)
- Message keys (package_id) for ordering
- Simulates package scan events
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Dict, Any

import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.schema_registry_client import Schema

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration - use environment variables with defaults for Docker network
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka-1:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
TOPIC = 'package-events'

# Package statuses
STATUSES = [
    'Package Picked Up',
    'In Transit',
    'Out for Delivery',
    'Delivered'
]

# Sample locations
LOCATIONS = [
    'New York Distribution Center',
    'Los Angeles Hub',
    'Chicago Sorting Facility',
    'Customer Location'
]


class PackageProducer:
    """Kafka producer for package scan events."""

    def __init__(self):
        """Initialize the producer with Avro schema from file."""
        self.schema_registry = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.value_schema = self._get_or_register_schema()
        self.producer = self._create_producer()
        logger.info("PackageProducer initialized successfully")

    def _get_or_register_schema(self) -> str:
        """Get or register the package event schema."""
        schema_str = json.dumps({
            "type": "record",
            "name": "PackageEvent",
            "namespace": "com.swifttrack.schemas",
            "fields": [
                {"name": "package_id", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "location", "type": "string"}
            ]
        })
        
        try:
            # Try to get existing subject
            schema = self.schema_registry.get_latest_version(f'{TOPIC}-value')
            logger.info(f"Using existing schema version {schema.version}")
            return schema.schema.schema_str
        except Exception as e:
            # Register new schema
            logger.info(f"Registering new schema with Schema Registry: {e}")
            schema = Schema(schema_str, schema_type='AVRO')
            self.schema_registry.register_schema(f'{TOPIC}-value', schema)
            return schema_str

    def _create_producer(self) -> AvroProducer:
        """Create Kafka producer with idempotence enabled."""
        config = {
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'schema.registry.url': SCHEMA_REGISTRY_URL,
            'enable.idempotence': True,  # Exactly-once semantics
            'acks': 'all',  # Wait for all replicas
            'retries': 10,
            'compression.type': 'snappy',
            'linger.ms': 10,
            'batch.size': 16384,
        }
        
        logger.info("Creating Kafka producer with idempotence enabled, acks=all, compression=snappy")
        # Use string schema for key, Avro schema for value
        key_schema_str = json.dumps({"type": "string"})
        key_schema = avro.schema.parse(key_schema_str)
        value_schema = avro.schema.parse(self.value_schema)
        
        producer = AvroProducer(
            config, 
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )
        return producer

    def send_package_event(self, package_id: str, status: str, location: str):
        """Send a package event to Kafka."""
        timestamp = int(time.time() * 1000)
        
        # Create message value
        value = {
            'package_id': package_id,
            'status': status,
            'timestamp': timestamp,
            'location': location
        }
        
        # Send with callback
        self.producer.produce(
            topic=TOPIC,
            key=package_id,
            value=value,
            on_delivery=self._delivery_report
        )

    @staticmethod
    def _delivery_report(err, msg):
        """Delivery callback."""
        if err is not None:
            logger.error(f"Failed to send message: {err}")
        else:
            logger.info(
                f"Message sent: PackageID={msg.key().decode('utf-8')}, "
                f"Status=*, Partition={msg.partition()}, Offset={msg.offset()}"
            )

    def simulate_package_scans(self, num_packages: int = 5, events_per_package: int = 4):
        """Simulate package scan events."""
        logger.info(f"Simulating {num_packages} packages with {events_per_package} events each")
        
        import random
        
        for i in range(num_packages):
            package_id = f"PKG-{str(uuid.uuid4())[:8]}"
            
            for j in range(events_per_package):
                status = STATUSES[j % len(STATUSES)]
                location = random.choice(LOCATIONS)
                
                self.send_package_event(package_id, status, location)
                time.sleep(0.1)  # Small delay between events
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        logger.info("All messages sent successfully. Flushed producer.")

    def close(self):
        """Close the producer."""
        if self.producer:
            self.producer.flush()
            logger.info("Producer closed successfully")


def main():
    """Main entry point."""
    logger.info("Starting PackageProducer...")
    
    producer = PackageProducer()
    
    try:
        # Simulate package scans
        producer.simulate_package_scans(num_packages=5, events_per_package=4)
        logger.info("PackageProducer completed successfully")
    finally:
        producer.close()


if __name__ == '__main__':
    main()
