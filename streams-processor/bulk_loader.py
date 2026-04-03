#!/usr/bin/env python3
"""
Bulk load processed_products messages from Kafka to PostgreSQL.
"""

import json
import logging
import os
import time
import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka-1:9092')
TOPIC = 'processed_products'

# PostgreSQL connection
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = int(os.getenv('PG_PORT', '5432'))
PG_DATABASE = os.getenv('PG_DATABASE', 'swifttrack')
PG_USER = os.getenv('PG_USER', 'swifttrack')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'swifttrack')

def bulk_load():
    """Bulk load messages from Kafka to PostgreSQL."""
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
        cursor = conn.cursor()
        logger.info(f"Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        return
    
    # Setup Kafka consumer with unique group
    unique_group = f'bulk_loader_{int(time.time())}'
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': unique_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([TOPIC])
    
    logger.info(f"Consuming from '{TOPIC}' with group '{unique_group}'")
    
    messages = []
    count = 0
    
    # Collect all messages
    empty_polls = 0
    while empty_polls < 20:  # 40 seconds of no messages
        msg = consumer.poll(timeout=2.0)
        
        if msg is None:
            empty_polls += 1
            continue
        
        empty_polls = 0
        
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
            messages.append((
                data.get('package_id'),
                data.get('status'),
                data.get('location'),
                data.get('timestamp')
            ))
            count += 1
            if count % 10 == 0:
                logger.info(f"Collected {count} messages...")
        except Exception as e:
            logger.error(f"Parse error: {e}")
    
    logger.info(f"Collected {count} total messages from Kafka")
    
    # Bulk insert into PostgreSQL (with upsert for duplicates)
    try:
        # Use INSERT ... ON CONFLICT to update if package_id already exists
        insert_sql = """
        INSERT INTO processed_products (package_id, status, location, timestamp) 
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (package_id) DO UPDATE SET
            status = EXCLUDED.status,
            location = EXCLUDED.location,
            timestamp = EXCLUDED.timestamp
        """
        cursor.executemany(insert_sql, messages)
        conn.commit()
        logger.info(f"Successfully upserted {cursor.rowcount} rows into PostgreSQL")
    except Exception as e:
        logger.error(f"Insert failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == '__main__':
    bulk_load()
