#!/usr/bin/env python3
"""
Simple utility to read from processed_products Kafka topic and insert into PostgreSQL.
"""

import json
import logging
import os
import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka-1:9092')
TOPIC = 'processed_products'
CONSUMER_GROUP = 'kafka_to_postgres_group'

# PostgreSQL connection
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = int(os.getenv('PG_PORT', '5432'))
PG_DATABASE = os.getenv('PG_DATABASE', 'swifttrack')
PG_USER = os.getenv('PG_USER', 'swifttrack')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'swifttrack')

def main():
    """Read from Kafka and insert into PostgreSQL."""
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        logger.info(f"Connected to PostgreSQL at {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return
    
    # Setup Kafka consumer
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    })
    consumer.subscribe([TOPIC])
    
    logger.info(f"Listening to topic '{TOPIC}' from broker {BOOTSTRAP_SERVERS}")
    
    count = 0
    empty = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            
            if msg is None:
                empty += 1
                if empty > 30:  # Stop after 60 seconds of no messages
                    logger.info("No messages received for 60 seconds, stopping")
                    break
                continue
            
            empty = 0
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                # Parse JSON message
                data = json.loads(msg.value().decode('utf-8'))
                
                package_id = data.get('package_id')
                status = data.get('status')
                location = data.get('location')
                timestamp = data.get('timestamp')
                
                # Insert into PostgreSQL
                cursor.execute(
                    "INSERT INTO processed_products (package_id, status, location, timestamp) VALUES (%s, %s, %s, %s)",
                    (package_id, status, location, timestamp)
                )
                conn.commit()
                
                count += 1
                logger.info(f"[{count}] Inserted: {package_id} | {status} | {location}")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    
    finally:
        logger.info(f"Processed {count} messages total")
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == '__main__':
    main()
