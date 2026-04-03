-- SwiftTrack PostgreSQL Schema Initialization
-- Phase 4: Kafka Connect Sink

-- Create database (if not using docker compose setup)
-- CREATE DATABASE swifttrack;

-- Connect to swifttrack database
\c swifttrack;

-- Table 1: package_events_delivered
-- Stores final delivery events from package-events-delivered topic
CREATE TABLE IF NOT EXISTS package_events_delivered (
    id SERIAL PRIMARY KEY,
    package_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    event_time_ms BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(package_id)
);

CREATE INDEX idx_package_events_delivered_package_id 
  ON package_events_delivered(package_id);
CREATE INDEX idx_package_events_delivered_status 
  ON package_events_delivered(status);

COMMENT ON TABLE package_events_delivered IS 
  'Final delivery status and location for each package (from package-events-delivered topic)';

-- Table 2: package_events_by_location
-- Tracks all events grouped by location
CREATE TABLE IF NOT EXISTS package_events_by_location (
    id SERIAL PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    package_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    event_time_ms BIGINT,
    count_at_location INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location, package_id)
);

CREATE INDEX idx_package_events_by_location_location 
  ON package_events_by_location(location);
CREATE INDEX idx_package_events_by_location_package_id 
  ON package_events_by_location(package_id);

COMMENT ON TABLE package_events_by_location IS 
  'Package events aggregated by location (from package-events-by-location topic)';

-- Table 3: package_count_by_status
-- Aggregate counts of packages by status
CREATE TABLE IF NOT EXISTS package_count_by_status (
    id SERIAL PRIMARY KEY,
    status VARCHAR(50) NOT NULL UNIQUE,
    count INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_package_count_by_status_status 
  ON package_count_by_status(status);

COMMENT ON TABLE package_count_by_status IS 
  'Aggregate count of packages by status (from package-count-by-status topic)';

-- Table 4: package_delivery_hops
-- Tracks number of hops for each package's delivery
CREATE TABLE IF NOT EXISTS package_delivery_hops (
    id SERIAL PRIMARY KEY,
    package_id VARCHAR(50) NOT NULL UNIQUE,
    hop_count INT DEFAULT 0,
    last_location VARCHAR(100),
    event_time_ms BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_package_delivery_hops_package_id 
  ON package_delivery_hops(package_id);
CREATE INDEX idx_package_delivery_hops_hop_count 
  ON package_delivery_hops(hop_count);

COMMENT ON TABLE package_delivery_hops IS 
  'Tracks number of hops/events for each package (from package-delivery-hops topic)';

-- Kafka Connect Offset Tracking Table (created automatically by Connector)
-- This is for reference - Kafka Connect creates __consumer_offsets topic internally
CREATE TABLE IF NOT EXISTS kafka_connect_offsets (
    id SERIAL PRIMARY KEY,
    connector_name VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INT,
    offset BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE kafka_connect_offsets IS 
  'Manual tracking of Kafka Connect offset progress (for monitoring purposes)';

-- Grant permissions to swifttrack user
ALTER TABLE package_events_delivered OWNER TO swifttrack;
ALTER TABLE package_events_by_location OWNER TO swifttrack;
ALTER TABLE package_count_by_status OWNER TO swifttrack;
ALTER TABLE package_delivery_hops OWNER TO swifttrack;
ALTER TABLE kafka_connect_offsets OWNER TO swifttrack;

-- Summary of tables
SELECT 
    schemaname,
    tablename,
    CASE 
        WHEN tablename = 'package_events_delivered' THEN 'Phase 4: Final Delivery Status'
        WHEN tablename = 'package_events_by_location' THEN 'Phase 4: Events by Location'
        WHEN tablename = 'package_count_by_status' THEN 'Phase 4: Status Count Aggregates'
        WHEN tablename = 'package_delivery_hops' THEN 'Phase 4: Delivery Hop Tracking'
        WHEN tablename = 'kafka_connect_offsets' THEN 'Phase 4: Connector Offset Tracking'
        ELSE 'System'
    END as purpose
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY tablename;
