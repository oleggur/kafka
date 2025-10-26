-- ClickHouse Initialization Script
-- This script runs automatically when ClickHouse container is first created
-- It creates the demo database and all necessary tables for the Kafka tutorials

-- Create demo database
CREATE DATABASE IF NOT EXISTS demo;

-- Use demo database
USE demo;

-- ============================================================================
-- Tutorial 01: Basics
-- ============================================================================

-- Table for basic producer/consumer tutorial
CREATE TABLE IF NOT EXISTS ads_basic (
    inserted_at DateTime DEFAULT now(),   -- Timestamp when row was inserted
    seq UInt64,                            -- Sequence number from message
    campaign_id String,                    -- Campaign identifier
    spend Float64,                         -- Ad spend amount
    currency String                        -- Currency code
) ENGINE = MergeTree()
ORDER BY (campaign_id, seq)                -- Primary key for efficient queries
COMMENT 'Tutorial 01: Basic Kafka messages';

-- ============================================================================
-- Tutorial 02: Serialization
-- ============================================================================

-- Table for JSON serialization (nullable fields to handle missing data)
CREATE TABLE IF NOT EXISTS ads_json (
    inserted_at DateTime DEFAULT now(),
    seq UInt64,
    campaign_id String,
    spend Nullable(Float64),               -- Nullable to handle missing values
    currency String,
    timestamp Nullable(String),            -- ISO timestamp as string
    metadata_source Nullable(String),      -- Flattened nested field
    metadata_version Nullable(String),     -- Flattened nested field
    tags Array(String) DEFAULT []          -- Array of tags
) ENGINE = MergeTree()
ORDER BY (campaign_id, seq)
COMMENT 'Tutorial 02-A: JSON serialization with flexible schema';

-- Table for Avro serialization (strict types, no nulls)
CREATE TABLE IF NOT EXISTS ads_avro (
    inserted_at DateTime DEFAULT now(),
    seq UInt64,
    campaign_id String,
    spend Float64,                         -- Not nullable (Avro ensures value exists)
    currency String,
    timestamp String,                      -- ISO timestamp
    metadata_source String,                -- Nested metadata fields (flattened)
    metadata_version String,
    tags Array(String)                     -- Array of tags
) ENGINE = MergeTree()
ORDER BY (campaign_id, seq)
COMMENT 'Tutorial 02-B: Avro serialization with schema enforcement';

-- ============================================================================
-- Tutorial 03: Partitioning
-- ============================================================================

-- Table for partitioning tutorial (tracks which partition messages came from)
CREATE TABLE IF NOT EXISTS ads_partitioned (
    inserted_at DateTime DEFAULT now(),
    seq UInt64,
    campaign_id String,
    spend Float64,
    currency String,
    strategy String,                       -- Partitioning strategy used
    partition_id UInt32,                   -- Kafka partition number
    consumer_instance String               -- Which consumer instance processed this
) ENGINE = MergeTree()
ORDER BY (partition_id, seq)
COMMENT 'Tutorial 03: Partitioning and parallel consumption';

-- ============================================================================
-- Tutorial 04: Reliability
-- ============================================================================

-- Table for reliability tutorial (manual commits, idempotence)
CREATE TABLE IF NOT EXISTS ads_reliability (
    inserted_at DateTime DEFAULT now(),
    seq UInt64,
    campaign_id String,
    spend Float64,
    currency String,
    idempotent Boolean,                    -- Whether producer was idempotent
    processing_time_ms UInt32              -- Time taken to process message
) ENGINE = MergeTree()
ORDER BY (campaign_id, seq)
COMMENT 'Tutorial 04: Reliability with idempotence and manual commits';

-- ============================================================================
-- Tutorial 05: Transactions
-- ============================================================================

-- Table for transactional messages
CREATE TABLE IF NOT EXISTS ads_transactions (
    inserted_at DateTime DEFAULT now(),
    seq UInt64,
    campaign_id String,
    spend Float64,
    currency String,
    transaction String                     -- Transaction ID from producer
) ENGINE = MergeTree()
ORDER BY (transaction, seq)
COMMENT 'Tutorial 05: Transactional producer with exactly-once semantics';

-- Table for transaction summaries (multi-topic writes)
CREATE TABLE IF NOT EXISTS ads_transactions_summary (
    inserted_at DateTime DEFAULT now(),
    batch_id String,                       -- Batch identifier
    message_count UInt32,                  -- Number of messages in batch
    total_spend Float64,                   -- Total spend across batch
    currency String,
    transaction String                     -- Transaction ID
) ENGINE = MergeTree()
ORDER BY batch_id
COMMENT 'Tutorial 05: Transaction summaries for multi-topic atomic writes';

-- ============================================================================
-- Materialized Views (Optional - for advanced analytics)
-- ============================================================================

-- Aggregated view: Total spend by campaign (updates automatically)
CREATE MATERIALIZED VIEW IF NOT EXISTS ads_basic_by_campaign
ENGINE = SummingMergeTree()
ORDER BY campaign_id
AS SELECT
    campaign_id,
    SUM(spend) as total_spend,
    COUNT(*) as message_count
FROM ads_basic
GROUP BY campaign_id;

-- Aggregated view: Hourly spend
CREATE MATERIALIZED VIEW IF NOT EXISTS ads_basic_hourly
ENGINE = SummingMergeTree()
ORDER BY (hour, campaign_id)
AS SELECT
    toStartOfHour(inserted_at) as hour,
    campaign_id,
    SUM(spend) as total_spend,
    COUNT(*) as message_count
FROM ads_basic
GROUP BY hour, campaign_id;

-- ============================================================================
-- Indexes (Optional - for faster queries)
-- ============================================================================

-- No additional indexes needed for small tutorial datasets
-- For production, consider:
-- - Bloom filter indexes for String columns
-- - MinMax indexes for numeric ranges
-- - Set indexes for high-cardinality columns

-- ============================================================================
-- Verification
-- ============================================================================

-- Show all created tables
SHOW TABLES FROM demo;

-- Show table sizes (initially all should be empty)
SELECT
    table,
    formatReadableSize(total_bytes) as size,
    total_rows as rows
FROM system.tables
WHERE database = 'demo'
ORDER BY table;
