# Kafka Tutorial: From Basics to Production

A comprehensive, hands-on tutorial for learning Apache Kafka with ClickHouse integration. This tutorial is designed for team training, with heavily commented code and progressive complexity.

## Table of Contents

- [Setup](#setup)
- [Tutorial Structure](#tutorial-structure)
- [Tutorial 01: Basics](#tutorial-01-basics)
- [Tutorial 02: Serialization](#tutorial-02-serialization)
- [Tutorial 03: Partitioning](#tutorial-03-partitioning)
- [Tutorial 04: Reliability](#tutorial-04-reliability)
- [Tutorial 05: Transactions](#tutorial-05-transactions)
- [ClickHouse Queries](#clickhouse-queries)
- [Troubleshooting](#troubleshooting)

---

## Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Git

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd kafka
```

### 2. Start Infrastructure

Create `docker-compose.yml` (already in repo):

```bash
docker compose up -d
```

This starts:
- **Kafka** (port 9092) - Message broker
- **Schema Registry** (port 8081) - Avro schema management
- **ClickHouse** (ports 8123, 9000) - Analytics database

Verify services are running:

```bash
docker compose ps
```

### 3. Python Environment Setup

Create and activate virtual environment:

```bash
# Create venv
python3 -m venv .venv

# Activate (Linux/Mac)
source .venv/bin/activate

# Activate (Windows Git Bash)
source .venv/Scripts/activate
```

Install dependencies:

```bash
pip install --upgrade pip
pip install confluent-kafka==2.5.3
pip install clickhouse-connect==0.7.0
pip install fastavro==1.9.7
```

### 4. Verify Setup

Check Kafka is accessible:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Check ClickHouse is accessible:

```bash
docker exec -it clickhouse clickhouse-client --password secret
```

Or connect via DBeaver:
- Host: `localhost:8123`
- Username: `default`
- Password: `secret`

---

## Tutorial Structure

Each tutorial builds on previous concepts:

```
tutorials/
├── 01-basics/              # Simple producer and consumer
├── 02-serialization/       # JSON vs Avro
├── 03-partitioning/        # Parallelism and ordering
├── 04-reliability/         # At-least-once, idempotence
└── 05-transactions/        # Exactly-once semantics
```

All tutorials integrate with **ClickHouse** so you can query and analyze the data produced by Kafka.

---

## Tutorial 01: Basics

**Goal**: Understand core Kafka concepts - producers, consumers, topics, offsets.

### Concepts Covered

- What is Kafka and why use it
- Producers and consumers
- Topics and partitions (basics)
- Offsets and offset management
- Consumer groups
- ClickHouse integration

### Scripts

#### 1.1 Basic Producer

**File**: [tutorials/01-basics/producer.py](tutorials/01-basics/producer.py)

**What it does**:
- Connects to Kafka broker
- Produces 10 simple ad spend events
- Demonstrates delivery callbacks
- Shows producer flush

**Run**:

```bash
python tutorials/01-basics/producer.py
```

**Expected output**:
```
[START] Producing messages to topic: 'ads_basic'
[SENT] Message #0: {"seq": 0, "campaign_id": "campaign_0", ...}
[DELIVERED] topic=ads_basic partition=0 offset=0
...
[SUCCESS] All 10 messages delivered successfully!
```

#### 1.2 Basic Consumer with ClickHouse

**File**: [tutorials/01-basics/consumer_clickhouse.py](tutorials/01-basics/consumer_clickhouse.py)

**What it does**:
- Connects to Kafka and ClickHouse
- Consumes messages from `ads_basic` topic
- Creates ClickHouse table automatically
- Inserts each message into ClickHouse
- Demonstrates auto-commit

**Run** (in a separate terminal):

```bash
python tutorials/01-basics/consumer_clickhouse.py
```

**Expected output**:
```
[START] Consuming from topic: 'ads_basic'
[CLICKHOUSE] Connected and table 'ads_basic' ready
[RECV #1] topic=ads_basic partition=0 offset=0 | campaign=campaign_0 spend=42.15 USD
[CLICKHOUSE] Inserted seq=0 into ads_basic table
...
```

Press `Ctrl+C` to stop.

### Verify in ClickHouse

```bash
docker exec -it clickhouse clickhouse-client --password secret
```

```sql
-- See all messages
SELECT * FROM demo.ads_basic ORDER BY seq;

-- Count messages
SELECT COUNT(*) FROM demo.ads_basic;

-- Aggregate by campaign
SELECT campaign_id, SUM(spend) as total_spend
FROM demo.ads_basic
GROUP BY campaign_id;
```

### Exercise

1. Run producer to send 10 messages
2. Run consumer to consume and store in ClickHouse
3. Query ClickHouse to verify data
4. Run producer again (send 10 more messages)
5. Notice consumer automatically picks up new messages

---

## Tutorial 02: Serialization

**Goal**: Compare JSON vs Avro serialization, understand schema management.

### Concepts Covered

- Serialization and deserialization
- JSON: flexibility vs safety trade-offs
- Avro: schema enforcement and evolution
- Schema Registry
- Type safety

### Scripts

#### 2.1 JSON Producer

**File**: [tutorials/02-serialization/producer_json.py](tutorials/02-serialization/producer_json.py)

**What it does**:
- Produces JSON-encoded messages
- Demonstrates schema flexibility (pro and con)
- Shows missing fields, extra fields, wrong types
- All get sent successfully (no validation!)

**Run**:

```bash
python tutorials/02-serialization/producer_json.py
```

#### 2.2 JSON Consumer with ClickHouse

**File**: [tutorials/02-serialization/consumer_json_clickhouse.py](tutorials/02-serialization/consumer_json_clickhouse.py)

**What it does**:
- Consumes JSON messages
- Handles missing fields gracefully (nullable columns)
- Stores in ClickHouse with proper error handling
- Demonstrates defensive coding for JSON

**Run**:

```bash
python tutorials/02-serialization/consumer_json_clickhouse.py
```

#### 2.3 Avro Producer with Schema Registry

**File**: [tutorials/02-serialization/producer_avro.py](tutorials/02-serialization/producer_avro.py)

**What it does**:
- Connects to Schema Registry
- Registers Avro schema
- Produces Avro-encoded messages
- Validates data against schema (catches errors!)
- Demonstrates schema validation

**Run**:

```bash
python tutorials/02-serialization/producer_avro.py
```

**View registered schema**:

```bash
# List all schemas
curl http://localhost:8081/subjects

# Get specific schema
curl http://localhost:8081/subjects/ads_avro-value/versions/latest
```

#### 2.4 Avro Consumer with ClickHouse

**File**: [tutorials/02-serialization/consumer_avro_clickhouse.py](tutorials/02-serialization/consumer_avro_clickhouse.py)

**What it does**:
- Automatically fetches schema from Schema Registry
- Deserializes Avro messages
- Type-safe (no need for defensive coding)
- Stores in ClickHouse

**Run**:

```bash
python tutorials/02-serialization/consumer_avro_clickhouse.py
```

### Comparison: JSON vs Avro

| Feature | JSON | Avro |
|---------|------|------|
| **Readability** | Human-readable | Binary (not readable) |
| **Schema** | No enforcement | Enforced by registry |
| **Type Safety** | Runtime errors | Compile-time validation |
| **Size** | Verbose (larger) | Compact (smaller) |
| **Performance** | Slower ser/deser | Faster ser/deser |
| **Evolution** | Flexible but risky | Managed with compatibility checks |
| **Use When** | Debugging, rapid prototyping | Production, high throughput |

### Exercise

1. Run JSON producer - notice it accepts invalid data
2. Run Avro producer - notice it rejects invalid data
3. Compare message sizes (Avro is smaller)
4. Query both tables in ClickHouse and compare

---

## Tutorial 03: Partitioning

**Goal**: Understand how Kafka partitions topics for parallelism and ordering.

### Concepts Covered

- Partitions and parallelism
- Partition assignment strategies
- Key-based partitioning
- Consumer groups and rebalancing
- Scaling consumers

### Scripts

#### 3.1 Partitioned Producer

**File**: [tutorials/03-partitioning/producer_partitioned.py](tutorials/03-partitioning/producer_partitioned.py)

**What it does**:
- Creates topic with 3 partitions
- Demonstrates 3 partitioning strategies:
  - **Key-based**: Same key → same partition (ordering)
  - **Null key**: Round-robin distribution (no ordering)
  - **Manual**: Explicit partition assignment

**Run**:

```bash
python tutorials/03-partitioning/producer_partitioned.py
```

**Expected output**:
```
[CREATED] Topic 'ads_partitioned' with 3 partitions
STRATEGY 1: Key-Based Partitioning
[DELIVERED] ... partition=0 key=campaign_A
[DELIVERED] ... partition=0 key=campaign_A
[DELIVERED] ... partition=1 key=campaign_B
...
```

Notice how same key always goes to same partition!

#### 3.2 Parallel Consumers with ClickHouse

**File**: [tutorials/03-partitioning/consumer_parallel_clickhouse.py](tutorials/03-partitioning/consumer_parallel_clickhouse.py)

**What it does**:
- Joins consumer group
- Gets assigned partitions
- Tracks which partitions it reads from
- Stores partition info in ClickHouse

**Run** (Terminal 1):

```bash
python tutorials/03-partitioning/consumer_parallel_clickhouse.py consumer1
```

**Run** (Terminal 2):

```bash
python tutorials/03-partitioning/consumer_parallel_clickhouse.py consumer2
```

**Run** (Terminal 3):

```bash
python tutorials/03-partitioning/consumer_parallel_clickhouse.py consumer3
```

**Expected output**:
```
Terminal 1:
[ASSIGNMENT] Consumer consumer1 assigned: partition-0

Terminal 2:
[ASSIGNMENT] Consumer consumer2 assigned: partition-1

Terminal 3:
[ASSIGNMENT] Consumer consumer3 assigned: partition-2
```

Each consumer gets ONE partition (1:1 mapping with 3 partitions).

**Experiment**: Kill one consumer (`Ctrl+C`) and watch partitions rebalance!

### Verify in ClickHouse

```sql
-- Messages per partition
SELECT partition_id, COUNT(*) as count
FROM demo.ads_partitioned
GROUP BY partition_id;

-- Messages per consumer
SELECT consumer_instance, COUNT(*) as count
FROM demo.ads_partitioned
GROUP BY consumer_instance;

-- Verify same campaign goes to same partition
SELECT campaign_id, partition_id, COUNT(*) as count
FROM demo.ads_partitioned
GROUP BY campaign_id, partition_id
ORDER BY campaign_id, partition_id;
```

### Exercise

1. Run producer to create partitioned topic
2. Run 1 consumer - it gets all 3 partitions
3. Start 2nd consumer - watch rebalance (each gets 1-2 partitions)
4. Start 3rd consumer - each gets 1 partition
5. Kill consumer2 - watch partitions reassign to consumer1 and consumer3
6. Query ClickHouse to see distribution

---

## Tutorial 04: Reliability

**Goal**: Achieve at-least-once delivery with idempotence and manual commits.

### Concepts Covered

- Producer retries and duplicates
- Idempotent producer (exactly-once on send)
- Acknowledgment levels (acks)
- Auto-commit pitfalls
- Manual offset commit
- At-least-once vs at-most-once

### Scripts

#### 4.1 Idempotent Producer

**File**: [tutorials/04-reliability/producer_idempotent.py](tutorials/04-reliability/producer_idempotent.py)

**What it does**:
- Demonstrates non-idempotent producer (can duplicate)
- Demonstrates idempotent producer (prevents duplicates)
- Explains acks levels (0, 1, all)
- Shows retry configuration

**Run**:

```bash
python tutorials/04-reliability/producer_idempotent.py
```

**Expected output**:
```
DEMO 1: Non-Idempotent Producer
[RISK] If network errors cause retries, messages may be duplicated!

DEMO 2: Idempotent Producer
[GUARANTEE] Even if retries occur, messages will NOT be duplicated!

DEMO 3: Acknowledgment Levels
acks=0: No acknowledgment (fire-and-forget)
acks=1: Leader acknowledgment only
acks=all: All in-sync replicas acknowledge
```

#### 4.2 Manual Commit Consumer with ClickHouse

**File**: [tutorials/04-reliability/consumer_manual_commit_clickhouse.py](tutorials/04-reliability/consumer_manual_commit_clickhouse.py)

**What it does**:
- Disables auto-commit
- Commits offset AFTER ClickHouse write
- Demonstrates 3 commit strategies:
  - Per-message (safest, slowest)
  - Per-batch (balanced)
  - Periodic (fastest, riskiest)
- Handles errors gracefully

**Run**:

```bash
python tutorials/04-reliability/consumer_manual_commit_clickhouse.py
```

**Expected output**:
```
[STRATEGY] Commit strategy: per_message
[RECV #1] partition=0 offset=0 | seq=0
[CLICKHOUSE] Inserted seq=0
[COMMIT] Committed offset 1 (per-message)
```

### Why Manual Commit Matters

**Auto-commit scenario (BAD)**:
1. Consumer polls messages 1-100
2. Auto-commit commits offset 100 every 5 seconds
3. Consumer crashes at message 50
4. **Messages 51-100 are LOST!**

**Manual commit scenario (GOOD)**:
1. Consumer processes message 1
2. Consumer writes to ClickHouse
3. Consumer commits offset 1
4. Consumer crashes
5. On restart, resumes at message 2
6. **No data loss!** (might reprocess message 1 if crashed before commit)

### Exercise

1. Run idempotent producer to send messages
2. Run manual commit consumer
3. Kill consumer (Ctrl+C) while it's processing
4. Restart consumer - notice it resumes from last commit
5. Verify in ClickHouse that no messages were lost

---

## Tutorial 05: Transactions

**Goal**: Achieve exactly-once end-to-end semantics with transactions.

### Concepts Covered

- Kafka transactions
- Exactly-once semantics (EOS)
- Atomic multi-topic writes
- Transaction abort and commit
- Read committed isolation level

### Scripts

#### 5.1 Transactional Producer

**File**: [tutorials/05-transactions/producer_transactional.py](tutorials/05-transactions/producer_transactional.py)

**What it does**:
- Initializes transactional producer
- Demonstrates simple transaction (commit)
- Demonstrates transaction abort (rollback)
- Shows multi-topic atomic writes

**Run**:

```bash
python tutorials/05-transactions/producer_transactional.py
```

**Expected output**:
```
DEMO 1: Simple Transaction
[TXN] Transaction started
[TXN] Produced seq=0 (buffered)
[TXN] Committing transaction...
[TXN] All 3 messages are now visible to consumers

DEMO 2: Transaction Abort
[ERROR] Simulating processing error...
[TXN] Aborting transaction...
[TXN] Messages seq=10,11,12 were discarded (not visible to consumers)

DEMO 3: Multi-Topic Atomic Write
[TXN] Messages in ads_transactions_output and ads_transactions_summary are atomically visible
```

#### 5.2 Read Committed Consumer with ClickHouse

**File**: [tutorials/05-transactions/consumer_read_committed_clickhouse.py](tutorials/05-transactions/consumer_read_committed_clickhouse.py)

**What it does**:
- Demonstrates `read_uncommitted` (sees aborted transactions)
- Demonstrates `read_committed` (filters aborted transactions)
- Shows exactly-once processing
- Stores in ClickHouse with manual commit

**Run**:

```bash
python tutorials/05-transactions/consumer_read_committed_clickhouse.py
```

**Expected output**:
```
DEMO 1: read_uncommitted
[RECV #4] ⚠️  Aborted transaction: seq=10 txn=demo2_aborted
[WARNING] With read_uncommitted, you saw 3 messages that were supposed to be rolled back!

DEMO 2: read_committed
[SUCCESS] ✓ Aborted transaction (demo2_aborted) was correctly filtered!
```

### Exactly-Once Recipe

**Producer**:
```python
"enable.idempotence": True
"transactional.id": "unique-id"

producer.init_transactions()
producer.begin_transaction()
# produce messages
producer.commit_transaction()
```

**Consumer**:
```python
"isolation.level": "read_committed"
"enable.auto.commit": False

# poll message
# process message
# write to ClickHouse
consumer.commit()  # Commit offset
```

**Result**: Each message processed exactly once, even with failures!

### Exercise

1. Run transactional producer
2. Note which transactions committed vs aborted
3. Run consumer with `read_uncommitted` - see aborted messages
4. Run consumer with `read_committed` - aborted messages filtered
5. Verify in ClickHouse only committed data exists

---

## ClickHouse Queries

### Basic Queries

```sql
-- Connect to ClickHouse
docker exec -it clickhouse clickhouse-client --password secret

-- List all databases
SHOW DATABASES;

-- Use demo database
USE demo;

-- List all tables
SHOW TABLES;

-- See table schema
DESCRIBE TABLE ads_basic;
```

### Tutorial 01: Basic Data

```sql
-- All messages
SELECT * FROM ads_basic ORDER BY seq LIMIT 10;

-- Total spend by campaign
SELECT campaign_id, SUM(spend) as total_spend
FROM ads_basic
GROUP BY campaign_id;

-- Messages per day
SELECT toDate(inserted_at) as date, COUNT(*) as count
FROM ads_basic
GROUP BY date;
```

### Tutorial 02: JSON vs Avro

```sql
-- JSON table (has nullable fields)
SELECT COUNT(*) as total,
       COUNT(spend) as with_spend,
       COUNT(*) - COUNT(spend) as missing_spend
FROM ads_json;

-- Avro table (no nulls, type-safe)
SELECT COUNT(*) FROM ads_avro;

-- Compare sizes (Avro should be smaller on disk)
SELECT table, formatReadableSize(total_bytes) as size
FROM system.tables
WHERE database = 'demo' AND table IN ('ads_json', 'ads_avro');
```

### Tutorial 03: Partitioning

```sql
-- Messages per Kafka partition
SELECT partition_id, COUNT(*) as count
FROM ads_partitioned
GROUP BY partition_id
ORDER BY partition_id;

-- Verify key-based partitioning (same campaign = same partition)
SELECT campaign_id,
       groupArray(DISTINCT partition_id) as partitions
FROM ads_partitioned
GROUP BY campaign_id;

-- Consumer distribution
SELECT consumer_instance,
       groupArray(DISTINCT partition_id) as partitions,
       COUNT(*) as messages
FROM ads_partitioned
GROUP BY consumer_instance;
```

### Tutorial 04: Reliability

```sql
-- Processing time statistics
SELECT AVG(processing_time_ms) as avg_ms,
       quantile(0.5)(processing_time_ms) as median_ms,
       quantile(0.95)(processing_time_ms) as p95_ms,
       quantile(0.99)(processing_time_ms) as p99_ms
FROM ads_reliability;

-- Idempotent vs non-idempotent
SELECT idempotent, COUNT(*) as count
FROM ads_reliability
GROUP BY idempotent;
```

### Tutorial 05: Transactions

```sql
-- Messages per transaction
SELECT transaction, COUNT(*) as count
FROM ads_transactions
GROUP BY transaction
ORDER BY transaction;

-- Verify aborted transaction is NOT in data
SELECT COUNT(*) FROM ads_transactions
WHERE transaction = 'demo2_aborted';
-- Should return 0!

-- Time series of inserted data
SELECT toStartOfMinute(inserted_at) as minute,
       transaction,
       COUNT(*) as count
FROM ads_transactions
GROUP BY minute, transaction
ORDER BY minute, transaction;
```

### Advanced Analytics

```sql
-- Find duplicates (if any)
SELECT seq, COUNT(*) as duplicates
FROM ads_basic
GROUP BY seq
HAVING duplicates > 1;

-- Campaign performance over time
SELECT campaign_id,
       toStartOfHour(inserted_at) as hour,
       COUNT(*) as events,
       SUM(spend) as total_spend,
       AVG(spend) as avg_spend
FROM ads_basic
GROUP BY campaign_id, hour
ORDER BY campaign_id, hour;
```

---

## Troubleshooting

### Kafka Issues

**Problem**: Can't connect to Kafka

```bash
# Check Kafka is running
docker ps | grep kafka

# Check Kafka logs
docker logs kafka

# Verify Kafka is listening
docker exec -it kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

**Problem**: Topic not auto-created

```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create topic manually
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic my_topic --partitions 3 --replication-factor 1

# Describe topic
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic my_topic
```

**Problem**: Consumer not receiving messages

```bash
# Check consumer group status
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group my_group --describe

# Reset consumer group (start from beginning)
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group my_group --topic my_topic --reset-offsets --to-earliest --execute
```

### Schema Registry Issues

**Problem**: Can't connect to Schema Registry

```bash
# Check Schema Registry is running
docker ps | grep schema-registry

# Check Schema Registry logs
docker logs schema-registry

# Test Schema Registry REST API
curl http://localhost:8081/subjects
```

**Problem**: Schema compatibility issues

```bash
# Get schema versions
curl http://localhost:8081/subjects/my-topic-value/versions

# Get specific version
curl http://localhost:8081/subjects/my-topic-value/versions/1

# Check compatibility
curl -X POST http://localhost:8081/compatibility/subjects/my-topic-value/versions/latest \
  -H "Content-Type: application/json" \
  -d '{"schema": "..."}'
```

### ClickHouse Issues

**Problem**: Can't connect to ClickHouse

```bash
# Check ClickHouse is running
docker ps | grep clickhouse

# Check ClickHouse logs
docker logs clickhouse

# Test connection
docker exec -it clickhouse clickhouse-client --password secret --query "SELECT 1"
```

**Problem**: Table doesn't exist

```sql
-- List all tables
SHOW TABLES FROM demo;

-- Create table manually (example)
CREATE TABLE IF NOT EXISTS demo.ads_basic (
    inserted_at DateTime DEFAULT now(),
    seq UInt64,
    campaign_id String,
    spend Float64,
    currency String
) ENGINE = MergeTree()
ORDER BY seq;
```

**Problem**: Permission denied

```bash
# ClickHouse requires password for default user
# Always use: --password secret
docker exec -it clickhouse clickhouse-client --password secret
```

### Python Issues

**Problem**: Module not found

```bash
# Make sure venv is activated
source .venv/bin/activate  # Linux/Mac
source .venv/Scripts/activate  # Windows

# Reinstall dependencies
pip install -r requirements.txt

# Or install manually
pip install confluent-kafka==2.5.3 clickhouse-connect==0.7.0
```

**Problem**: Script hangs

- Producer might be waiting for Kafka (check Kafka is running)
- Consumer might be waiting for messages (check topic has data)
- Use `Ctrl+C` to stop gracefully

### Reset Everything

```bash
# Stop all services
docker compose down

# Remove all data (WARNING: deletes all Kafka/ClickHouse data)
rm -rf data/
docker volume rm kafka_ch_data kafka_ch_logs

# Start fresh
docker compose up -d

# Verify
docker ps
```

---

## Next Steps

1. **Complete all tutorials** in order (01 → 05)
2. **Experiment** with the code:
   - Change partition counts
   - Modify schemas
   - Add new fields
   - Test failure scenarios
3. **Query ClickHouse** to understand the data
4. **Read the comments** in each script (they explain Kafka concepts)
5. **Build your own pipeline**:
   - Producer: Generate your own data
   - Consumer: Process and store in ClickHouse
   - Analytics: Query and visualize

## Additional Resources

- **Confluent Kafka Python Docs**: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/
- **Kafka Documentation**: https://kafka.apache.org/documentation/
- **ClickHouse Documentation**: https://clickhouse.com/docs/
- **Schema Registry REST API**: https://docs.confluent.io/platform/current/schema-registry/develop/api.html

---

**Happy Learning!** 🚀
