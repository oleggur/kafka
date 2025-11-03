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
pip install -r requirements.txt
```

### 4. Verify Setup

Check Kafka is accessible:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
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

**Goal**: Send and receive your first Kafka messages.

### Concepts Covered

- Kafka producers (sending messages)
- Kafka consumers (receiving messages)
- Topics (named message streams)
- Offsets (message position in log)
- ClickHouse integration (storing Kafka data)

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

### What Just Happened?

**Producer → Kafka → Consumer → ClickHouse:**

1. **Producer** sent 10 messages to Kafka topic `ads_basic`
2. **Kafka** stored messages in a durable log (survives restarts)
3. **Consumer** read messages from Kafka
4. **ClickHouse** stored messages for SQL queries

**Key terms:**
- **Topic**: A named stream of messages (like `ads_basic`)
- **Offset**: Position of each message in the log (`offset=0`, `offset=1`, ...)
- **Partition**: Topics are split into partitions for parallelism (more in Tutorial 03)

That's it for the basics! Concepts like consumer groups, commits, and partitioning are covered in later tutorials.

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

### Cleanup (Tutorial 01)

Before starting, clean up any previous data:

```bash
# Delete Kafka topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_basic

# Clear ClickHouse table
docker exec -it clickhouse clickhouse-client --password secret --query "TRUNCATE TABLE demo.ads_basic"
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

### Compare Serialized Message Sizes

To see the actual serialized message sizes (what's sent over the wire in Kafka):

```bash
# Check JSON topic size
docker exec kafka /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server localhost:9092 \
  --topic-list ads_json --describe | grep -A5 ads_json

# Check Avro topic size
docker exec kafka /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server localhost:9092 \
  --topic-list ads_avro --describe | grep -A5 ads_avro
```

**Expected result**: Avro messages are typically 30-50% smaller than equivalent JSON messages because:
- Binary encoding (no field names repeated in each message)
- Compact number representation
- Schema stored separately (not in each message)

### Cleanup (Tutorial 02)

Before starting, clean up any previous data:

```bash
# Delete Kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_json
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_avro

# Clear ClickHouse tables
docker exec -it clickhouse clickhouse-client --password secret --query "TRUNCATE TABLE demo.ads_json"
docker exec -it clickhouse clickhouse-client --password secret --query "TRUNCATE TABLE demo.ads_avro"
```

### Exercise

1. Run JSON producer - notice it accepts invalid data (missing fields, wrong types)
2. Run Avro producer - notice it rejects invalid data at serialization time
3. Run both consumers to store data in ClickHouse
4. Compare Kafka topic sizes (see commands above) - Avro is more compact
5. Query both tables in ClickHouse to see the data

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

### Consumer Groups Explained

**What is a Consumer Group?**
- Consumers with the same `group.id` form a **consumer group**
- They work together to share partitions of a topic
- Each partition is assigned to exactly ONE consumer in the group

**Why Consumer Groups?**
- **Parallel processing**: Multiple consumers = faster consumption
- **Automatic load balancing**: Kafka distributes partitions evenly
- **Fault tolerance**: If a consumer dies, its partitions reassign to others

**Rebalancing:**
- When a consumer joins/leaves, Kafka **rebalances** (redistributes partitions)
- During rebalance, consumption pauses briefly
- After rebalance, each consumer knows which partitions it owns

**Example with 3 partitions:**
- 1 consumer: gets all 3 partitions
- 2 consumers: each gets 1-2 partitions
- 3 consumers: each gets 1 partition (optimal)
- 4+ consumers: some are idle (more consumers than partitions)

**Independent Groups:**
- Different `group.id` = different consumer groups
- Each group reads independently (own offsets, own progress)
- Example: One group for ClickHouse sink, another for alerting

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

### Cleanup (Tutorial 03)

Before starting, clean up any previous data:

```bash
# Delete Kafka topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_partitioned

# Clear ClickHouse table
docker exec -it clickhouse clickhouse-client --password secret --query "TRUNCATE TABLE demo.ads_partitioned"
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

### Offset Commits Explained

**What is an Offset Commit?**
- A commit saves your current read position (offset) to Kafka
- Kafka stores commits in a special topic `__consumer_offsets`
- On restart, the consumer resumes from the last committed offset
- Commits are tracked per consumer group + partition

**Auto-commit (Tutorial 01):**
- Enabled by default (`enable.auto.commit=true`)
- Kafka automatically commits every 5 seconds
- Simple but risky - can lose data (see below)

**Manual commit (Tutorial 04):**
- You control when to commit (`enable.auto.commit=false`)
- Commit AFTER processing succeeds
- More code, but safer

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

#### 4.2 Auto-Commit vs Manual Commit Consumer

**File**: [tutorials/04-reliability/consumer_manual_commit_clickhouse.py](tutorials/04-reliability/consumer_manual_commit_clickhouse.py)

**What it does**:
- Supports two modes: `auto` (auto-commit) and `manual` (manual commit)
- Auto-commit mode: Demonstrates data LOSS risk (at-most-once)
- Manual commit mode: Demonstrates NO data loss (at-least-once)
- Slow processing (2 seconds per message) to make the problem visible

**Run**:

```bash
# Auto-commit mode (demonstrates data loss)
python tutorials/04-reliability/consumer_manual_commit_clickhouse.py auto

# Manual commit mode (safe - no data loss)
python tutorials/04-reliability/consumer_manual_commit_clickhouse.py manual
```

### Cleanup (Tutorial 04)

Before starting, clean up any previous data:

```bash
# Delete Kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_reliability_nonidem
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_reliability_idem

# Clear ClickHouse table
docker exec -it clickhouse clickhouse-client --password secret --query "TRUNCATE TABLE demo.ads_reliability"
```

### Exercise: Auto-Commit vs Manual Commit Comparison

**Goal**: See side-by-side how manual commit AFTER insert prevents data loss

Produce 10 messages:
```bash
python tutorials/04-reliability/producer_idempotent.py
```

---

#### Part 1: Auto-Commit (DATA LOSS)

1. Run consumer in **auto-commit mode**:
   ```bash
   python tutorials/04-reliability/consumer_manual_commit_clickhouse.py auto
   ```

2. Watch the output - you'll see:
   - Messages being received and processed slowly (2 seconds each)
   - "AUTO-COMMIT" messages indicating offset committed in background

3. **After processing 2-3 messages, press Ctrl+C** to simulate crash

4. Check what was actually saved to ClickHouse:
   ```bash
   docker exec -it clickhouse clickhouse-client --password secret
   ```
   ```sql
   SELECT seq FROM demo.ads_reliability ORDER BY seq;
   ```

   Example: You'll see only `0, 1, 2` (3 messages saved)

5. Restart the consumer:
   ```bash
   python tutorials/04-reliability/consumer_manual_commit_clickhouse.py auto
   ```

6. **Notice: It resumes from a LATER offset** (auto-commit already committed offsets in background)
   - It skips messages that were fetched but not processed
   - Check ClickHouse again - **MISSING MESSAGES** (e.g., seq 3-9 are gone forever!)

**Result**: ❌ **DATA LOSS** - Auto-commit committed offsets before processing completed

---

#### Part 2: Manual Commit (NO DATA LOSS)

1. Clean up and restart:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_reliability_idem
   docker exec -it clickhouse clickhouse-client --password secret --query "TRUNCATE TABLE demo.ads_reliability"
   python tutorials/04-reliability/producer_idempotent.py  # Produce 10 messages again
   ```

2. Run consumer in **manual commit mode**:
   ```bash
   python tutorials/04-reliability/consumer_manual_commit_clickhouse.py manual
   ```

3. Watch the output - you'll see:
   - Messages being processed slowly (2 seconds each)
   - "[COMMIT] Committed offset X" AFTER each insert
   - Commit happens AFTER ClickHouse write, not before!

4. **After processing 2-3 messages, press Ctrl+C** during the "[PROCESSING]..." message

5. Check what was saved:
   ```bash
   docker exec -it clickhouse clickhouse-client --password secret
   ```
   ```sql
   SELECT seq FROM demo.ads_reliability ORDER BY seq;
   ```

6. Restart the consumer:
   ```bash
   python tutorials/04-reliability/consumer_manual_commit_clickhouse.py manual
   ```

7. **Notice: It resumes from the LAST COMMITTED offset**
   - It reprocesses the message you were processing when it crashed
   - All messages eventually get processed (no data loss!)

8. Check ClickHouse - **ALL 10 MESSAGES** are present:
   ```sql
   SELECT seq, COUNT(*) as count
   FROM demo.ads_reliability
   GROUP BY seq
   ORDER BY seq;
   ```

**Result**: ✅ **NO DATA LOSS** - Manual commit after insert ensures all messages are processed

---

### Key Takeaway

| Mode | Data Loss? | Why? |
|------|-----------|------|
| **Auto-commit** | ❌ YES | Commits offsets periodically in background, BEFORE processing completes. Crash → unprocessed messages skipped. |
| **Manual commit AFTER insert** | ✅ NO | Commits only AFTER successful processing. Crash → message reprocessed on restart. |

**Recommendation**: Always use manual commit AFTER critical operations (database writes, API calls) to prevent data loss!

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

DEMO 2: Transaction Abort (added sleep 10 so you can start the consumer and see aborted messages)
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

**Important Note**: The `read_uncommitted` consumer will only see messages from **ongoing (not yet committed/aborted) transactions**. Once a transaction is aborted, even `read_uncommitted` consumers won't see those messages. To observe the difference between isolation levels, you would need the producer to keep a transaction open while the consumer is reading.

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

### Cleanup (Tutorial 05)

Before starting, clean up any previous data:

```bash
# Delete Kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_transactions
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_transactions_output
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ads_transactions_summary

# Clear ClickHouse table
docker exec -it clickhouse clickhouse-client --password secret --query "TRUNCATE TABLE demo.ads_transactions"
```

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
docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

**Problem**: Topic not auto-created

```bash
# List topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create topic manually
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic my_topic --partitions 3 --replication-factor 1

# Describe topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic my_topic
```

**Problem**: Consumer not receiving messages

```bash
# Check consumer group status
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my_group --describe

# Reset consumer group (start from beginning)
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
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
