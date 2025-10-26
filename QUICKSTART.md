# Kafka Tutorial - Quick Start Guide

Get up and running in 5 minutes!

## Step 1: Start Infrastructure (2 min)

```bash
# Start Kafka, Schema Registry, and ClickHouse
docker compose up -d

# Verify all services are running
docker compose ps

# Should see 3 services: kafka, schema-registry, clickhouse
```

## Step 2: Setup Python Environment (1 min)

```bash
# Create virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate  # Linux/Mac
# OR
source .venv/Scripts/activate  # Windows Git Bash

# Install dependencies
pip install -r requirements.txt
```

## Step 3: Run Your First Tutorial (2 min)

**Terminal 1 - Start Consumer:**
```bash
python tutorials/01-basics/consumer_clickhouse.py
```

You should see:
```
[START] Consuming from topic: 'ads_basic'
[CLICKHOUSE] Table 'ads_basic' ready
```

**Terminal 2 - Run Producer:**
```bash
python tutorials/01-basics/producer.py
```

You should see:
```
[START] Producing messages to topic: 'ads_basic'
[DELIVERED] topic=ads_basic partition=0 offset=0
[SUCCESS] All 10 messages delivered successfully!
```

**Back in Terminal 1** - you should see messages being consumed:
```
[RECV #1] topic=ads_basic partition=0 offset=0 | campaign=campaign_0 spend=42.15 USD
[CLICKHOUSE] Inserted seq=0 into ads_basic table
```

## Step 4: Query Data in ClickHouse

```bash
# Open ClickHouse SQL shell
docker exec -it clickhouse clickhouse-client --password secret
```

```sql
-- See your messages
SELECT * FROM demo.ads_basic ORDER BY seq;

-- Count messages
SELECT COUNT(*) FROM demo.ads_basic;

-- Exit
exit;
```

## What Just Happened?

1. **Producer** sent 10 ad spend events to Kafka topic `ads_basic`
2. **Kafka** stored the messages in partitions (durable log)
3. **Consumer** read messages from Kafka and wrote them to ClickHouse
4. **ClickHouse** stored the data for SQL queries and analytics

## Next Steps

Continue with the full tutorial:

```bash
# Read the comprehensive guide
cat TUTORIAL.md

# Or open in browser
open TUTORIAL.md  # Mac
xdg-open TUTORIAL.md  # Linux
```

## Common Commands

```bash
# Stop all services
docker compose down

# View logs
docker logs kafka
docker logs schema-registry
docker logs clickhouse

# List Kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Access ClickHouse shell
docker exec -it clickhouse clickhouse-client --password secret

# Deactivate Python venv
deactivate
```

## Troubleshooting

**Can't connect to Kafka?**
```bash
docker logs kafka  # Check for errors
```

**Can't connect to ClickHouse?**
```bash
# Use password: secret
docker exec -it clickhouse clickhouse-client --password secret
```

**Module not found?**
```bash
# Make sure venv is activated
source .venv/bin/activate
pip install -r requirements.txt
```

**Reset everything:**
```bash
docker compose down
rm -rf data/
docker compose up -d
```

---

**Ready to learn more?** → See [TUTORIAL.md](TUTORIAL.md) for the complete tutorial!
