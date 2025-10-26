# Kafka Tutorial Repository

A comprehensive, hands-on tutorial for learning Apache Kafka with ClickHouse integration. Designed for team training with heavily commented code and progressive complexity.

## Quick Start

```bash
# 1. Clone and navigate
git clone <your-repo-url>
cd kafka

# 2. Start infrastructure (Kafka, Schema Registry, ClickHouse)
docker compose up -d

# 3. Setup Python environment
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt

# 4. Run first tutorial
python tutorials/01-basics/producer.py
python tutorials/01-basics/consumer_clickhouse.py

# 5. Query data in ClickHouse
docker exec -it clickhouse clickhouse-client --password secret
SELECT * FROM demo.ads_basic;
```

## What You'll Learn

- **Tutorial 01: Basics** - Producers, consumers, topics, offsets, consumer groups
- **Tutorial 02: Serialization** - JSON vs Avro, schema registry, type safety
- **Tutorial 03: Partitioning** - Parallelism, ordering, consumer scaling
- **Tutorial 04: Reliability** - Idempotence, manual commits, at-least-once delivery
- **Tutorial 05: Transactions** - Exactly-once semantics, atomic multi-topic writes

## Repository Structure

```
kafka/
├── docker-compose.yml           # Infrastructure (Kafka, Schema Registry, ClickHouse)
├── init_clickhouse.sql          # Auto-creates ClickHouse tables
├── TUTORIAL.md                  # Comprehensive tutorial guide
├── README.md                    # This file
├── requirements.txt             # Python dependencies
│
├── tutorials/                   # Main tutorial scripts (USE THESE!)
│   ├── 01-basics/              # Simple producer and consumer
│   ├── 02-serialization/       # JSON and Avro
│   ├── 03-partitioning/        # Parallelism and scaling
│   ├── 04-reliability/         # At-least-once delivery
│   └── 05-transactions/        # Exactly-once semantics
│
└── scripts/                     # Old scripts (archived, see scripts/OLD_SCRIPTS_README.md)
```

## Features

- **Heavily Commented**: Every line explained for learning
- **Fully Typed**: All functions and variables have type annotations
- **ClickHouse Integration**: See and query all messages in SQL
- **Progressive Complexity**: Start simple, build to production patterns
- **Production-Ready**: Best practices and real-world patterns

## Documentation

- **[TUTORIAL.md](TUTORIAL.md)** - Complete tutorial with exercises
- **[Confluent Kafka Python Docs](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/)** - Official client docs
- **[Kafka Documentation](https://kafka.apache.org/documentation/)** - Official Kafka docs
- **[ClickHouse Docs](https://clickhouse.com/docs/)** - ClickHouse documentation

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Basic understanding of message queues (helpful but not required)

## Infrastructure

All services run in Docker containers:

- **Kafka** (port 9092) - Apache Kafka 4.0.0 with KRaft (no ZooKeeper)
- **Schema Registry** (port 8081) - Confluent Schema Registry for Avro schemas
- **ClickHouse** (ports 8123, 9000) - Analytics database for querying Kafka data

## Support

For issues or questions:
1. Check [TUTORIAL.md](TUTORIAL.md) troubleshooting section
2. Review script comments (they explain concepts in detail)
3. Check ClickHouse for data (`docker exec -it clickhouse clickhouse-client --password secret`)

## Contributing

This is a learning repository. Feel free to:
- Add more tutorials
- Improve comments and explanations
- Add more ClickHouse queries
- Report issues or suggest improvements

## License

Educational use - feel free to use for team training and learning.

---

**Start here**: [TUTORIAL.md](TUTORIAL.md)
