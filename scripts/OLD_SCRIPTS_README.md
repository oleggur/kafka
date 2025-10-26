# Old Scripts Directory

This directory contains the original scripts before reorganization. These scripts are kept for reference but are not part of the main tutorial.

## Migration to New Tutorial Structure

All scripts have been reorganized into the `tutorials/` directory with the following structure:

- `tutorials/01-basics/` - Basic producer and consumer (replaces `day5/`)
- `tutorials/02-serialization/` - JSON and Avro serialization (replaces `producer_json.py`, `consumer_json.py`, `producer_avro.py`, `consumer_avro.py`)
- `tutorials/03-partitioning/` - Partitioning and parallelism (replaces `producer_partitioned.py`, `consumer_partitioned.py`)
- `tutorials/04-reliability/` - Idempotence and manual commits (replaces `producer_reliable.py`, `producer_retries_demo.py`, `consumer_json_manual.py`)
- `tutorials/05-transactions/` - Transactional processing (replaces `transactions/`)

## Key Improvements in New Tutorials

1. **Heavy Comments**: Every script has extensive comments explaining Kafka concepts
2. **Type Annotations**: All functions and variables are fully typed
3. **ClickHouse Integration**: Every consumer writes to ClickHouse for persistence and analytics
4. **Progressive Learning**: Tutorials build on each other from simple to complex
5. **Consistent Naming**: Clear naming conventions for topics and scripts
6. **Error Handling**: Proper error handling and recovery patterns
7. **Best Practices**: Production-ready patterns and configurations

## Original Script Mapping

| Old Script | New Tutorial | Notes |
|------------|--------------|-------|
| `day5/simple_producer.py` | `tutorials/01-basics/producer.py` | Enhanced with more comments |
| `day5/simple_consumer_clickhouse.py` | `tutorials/01-basics/consumer_clickhouse.py` | Enhanced with offset management explanation |
| `producer_json.py` | `tutorials/02-serialization/producer_json.py` | Added schema flexibility demo |
| `consumer_json.py` | `tutorials/02-serialization/consumer_json_clickhouse.py` | Added ClickHouse integration |
| `producer_avro.py` | `tutorials/02-serialization/producer_avro.py` | Added validation demo |
| `consumer_avro.py` | `tutorials/02-serialization/consumer_avro_clickhouse.py` | Added ClickHouse integration |
| `producer_partitioned.py` | `tutorials/03-partitioning/producer_partitioned.py` | Added multiple partitioning strategies |
| `consumer_partitioned.py` | `tutorials/03-partitioning/consumer_parallel_clickhouse.py` | Added parallel consumer demo |
| `producer_reliable.py` | `tutorials/04-reliability/producer_idempotent.py` | Added idempotence explanation |
| `producer_retries_demo.py` | `tutorials/04-reliability/producer_idempotent.py` | Merged into idempotence demo |
| `consumer_json_manual.py` | `tutorials/04-reliability/consumer_manual_commit_clickhouse.py` | Added commit strategies |
| `transactions/seed_input.py` | Not migrated | Not needed for tutorial |
| `transactions/tx_pipeline.py` | `tutorials/05-transactions/producer_transactional.py` | Simplified and enhanced |
| `transactions/consumer_read_committed.py` | `tutorials/05-transactions/consumer_read_committed_clickhouse.py` | Added isolation level comparison |
| `consumer_seq_dump.py` | Not migrated | Debug script, not tutorial material |
| `check_duplicates.py` | Not migrated | Utility script, not tutorial material |

## Recommendation

**Use the new `tutorials/` directory** for learning and team training. These old scripts are kept only for reference and backward compatibility.

If you need any functionality from the old scripts that's not in the tutorials, please refer to this mapping table to find the equivalent tutorial.

## Cleanup

To remove these old scripts (after verifying tutorials work):

```bash
# Backup old scripts first
mv scripts scripts_backup

# Or delete them
rm -rf scripts
```

The new tutorials are self-contained and don't require these old scripts.
