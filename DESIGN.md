# Mini-Kafka — Design Notes

> Architecture decisions, trade-offs, and what I'd do differently at production scale.

---

## Storage: Why segment files instead of a single log file?

Rolling segments at a fixed size (1 MB in this implementation, 1 GB in Kafka) enables three things that a single file can't do cleanly:

1. **Log retention** — delete the oldest segment atomically by unlinking one file rather than rewriting the whole log.
2. **Parallel I/O** — different consumers can read different segments simultaneously without contention.
3. **Recovery bounds** — on crash, only the active segment (the last one written) can be incomplete. Older sealed segments are immutable.

The sparse index (one entry per 512 bytes) limits the index size to ~1% of the log size while keeping seek time to O(log n) binary search + O(small constant) linear scan.

---

## Storage: Why `WriteAt` instead of `O_APPEND`?

`O_APPEND` is POSIX-atomic — every write goes to EOF regardless of any preceding `Seek()` call. This is ideal for pure append-only workloads but makes it impossible to write at specific byte offsets for testing (e.g. corruption injection). Using `WriteAt` with an explicitly tracked `logSize` position is equally correct for a single writer protected by a mutex, and it lets tests corrupt specific byte ranges to validate CRC detection.

In production Kafka, the log file is also opened without `O_APPEND` — explicit position tracking is the standard approach.

---

## Wire protocol: Why custom binary instead of Kafka compatibility?

Building a wire-compatible Kafka protocol would add substantial complexity (variable-length encoding, schema versioning, SASL) without adding learning value. The goal is to implement the *design* of Kafka — segment storage, ISR replication, consumer group protocol — not to clone its wire encoding.

The custom protocol is simpler to reason about: big-endian fixed-width integers, 2-byte length-prefixed strings, 4-byte length-prefixed byte slices. Every field is explicit in the encoding helpers in `internal/protocol/frame.go`.

---

## Partition routing: Why FNV-1a?

FNV-1a (32-bit) is the algorithm Kafka's `DefaultPartitioner` uses for keyed messages. It's:

- **Fast** — single-pass, no division beyond the final modulo.
- **Well-distributed** — empirically good for string keys.
- **Stable** — the same key always maps to the same partition, which is the ordering guarantee producers rely on.

Round-robin for keyless messages uses a per-topic `atomic.Uint64` counter, which is lock-free under concurrent producers on the same topic.

---

## Metadata persistence: Why bbolt instead of flat files?

Topic configurations (name, partition count, replication factor) must survive broker restarts. The options are:

| Approach | Crash safety | Complexity |
|----------|-------------|------------|
| Flat files + fsync | Fragile if crash between write and sync | Medium |
| bbolt (BoltDB) | ACID transactions, B+ tree | Low |
| etcd / ZooKeeper | Full consensus | Very high |

bbolt gives ACID transactions with a single write lock per file and O(log n) reads — exactly right for a metadata store that is read-heavy and rarely written. The database file holds a single `"topics"` bucket with topic name as key and JSON config as value.

**Ordering of operations in `CreateTopic`**: log directories are created first, then bbolt is written. If the broker crashes between the two, orphaned log directories exist but bbolt has no record — they are skipped on next startup (harmless). The reverse ordering (bbolt first) would cause replay to fail if directories weren't created yet, which is harder to recover from.

---

## Consumer groups: Why block the connection goroutine in JoinGroup?

The JoinGroup response must be delayed until all members have joined (or the rebalance delay window closes). Two implementation options:

1. **Block the goroutine** — park the connection's goroutine inside the coordinator until the phase closes, then respond.
2. **Async callback** — immediately return a "pending" response, then push the real response later.

Option 1 is simpler and is exactly what Kafka does. Each connection has its own goroutine; goroutines are cheap in Go (~2 KB stack, grows as needed). The 500ms rebalance delay window is the only time a goroutine is parked, and it holds no locks while waiting.

Option 2 would require a response-queuing layer keyed by connection + correlation ID — significantly more complex with no user-visible benefit, since the client is already blocked waiting.

---

## Consumer groups: Range assignor

The range assignor assigns contiguous partition ranges to sorted members:

```
partitions_per_member = ceil(total_partitions / num_members)
member[0] → [0, partitions_per_member)
member[1] → [partitions_per_member, 2*partitions_per_member)
...
```

Members are sorted by member ID (deterministic string sort) so all members compute the same assignment independently. This is the same algorithm Kafka's `RangeAssignor` uses.

**Known limitation**: when `num_members > num_partitions`, some members get no assignment. This is correct behaviour — documented in `coordinator_test.go` (`TestRangeAssignorMoreMembersThanPartitions`).

**Not implemented**: incremental cooperative rebalancing (stop-the-world rebalance is sufficient for the group sizes tested here).

---

## Replication: ISR and high-watermark

The In-Sync Replica (ISR) set is the set of replicas that are "caught up" to the leader. The high-watermark (HWM) is the minimum log-end-offset across all ISR members. Consumers only see records up to the HWM — this is the "read-committed" guarantee.

A follower is removed from the ISR if:
- Its last fetch time is older than `lagTimeMax` (default 30s), OR
- Its fetch offset is more than `lagRecordsMax` records behind the leader LEO (default 10,000).

A follower re-joins the ISR automatically when it catches up within both thresholds.

**Leader election**: this implementation uses a static assignment — partition `i` is led by broker `(i % clusterSize) + 1`. In a production system, KRaft (Kafka's built-in Raft) or ZooKeeper would handle dynamic leader election on broker failure. The static assignment is documented as a known limitation and is suitable for a portfolio cluster of known, stable brokers.

---

## Multi-broker cluster: Static vs dynamic membership

The `--peers` flag takes a static list of `nodeID:host:port` entries. On startup, the broker:

1. Loads all topics from bbolt.
2. For each topic with RF > 1, determines whether it is the leader or follower for each partition using the static `(partID % clusterSize) + 1` formula.
3. Initialises ISR trackers for leader partitions and follower fetchers for follower partitions.

**Trade-off**: static membership means adding or removing a broker requires restarting the cluster. A production system would use dynamic membership via a consensus protocol (Raft, ZAB). For a portfolio cluster of 3 known brokers, static membership is sufficient and dramatically simpler to implement and reason about.

---

## Offset persistence: Log vs bbolt

Consumer committed offsets are stored in an append-only log (`__consumer_offsets` directory) rather than bbolt. The reasons:

- **Write frequency** — offsets are committed after every batch of consumed records; a B+ tree write transaction per commit would be significant overhead.
- **Sequential writes** — the offset log is append-only, which maps perfectly to the storage layer already built.
- **Recovery** — on startup, the log is replayed forward; the latest record per `(group, topic, partition)` key wins. This is O(total commits) but happens once at startup.

The in-memory map on `Partition` is kept in sync for O(1) `FetchOffset` reads during normal operation.

---

## What I'd do differently at production scale

1. **KRaft leader election** — replace static partition assignment with a Raft-based controller that handles broker failures, leader failover, and partition reassignment without a coordinator restart.

2. **Log compaction** — for topics with keyed records, compact the log by keeping only the latest record per key (Kafka's "log compacted" topics). The segment structure already supports this — compaction is a background job that rewrites old segments.

3. **Zero-copy sends** — use `sendfile(2)` (Linux) / `sendfile(2)` (macOS) to transfer log bytes directly from disk to the network socket without copying through userspace. This is Kafka's primary throughput mechanism.

4. **Replica-aware routing** — today followers reject Produce requests with `ErrNotLeaderForPartition`. A production client would use the Metadata response to learn which broker leads each partition and route directly. The current implementation is correct but requires clients to know the topology.

5. **Incremental cooperative rebalancing** — the current stop-the-world rebalance pauses all consumers in a group during reassignment. Cooperative rebalancing (Kafka 2.4+) allows members to continue fetching their current partitions while the new assignment is computed.

6. **Segment `fsync` policy** — currently the log uses `Sync()` on segment close. A production system would tune fsync frequency (e.g. every N records or every M milliseconds) as a latency/durability trade-off, configurable per topic.

---

## Performance characteristics (single broker, local disk)

Measured with `make bench` (100K messages, 128B values, batch=100):

| Metric | Value |
|--------|-------|
| Throughput | ~80,000–120,000 msg/sec |
| Bandwidth | ~10–15 MB/sec |
| p50 latency | < 1 ms |
| p99 latency | < 5 ms |

These numbers are disk-bound — the bottleneck is `WriteAt` syscalls to the segment `.log` file. Zero-copy and `O_DIRECT` would be the next optimisation levers.
