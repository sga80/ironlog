# Commit Log Design

1) The heart of the broker is the commit log which durably stores all incoming messages.
2) The commit log must write data as fast as possible to allow blazing writes and scale massively. All writes are
   append-only — the file descriptor is opened in append mode, so there is never any seeking or random writes.
3) As an initial first pass, we will create one commit log file per channel. This means more open file descriptors but
   avoids contention — writes to different channels never block each other. A single shared commit log file descriptor
   would become a bottleneck. The tradeoff is that many channels means many open file descriptors, which is bounded by
   the OS limit (ulimit -n). Replication for HA will also be more complex with per-channel files, but that is a future
   problem.
4) Each commit log is an ordered, append-only sequence of records. Order is determined by arrival time. Consumers read
   sequentially from a given offset — they never cause writes or modify the log.

## On-Disk Record Format

Each record written to a segment file follows this structure:

```
| 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | N bytes: payload |
```

- **offset** (`u64`): broker-assigned message sequence number. Offset 0 is the first message, offset 1 the second, etc. Message offsets are used instead of byte offsets so that consumer positions remain valid across format changes.
- **timestamp** (`u64`): Unix timestamp in milliseconds when the broker wrote the record to disk (broker time, not producer time). Useful for consumers doing time-based replay.
- **payload type** (`u16`): the format of the payload data (mirrors the wire protocol payload type)
- **payload length** (`u32`): number of bytes in the payload — required on disk because the file is a flat byte stream and the reader must know where each record ends
- **payload** (`[u8; N]`): the raw message data

The channel name is encoded in the segment filename, not in each record.

## Channel Lifecycle

- **Deletion is cheap and isolated**: deleting a channel means deleting its directory and all segment files within it. No other channel is affected, no compaction or rewriting of other files is required. This is a direct benefit of the per-channel directory design.
- **Channel limit per node**: since each channel consumes file descriptors and memory for its active segment, the number of channels per node is bounded. This limit should be configured explicitly at broker startup based on the known OS file descriptor limit and available memory, rather than allowing unbounded channel creation that silently degrades performance.

## Segmentation

Each channel's commit log is a directory of ordered segment files. A segment file is named after the base offset of its first record (e.g. `00000000000000000000.log`). When the active segment reaches the size limit, it is closed (becomes read-only) and a new segment is opened.

- **Initial segment size limit**: 1KB (artificial, for development — production would use ~1GB)
- Consumers find the correct segment for a given offset by scanning segment filenames
- Old segments can be deleted independently for retention without touching active writes

## TODO: Index File

Each segment will likely need a companion index file to support fast consumer lookups by offset, avoiding a full scan of the segment. To be designed when implementing the consumer.