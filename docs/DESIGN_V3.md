# IronLog V3 — Design Document

V2 of IronLog is an async, io_uring-based broker with thread-per-core architecture and a share-nothing design. It is
benchmarked and correct. V3 targets write and read path efficiency.

## V2 Findings

1. **Write path copies payload through user space.** The broker reads the full producer frame from the TCP socket into
   a `Vec<u8>`, constructs a new commit log record in user space, and writes it to disk.

2. **Read path is O(n) — full file scan on every fetch.** `read_from_commit_log` scans the commit log from byte 0 on
   every consumer fetch, reading every record until it reaches the requested offset.

3. **Consumer OOM.** The consumer accumulates all results in a `Vec<ConsumerResult>` before sending. At scale the
   consumer pod OOMKilled.

## V3 Requirements

1. Zero-copy write path.
2. Zero-copy read path.
3. O(1) offset lookup.

## Design Decision

V3 splits the single commit log file into two files per channel — a metadata file and a payload file. Details will
be documented as the implementation progresses.

## File Formats

### Producer Wire Protocol (unchanged from V2)

```
| 4 bytes: payload_length | 2 bytes: payload_type | N bytes: payload |
```

### Metadata File (`1.meta`)

Fixed-width — **30 bytes** per record:

```
| 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload_type | 4 bytes: payload_length | 8 bytes: byte_offset_in_payload_file |
```

The fixed width enables O(1) lookup: to locate message at offset N, seek to `N × 30` bytes in the metadata file.

### Payload File (`1.payload`)

Raw payload bytes only — no framing, no headers:

```
| N bytes: payload |
```

The metadata file provides everything needed to locate and length-delimit each payload in this file.

## Write Path Design

`splice()` via a kernel pipe pair — socket → pipe → payload file. The metadata record is written after the payload write succeeds, making the metadata write the atomic commit. The pipe pair is created once per channel and reused across all writes.

Write order:
1. `splice` payload from TCP socket into pipe, then from pipe into payload file (zero user-space copy)
2. Write 30-byte metadata record (offset, timestamp, payload_type, payload_length, payload_byte_offset)
3. Advance all offsets

If the payload write fails, no metadata record is written — the message never existed from a reader's perspective. If the metadata write fails after a successful payload write, the payload bytes are orphaned but unreachable.

## Read Path Design

One record per `ConsumerFrame` request (batch size = 1). The server reads the metadata record at `offset × 30`, sends the 23-byte metadata wire frame, then splices the payload directly from the payload file to the TCP socket. No accumulation — each record is streamed and discarded.

### Nagle + Delayed ACK

The server sends metadata (23 bytes) and payload (~N bytes) in two separate writes. This triggers the classic Nagle + delayed ACK deadlock: the consumer delays its ACK for the first segment; Nagle holds the second write until that ACK arrives. Fix: `set_nodelay(true)` on the server's accepted `TcpStream`.

V2 was not affected — it sent all records in one `write_all`, avoiding the two-write split. The cost was OOM at scale.

See [docs/versions/v3.md](versions/v3.md) for full benchmark results.

## What V3 Deliberately Does Not Solve

- Per-connection compio task spawning (concurrent producers on same channel)
- Producer batching — see DESIGN_V4.md
- Replication and HA
- Retention policy
- Consumer group offset tracking