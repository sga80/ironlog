# IronLog — Design Document

A single-node log broker inspired by Apache Kafka, built in Rust.

## Version Roadmap

Each version is benchmarked against the Alibaba Cloud block storage traces and results are documented in `benchmarks/vX.md`. Versions are tagged in git (`v1.0`, `v2.0`, etc.) so the baseline for each phase is reproducible.

| Version | Focus | Key Concepts |
|---------|-------|--------------|
| **v1** | Single-node, synchronous | Wire protocol, commit log, producer, consumer |
| **v2** | Async | Tokio, concurrent connections, non-blocking I/O |
| **v3** | Replication | Leader/follower, HA, consistency guarantees |
| **v4** | Producer batching | Throughput optimization, batch Acks |
| **v5** | Zero-copy write path | `splice()`, kernel page cache, unsafe Rust |

Each version builds on the previous — no async until the synchronous baseline is understood, no replication until concurrency is solid. Benchmark numbers drive decisions rather than speculation.

## Actors

| Actor        | Role                                                                                             |
|--------------|--------------------------------------------------------------------------------------------------|
| **Producer** | Connects to the broker and streams data into a channel                                           |
| **Channel**  | Named medium through which data flows; producers write to it, consumers read from it             |
| **Broker**   | Accepts streaming data from producers, durably stores it on the node, and serves it to consumers |
| **Consumer** | Connects to the broker and consumes streaming data from a channel                                |

## Guarantees

- **Durability**: Data written to the broker is persisted on disk

## TODO: Zero-Copy Write Path

Currently the payload is read from the network socket into a `Vec<u8>` in user space, then written to disk — two kernel/user space transitions. 

A future optimization is to use the Linux `splice()` syscall to move the payload directly from the network socket to the disk file entirely within kernel space, bypassing user space. The wire frame header fields (length, request type, payload type, channel name) must still be read through user space to determine routing and payload size, but the payload itself can be zero-copy.

This requires unsafe Rust via the `libc` crate and is Linux-only. Implement after the full producer → broker → consumer flow is working.

**Risks and constraints when implementing:**

- **Partial writes**: `splice()` can transfer fewer bytes than requested. Must loop until all bytes are transferred — a partial write silently corrupts the commit log record.
- **Pipe buffer size**: `splice()` requires an intermediary pipe. The default pipe buffer is 64KB. Payloads exceeding this require multiple `splice()` calls, increasing the chance of partial write bugs.
- **File descriptor leaks**: errors during `splice()` must be handled carefully to ensure pipe fds are always closed. Leaked fds in a long-running broker will exhaust the OS fd limit and cause the server to stop accepting connections.
- **Kernel attack surface**: `splice()` operates directly on the page cache. IronLog must run as a non-privileged user — never as root — to limit exposure.
- **Non-portable**: Linux-only. Guard with `#[cfg(target_os = "linux")]`.

## Client Protocol

- The broker exposes a well-defined wire protocol
- Producers and consumers interact with the broker directly via this protocol
- Language-specific SDKs are optional convenience wrappers over the protocol
- Anyone can implement a client in any language by following the protocol spec

### Connection Handshake

When a client connects, it immediately sends a 2-byte request type to identify itself:

```
| 2 bytes: request type |
```

The broker reads this once per connection and routes to the appropriate handler. All subsequent frames on the connection omit the request type — it is a connection-level identifier, not a per-message field.

### Producer Frame

Sent by producers after the handshake, once per message:

```
| 4 bytes: length | 2 bytes: payload type | 1 byte: channel name length | M bytes: channel name | N bytes: payload |
```

- **length** (`u32`): number of bytes in the payload (N only, does not include header or channel name)
- **payload type** (`u16`): the format of the payload data
- **channel name length** (`u8`): number of bytes in the channel name (M)
- **channel name** (`[u8; M]`): UTF-8 encoded channel name

### Consumer Frame

Sent by consumers after the handshake to initiate a fetch:

```
| 1 byte: channel name length | M bytes: channel name | 8 bytes: offset |
```

- **channel name length** (`u8`): number of bytes in the channel name (M)
- **channel name** (`[u8; M]`): UTF-8 encoded channel name
- **offset** (`u64`): the offset to start reading from. `0` means start from the beginning — offset 0 and "start from beginning" are identical, so no separate flag is needed

### Consumer Result

Streamed by the broker to the consumer, one record at a time, until all available records are sent. The broker closes the connection when done (fetch-and-close).

```
| 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | N bytes: payload |
```

- **offset** (`u64`): the offset of this record in the commit log
- **timestamp** (`u64`): Unix timestamp in milliseconds when the broker wrote this record
- **payload type** (`u16`): the format of the payload data
- **payload length** (`u32`): number of bytes in the payload
- **payload** (`[u8; N]`): the raw message data

Note: the `ConsumerResult` wire format mirrors the on-disk record format intentionally — in a future zero-copy read path, records can be sent directly from the page cache without transformation.

### Request Types

| Code | Name | Description |
|------|------|-------------|
| `0x0001` | Produce | Client sending a message to the broker |
| `0x0002` | Fetch | Consumer requesting messages from the broker |
| `0x0003` | Ack | Broker confirming receipt of a message |

### Ack Response Frame

After a successful Produce, the broker sends back:

```
| 2 bytes: request type | 1 byte: status | 8 bytes: offset | 8 bytes: timestamp |
```

- **request type** (`u16`): `0x0003` — identifies this as an Ack
- **status** (`u8`): `0x00` success, `0x01` failure
- **offset** (`u64`): the offset assigned to the message in the commit log
- **timestamp** (`u64`): Unix timestamp in milliseconds when the broker wrote the message to disk (broker time)

### Payload Types

| Code | Name | Description |
|------|------|-------------|
| `0x0001` | Text | Plain UTF-8 text |
| `0x0002` | JSON | JSON encoded data |
| `0x0003` | Binary | Raw binary data |