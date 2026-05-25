# Rust Learning Journal

Struggles, discoveries, and moments of clarity encountered while building IronLog. Written in the order they were hit.

---

# v1 — Synchronous Single-Connection Broker

## Ownership and the Borrow Checker

The first real fight. Coming from other languages, the instinct is to pass references everywhere and let the runtime figure out lifetimes. Rust doesn't allow that.

**The lesson:** structs own their data. If a struct needs a `String`, it holds a `String` — not a `&str`. If a struct needs a `File`, it owns the `File`. References in structs require lifetime annotations, which adds complexity for no benefit when ownership is the right model.

**Where I hit it:** Trying to store `&File` in `CommitLogger` instead of `File`. Rust rejected it immediately — what would own the `File` if `CommitLogger` only borrows it?

---

## `String` vs `&str`

Took a while to understand when to use each.

**The rule:** structs own data (`String`), functions that just read use references (`&str`), getters expose owned data as references (`&str`). `&str` is more general than `&String` — prefer it for function parameters and return types.

**Where I hit it:** `RequestFrame` stores `channel_name: String` because it owns the data parsed from the wire. But the getter returns `&str` because callers just need to read it, not own it.

---

## The Borrow Checker and HashMap

The classic double-borrow problem. Calling `hashmap.get()` holds an immutable borrow. Calling `hashmap.insert()` needs a mutable borrow. Rust rejects both at the same time.

**The solution:** `HashMap::entry()` API. It handles "insert if missing, then get a mutable reference" in a single operation without triggering a double borrow. Idiomatic Rust for this exact pattern.

**Where I hit it:** `CommitLogger::write_to_commit_log` — trying to check if a channel existed, create the file if not, then write to it. The naive approach (`get` then `insert`) failed. `entry().or_insert_with()` solved it cleanly.

---

## `?` Inside Closures

`?` propagates errors out of the current function. But a closure is not the enclosing function — it has its own return type. Using `?` inside a closure that must return `T` (not `Result<T>`) fails to compile.

**The workaround:** `expect()` inside closures where error handling is unavoidable. The proper solution (`or_try_insert_with`) is not yet stable in Rust. This is a known limitation.

**Where I hit it:** Creating the file descriptor inside `entry().or_insert_with(|| { ... })`. `fs::create_dir_all(...)?` inside the closure was rejected because the closure must return `ChannelState`, not `Result<ChannelState>`.

---

## `mut` Is Explicit and Precise

In other languages, variables are mutable by default. In Rust, everything is immutable unless you say `mut`. This applies to variables, references, and function parameters.

**The lesson:** `&mut self` means the method can mutate the struct. `&self` means it cannot. Rust enforces this at compile time — calling a mutating method on an immutable binding fails.

**Where I hit it:** `CommitLogger::write_to_commit_log` — initially took `&self` but called `self.segments.insert()`. Rust rejected it. Changed to `&mut self`.

---

## Traits Must Be In Scope to Use Their Methods

Implementing a trait and calling its methods are two different things. Even if `TcpStream` implements `Read`, you cannot call `read_exact` without `use std::io::Read` in scope.

**The lesson:** Rust requires explicit trait imports to resolve method calls. This prevents naming conflicts when multiple traits define methods with the same name.

**Where I hit it:** `read_exact` on `TcpStream` failing with "method not found" until `use std::io::Read` was added.

---

## `Vec<u8>` Is The Universal Byte Buffer

TCP streams deliver raw bytes. Disk files store raw bytes. The universal Rust type for "a sequence of bytes" is `Vec<u8>` (owned) or `&[u8]` (borrowed). Everything on the wire and on disk flows through these types.

**The lesson:** `u8` is exactly one byte (0–255). Arrays and vecs of `u8` are byte buffers. `from_be_bytes()` and `to_be_bytes()` convert between integer types and their byte representations.

---

## Stack vs Heap for Buffers

Fixed-size buffers go on the stack (`[0u8; 4]`). Variable-size buffers go on the heap (`vec![0u8; n]`). The distinction matters because stack sizes must be known at compile time.

**Where I hit it:** Reading the fixed header fields (4 bytes for length, 2 bytes for request type) uses stack arrays. Reading the variable-length channel name and payload uses `vec!` because the size is only known at runtime.

---

## `TryFrom` for Enum Conversion

Rust doesn't automatically convert integers to enums. A `u16` value of `1` is not automatically `RequestType::Produce`. You must implement `TryFrom<u16>` for the enum, with a `match` that maps known values to variants and returns an error for unknown ones.

**The lesson:** explicit conversions make invalid states impossible to represent silently. If a client sends an unknown request type, the broker catches it at the boundary rather than propagating garbage.

---

## Reconnect Logic and Ownership

Storing a `TcpStream` in a struct and replacing it on reconnect is natural in Rust — the struct owns the stream, and assigning a new one drops the old one automatically. No manual cleanup needed.

**Where I hit it:** `Producer::connect()` — assigning `self.tcp_stream = TcpStream::connect(...)?` drops the broken stream and replaces it. The borrow checker ensures no dangling references to the old stream exist.

---

# v2 — Async, io_uring, Thread-per-Core

## io_uring — Why Async Alone Doesn't Fix CPU Context Switching

Adding async (Tokio, epoll) doesn't eliminate kernel/user context switches — it just schedules more of them. Every `write_all` call crosses from user space (ring 3) to kernel space (ring 0) regardless of whether it's async. With 1.9M records, that's 1.9M crossings per producer.

**The lesson:** io_uring solves this at a different level. It creates a shared ring buffer between user space and kernel space. The application writes I/O requests to the Submission Queue; the kernel writes completions to the Completion Queue. Because the buffers are shared memory, there is no copy and far fewer syscalls — multiple operations can be batched into a single kernel entry. The kernel:user ratio (`system_usec / user_usec` from cgroup `cpu.stat`) is the direct measurement of this: v1 never measured it, v2 confirmed 4.41:1 with io_uring, and batching (v4) targets below 1:1.

**Where I hit it:** Choosing between Tokio (epoll, work-stealing) and Compio (io_uring, thread-per-core). The key insight was that work-stealing itself introduces core-to-core context switches — the task assigned to Core 0 can be stolen by Core 1. For a streaming broker with predictable workloads, share-nothing thread-per-core avoids this entirely. See [DESIGN_V2.md](DESIGN_V2.md) for the full runtime decision and validation.

---

## Dropping a JoinHandle in Compio Cancels the Task

In Tokio, dropping a `JoinHandle` detaches the task — it keeps running in the background. In Compio, dropping a `JoinHandle` cancels the task immediately. This is a silent correctness bug: the task appears to start, but is immediately cancelled when the handle goes out of scope.

**The lesson:** In Compio, fire-and-forget tasks must call `.detach()` on the `JoinHandle`. Without it, `let _ = spawn(task)` drops the handle instantly and the task never runs.

**Where I hit it:** The TaskRunner spawn in `worker.rs` — `let _ = spawn(task_runner.run())` looked correct but the TaskRunner was being cancelled before processing a single connection. The symptom was a `SendError` on the flume channel: the receiver had been dropped because the task no longer existed. Changing to `spawn(task_runner.run()).detach()` fixed it.

---

## `BufResult` — Discarding the Buffer Discards the Data

Compio's async I/O operations return `BufResult(result, buffer)` instead of just `Result`. This is because Compio takes ownership of the buffer for the duration of the I/O operation and returns it back on completion. The filled data lives in the returned buffer, not the original binding.

**The lesson:** `BufResult(res, _)` discards the returned buffer. For fixed-size arrays (`[0u8; N]`), the original binding is `Copy` — it was cloned when passed to the I/O call and the original remains zero-initialized. Reading from the original after an `BufResult(res, _)` reads zeros, not the data that was written by the kernel.

**Where I hit it:** Reading `payload_type` in `ConsumerResult::from_file` — `BufResult(res, _)` left the `[0u8; 2]` array untouched. `u16::from_be_bytes([0, 0])` = 0, which was not a valid `PayloadType`, producing an "invalid payload type" error on every read. Changing `_` to `buf` and reading from `buf` fixed it.

---

## The Actor Model Without `Rc<RefCell<>>`

The natural instinct for shared mutable state in async Rust is `Rc<RefCell<T>>` (single-threaded) or `Arc<Mutex<T>>` (multi-threaded). But shared state is only needed if multiple tasks need concurrent access. If you can route all access through a single owner, you eliminate the need for any wrapper.

**The lesson:** A long-lived spawned task that owns its data exclusively and receives work via a channel is an actor. The `CommitLoggerImpl` in each TaskRunner is owned entirely by that task — no other task ever touches it. The flume channel is the only interface. No `Rc`, no `RefCell`, no `Mutex` needed.

**Where I hit it:** Designing the TaskRunner architecture. The first instinct was to wrap `CommitLoggerImpl` in `Rc<RefCell<>>` so multiple async handlers could borrow it. Then the realisation: only one task ever needs it at a time. The channel serialises access naturally, and the data is owned, not shared.

---

## Positional Writes — You Own the Cursor

`File::write_all` in standard Rust maintains an internal file position. `write_at(bytes, offset)` in Compio (and `pwrite` on Linux) does not — it writes at a specified byte offset and the file cursor does not advance. The next call at the same offset overwrites the previous write.

**The lesson:** With positional writes, you are responsible for tracking the byte offset. After a successful `write_at`, increment your tracked offset by `bytes.len()`. On restart, scan the file from offset 0, accumulate byte positions as you read each record, and resume from the end. Assign the returned position (`byte_offset = cr.1`), do not add it (`byte_offset += cr.1` double-counts).

**Where I hit it:** Two separate bugs. First: after a server restart, new records were overwriting the end of the file because `byte_offset` was not restored correctly on startup. Second: the consumer was skipping records because `byte_offset += cr.1` was adding a cumulative position to itself — after three records the offset jumped past the fourth record entirely.