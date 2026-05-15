# Rust Learning Journal

Struggles, discoveries, and moments of clarity encountered while building IronLog. Written in the order they were hit.

---

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