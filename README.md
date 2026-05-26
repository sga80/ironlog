# IronLog

A Kafka-inspired log broker built in Rust — written as a learning project at the intersection of systems engineering and memory-safe programming.

> **This is strictly a learning project.** It is not production-ready and is not intended to be. If you are looking for a production-grade Rust-based streaming broker, check out [Apache Iggy](https://github.com/iggy-rs/iggy).

---

## Why I Built This

My background is in software engineering. Over my career I have used high-impact distributed systems — Kafka, Redis, DynamoDB — systems that move enormous amounts of data reliably at scale. They always intrigued me. I read the original Dynamo design paper, dug into how Redis clusters shard data, and spent time understanding how these systems scale as complexity compounds with each new feature: replication, durability, sharding, consensus.

On the side, I kept reading about Rust. The idea that a compiler could enforce memory safety at compile time — no garbage collector, no runtime overhead, predictable p99 tail latency — was genuinely exciting. Having spent my entire career in GC-based languages, I tuned more JVMs than I can count — heap sizing, native memory, GC pause tuning — and came to deeply understand the tradeoffs that come with that model. I wanted to understand what it actually means to write memory-correct code by construction, without leaning on a runtime to clean up after you.

I started the Rust book. I went through it. But I wasn't making real progress — reading about ownership is not the same as fighting the borrow checker on something you care about.

So I decided to combine both things: learn Rust by building the kind of system I had always wanted to understand from the inside. A log broker. Something that touches TCP, file I/O, concurrency, wire protocols, and eventually replication — the full stack of systems engineering concepts, one version at a time.

When I started v1, I hit everything everyone talks about with Rust. The borrow checker. Lifetimes. The compiler rejecting code that looked obviously correct. It takes time. But once you understand *why* the compiler fights you, you see the payoff at runtime — and that payoff is the point. The effort you invest writing Rust and respecting its rules is effort you don't spend debugging memory corruption, data races, or undefined behaviour in production. For a systems project like a log broker, that trade is worth everything.

I document all my learnings as I go, while doing this alongside my day job.

---

## Where It Is Now

Two versions exist:

- **v1 — Synchronous, single-connection broker.** One producer, one consumer, blocking I/O. The foundation.
- **v2 — Async, io_uring, thread-per-core.** Compio runtime, share-nothing architecture, multiple concurrent producers and consumers across channels. Benchmarked on EKS against 1.9M real NASA HTTP log records.

---

## Read More

- [Approach and philosophy](docs/APPROACH.md) — how the project is structured and the role of AI assistance
- [Design v1](docs/DESIGN.md) — synchronous broker design
- [Design v2](docs/DESIGN_V2.md) — async architecture, io_uring, known limitations, and what comes next
- [Rust Learning Journal](docs/RUST_JOURNAL.md) — every borrow checker fight, every discovery, in the order they were hit
- [v1 Benchmark Results](docs/versions/v1.md)
- [v2 Benchmark Results](docs/versions/v2.md)