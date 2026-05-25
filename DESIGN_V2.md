# IronLog V2 — Design Document

V1 version of IronLog is done and tested with a sample half a million rows for inserts and fetches. The V1 version is a
synchronous application which supports only one connection at a time. This is by design.

## V1 Findings

> Unscientific user observations

1. The ingestion is fine and retrieval is also good.
2. Observed high CPU usage of at least 50% on inserts on a Mac M5 Pro Max machine.
3. Observed high memory usage when fetching the entire commit log from offset 0.

## V2 Requirements

1. Support multiple producers and multiple consumers via async.
2. Reduce CPU and memory usage.
3. Use zero-copy or something similar to be more efficient from a resource usage standpoint.

## Async Design

Rust supports async as a feature of the programming language. However, it doesn't have a default runtime or a default
runtime API that libraries can adhere to so that we can swap runtimes transparently. Currently we will be tightly
coupled to any runtime we use.

Before we get into why we are discussing the async runtime, we need to understand the problem.

### V1 Problem — High CPU and Memory Usage

We will focus on high CPU usage. The current understanding is that in V1 we call `write_all` on each offset we receive
from the producer. Whenever we do this in Linux and Mac, the CPU has to move from user space to kernel space (ring 3 to
ring 0 in Linux). For the rest of the document we will focus on Linux. This is done for security and we need to move
between rings as `write_all` writes to page cache which is in the kernel space. Every time we call `write_all`, the CPU
needs to pause the current context, save the current context and switch the context to kernel. This is done so that the
user space is not corrupted when we switch the context to kernel space. Usually this is not a big issue when it is done
sporadically, however for a streaming broker this is done a lot. If we do this 500k times in a short period of time, the
CPU is constantly switching contexts from R3 to R0 and back which causes the CPU to spike higher.

### Why Async Alone Doesn't Solve It

This problem is not solved by simply adding async — we will start supporting multiple producers who are constantly
calling `write_all` and the CPU context switch will make the problem harder.

The default runtime in Rust today is Tokio. Tokio uses a work-stealing algorithm to steal work from CPU cores and
schedules tasks, thus achieving high async functionality. It uses epoll, which is a Linux system call for scalable I/O.
When this happens, the CPU again has to do the context switch because the task assigned to one CPU core is moved to
another. So the problem is not solved by the runtime. The core issue of CPU context switching is not resolved.

### Solution — io_uring

Linux introduced a relatively new feature in 2019 called io_uring. This is an interface to support async I/O and to
address the limitations of epoll. The basic concept is that it creates a shared memory region from user space to kernel
space with 2 ring buffers: a Submission Queue and a Completion Queue. The application writes the I/O requests to the
Submission Queue and the kernel writes the completions to the Completion Queue. This is done asynchronously. Because the
ring buffers are shared across application and kernel, it provides a zero-copy abstraction. This reduces the syscall
overhead and thus reduces CPU context switching. io_uring addresses both requirements (2) and (3). So for IronLog we
decided to use any async runtime that is built on io_uring as it prevents the context switching from application space
to kernel space.

## Runtime Decision — Compio

Compio uses a thread-per-core architecture and not work stealing. For a streaming log broker, which has predictive
workloads and behavior, we would benefit from a thread-per-core architecture. Why? Because it is a share-nothing
architecture. We are either writing to an append-only file or retrieving data from a file. This prevents the context
switch from core to core because there is no work stealing.

IronLog decided to use the Compio async runtime. The decision was based on the support for io_uring and other
cross-platform support. It defaults to epoll on unsupported operating systems.

### Validation

Apache Iggy, which is a streaming platform built in Rust, is also using the same architecture and runtime. This design
was independently arrived at before discovering Iggy, which validates the approach.

## Known Limitations

### Concurrent Producers on the Same Channel

Each channel is pinned to a specific worker thread via consistent hashing (`hash(channel_name) % num_cores`). Within a
worker thread, connections are handled sequentially — the worker processes one producer's frames fully before picking up
the next connection from its queue. This means two producers writing to the same channel at the same time will be
serialised: the second producer's connection waits in the flume channel until the first producer disconnects.

Multiple producers on **different** channels that hash to different threads are fully concurrent and unaffected.

To fix this, each incoming connection needs to be spawned as a separate compio task (`compio::runtime::spawn`) so the
worker can multiplex multiple connections concurrently on the same thread. This requires the `CommitLogServiceImpl` to
be wrapped in `Rc<RefCell<>>` so tasks can borrow the commit logger without moving it. Ordering guarantees for
concurrent writes to the same channel will also need to be considered at that point.

## Next Version

### Repo Cleanup — Separate CLI Repos

`producer-cli` and `consumer-cli` are currently merged into the `ironlog` monorepo as a pragmatic fix for Docker build
context. The cleaner long-term approach is to keep them in a separate `ironlog-examples` repo and use git dependencies
in `Cargo.toml`:

```toml
ironlog_producer = { git = "https://github.com/sga80/ironlog", package = "producer" }
```

This removes the need for local path dependencies and works cleanly with Docker since cargo fetches from git during
build. No crates.io publishing required.
