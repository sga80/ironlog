# Learning Approach

IronLog is a deliberate learning project at the intersection of Rust and systems engineering. This document explains how
the project is being built and why.

## Goal

The goal is not to ship a production message broker. The goal is to deeply understand systems engineering concepts —
wire protocols, commit logs, file I/O, concurrency, replication — by building one from scratch in Rust. The versioned
roadmap in `DESIGN.md` reflects this: each version introduces one layer of complexity, understood fully before the next
is added.

## Use of AI Assistance

This project uses Claude (Anthropic) and Gemini (Google) as guides throughout the build. The collaboration is deliberately structured:

- **I write all the code.** Claude does not write code unless explicitly asked for a small reference snippet to unblock
  understanding.
- **Claude guides, explains, and reviews.** When I am stuck or uncertain, I ask for an explanation of the concept. When
  I write code, Claude reviews it and points out issues — but I fix them.
- **Design decisions are mine.** Claude presents tradeoffs and options. I make the call and explain my reasoning. The
  decision logs in `versions/` reflect my thinking, not AI-generated rationale.
- **The borrow checker fights are real.** Every ownership and lifetime error in this project was hit, understood, and
  fixed by me. That is the point.
- **Claude handles the surrounding work.** Documentation, capturing learnings from our discussions into the journal,
  Makefile setup, Kubernetes and Helm configuration, benchmark write-ups — everything that is not the core Rust and
  systems engineering learning is delegated so the focus stays on what matters.
- **Gemini was used for deep architectural discussions.** Thread-per-core vs work-stealing, log broker architecture
  patterns, io_uring internals, and comparisons with Kafka and other brokers — hours of architectural thinking that
  shaped the design decisions before a line of code was written.

This approach mirrors how a senior engineer might mentor a junior one — the mentor does not write the code for you, they
help you understand why your approach is wrong and point you toward the right one.

## Why Rust

Rust's ownership model forces you to think about memory, lifetimes, and data flow in a way that other languages don't.
For a systems project like a message broker — where performance, safety, and correctness matter — Rust is the right
tool. The friction is the feature.

## Why a Message Broker

A message broker touches nearly every systems engineering concept worth understanding:

- Network programming (TCP, wire protocols)
- File I/O (append-only logs, fsync, page cache)
- Concurrency (multiple producers and consumers)
- Replication (distributed consensus, leader/follower)
- Performance (zero-copy, batching, benchmarking)

Building one end-to-end, version by version, is a complete systems engineering education.

## Benchmarking

Each major version is benchmarked against the NASA HTTP access log (July 1995) — 1.9M real HTTP request records, publicly available. Results are documented in `versions/`. Numbers drive decisions, not intuition.