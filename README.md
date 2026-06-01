# plato-ring

> Lock-free ring buffer for high-frequency PLATO sensor data

## What This Does

plato-ring provides a fixed-capacity circular buffer optimized for high-frequency sensor data. Pre-allocated at construction — no heap allocations during operation. Supports both overwrite (evict oldest) and reject (return error when full) modes. Tracks total reads, writes, and overwrites for observability.

## The Key Idea

Sensors produce data faster than you can process it. A ring buffer absorbs the burst: new readings overwrite the oldest when full. It's a fixed-size sliding window of the most recent N values, pre-allocated once, with O(1) push and pop. The ring buffer is the backbone of PLATO's real-time data flow.

## Install

```bash
cargo add plato-ring
```

## Quick Start

```rust
use plato_ring::RingBuffer;

let mut ring: RingBuffer<f64> = RingBuffer::new(100);

// Push values (overwrite mode evicts oldest)
ring.push(22.5).unwrap();
ring.push(23.0).unwrap();

// Read oldest
let oldest = ring.pop();

// Stats
let stats = ring.stats();
println!("Written: {}, Read: {}, Overwrites: {}", 
    stats.total_written, stats.total_read, stats.overwrites);
```

## API Reference

| Type | Description |
|---|---|
| `RingBuffer<T>` | Fixed-capacity circular buffer. Generic over `T`. |
| `RingConfig { capacity, overwrite }` | Configuration. Default: capacity=1024, overwrite=false. |
| `RingStats { total_written, total_read, overwrites, current_len }` | Usage statistics. |
| `PushError<T>` | Error when buffer is full and overwrite is disabled. |

### Methods

```rust
RingBuffer::new(capacity);            // overwrite=false
RingBuffer::with_config(config);      // custom config
ring.push(item);                      // Result<Option<evicted>, PushError>
ring.pop();                           // Option<T>
ring.peek();                          // Option<&T>
ring.is_full() / is_empty();
ring.len() / capacity();
ring.stats();                         // RingStats
ring.iter();                          // Front to back
ring.clear();
```

## Testing

19 tests: push/pop, overwrite vs reject modes, peek, capacity tracking, stats, iteration, clear, empty/full edge cases, generics.

## License

Apache-2.0
