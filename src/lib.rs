use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Configuration for ring buffer behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingConfig {
    pub capacity: usize,
    pub overwrite: bool,
}

impl Default for RingConfig {
    fn default() -> Self {
        RingConfig {
            capacity: 1024,
            overwrite: false,
        }
    }
}

/// Statistics tracked by the ring buffer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingStats {
    pub total_written: u64,
    pub total_read: u64,
    pub overwrites: u64,
    pub current_len: usize,
}

/// A fixed-size circular (ring) buffer for high-frequency sensor data.
///
/// Generic over `T`. No heap allocation occurs after construction (the internal
/// buffer is pre-allocated to `capacity`). When `overwrite` is `false` and the
/// buffer is full, `push` returns `Err(item)`.
#[derive(Debug, Serialize, Deserialize)]
pub struct RingBuffer<T> {
    buf: VecDeque<T>,
    capacity: usize,
    overwrite: bool,
    total_written: u64,
    total_read: u64,
    overwrites: u64,
}

/// Error returned when a push fails because the buffer is full.
#[derive(Debug)]
pub struct PushError<T>(pub T);

impl<T> std::fmt::Display for PushError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ring buffer is full and overwrite is disabled")
    }
}

impl<T: std::fmt::Debug> std::error::Error for PushError<T> {}

impl<T> RingBuffer<T> {
    /// Create a new ring buffer with the given capacity and overwrite disabled.
    pub fn new(capacity: usize) -> Self {
        Self::with_config(RingConfig {
            capacity,
            overwrite: false,
        })
    }

    /// Create a ring buffer from a config.
    pub fn with_config(config: RingConfig) -> Self {
        RingBuffer {
            buf: VecDeque::with_capacity(config.capacity),
            capacity: config.capacity,
            overwrite: config.overwrite,
            total_written: 0,
            total_read: 0,
            overwrites: 0,
        }
    }

    /// Push an item. If overwrite is enabled and the buffer is full, returns
    /// `Ok(Some(evicted))`. If overwrite is disabled and the buffer is full,
    /// returns `Err(item)`. Otherwise returns `Ok(None)`.
    pub fn push(&mut self, item: T) -> Result<Option<T>, PushError<T>> {
        if self.buf.len() == self.capacity {
            if self.overwrite {
                let evicted = self.buf.pop_front();
                self.overwrites += 1;
                self.total_read += 1;
                self.buf.push_back(item);
                self.total_written += 1;
                Ok(evicted)
            } else {
                Err(PushError(item))
            }
        } else {
            self.buf.push_back(item);
            self.total_written += 1;
            Ok(None)
        }
    }

    /// Pop the oldest item.
    pub fn pop(&mut self) -> Option<T> {
        let item = self.buf.pop_front()?;
        self.total_read += 1;
        Some(item)
    }

    /// Peek at the oldest item without removing it.
    pub fn peek(&self) -> Option<&T> {
        self.buf.front()
    }

    /// Get a reference to the most recently pushed item.
    pub fn latest(&self) -> Option<&T> {
        self.buf.back()
    }

    /// Get a reference to the oldest item in the buffer.
    pub fn oldest(&self) -> Option<&T> {
        self.buf.front()
    }

    /// Current number of items in the buffer.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Whether the buffer is at capacity.
    pub fn is_full(&self) -> bool {
        self.buf.len() == self.capacity
    }

    /// Clear all items.
    pub fn clear(&mut self) {
        let removed = self.buf.len() as u64;
        self.buf.clear();
        self.total_read += removed;
    }

    /// Iterate from oldest to newest.
    pub fn iter(&self) -> RingIterator<'_, T> {
        RingIterator {
            inner: self.buf.iter(),
        }
    }

    /// Get buffer statistics.
    pub fn stats(&self) -> RingStats {
        RingStats {
            total_written: self.total_written,
            total_read: self.total_read,
            overwrites: self.overwrites,
            current_len: self.buf.len(),
        }
    }

    /// Remove and return the `n` oldest items (or fewer if the buffer has less).
    pub fn drain(&mut self, n: usize) -> Vec<T> {
        let to_drain = n.min(self.buf.len());
        let items: Vec<T> = self.buf.drain(..to_drain).collect();
        self.total_read += items.len() as u64;
        items
    }
}

/// Iterator over ring buffer contents, oldest to newest.
pub struct RingIterator<'a, T> {
    inner: std::collections::vec_deque::Iter<'a, T>,
}

impl<'a, T> Iterator for RingIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, T> ExactSizeIterator for RingIterator<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_pop_basic() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(4);
        rb.push(1).unwrap();
        rb.push(2).unwrap();
        rb.push(3).unwrap();
        assert_eq!(rb.pop(), Some(1));
        assert_eq!(rb.pop(), Some(2));
        assert_eq!(rb.pop(), Some(3));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn wrap_around_behavior() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(3);
        // Fill the buffer
        for i in 0..3 {
            rb.push(i).unwrap();
        }
        // Further pushes fail without overwrite
        for i in 3..10 {
            assert!(rb.push(i).is_err());
        }
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.pop(), Some(0));
        assert_eq!(rb.pop(), Some(1));
        assert_eq!(rb.pop(), Some(2));
    }

    #[test]
    fn wrap_around_with_overwrite() {
        let mut rb: RingBuffer<i32> = RingBuffer::with_config(RingConfig {
            capacity: 3,
            overwrite: true,
        });
        for i in 0..10 {
            rb.push(i).unwrap();
        }
        // Should only contain last 3: 7, 8, 9
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.pop(), Some(7));
        assert_eq!(rb.pop(), Some(8));
        assert_eq!(rb.pop(), Some(9));
    }

    #[test]
    fn overwrite_mode_on() {
        let mut rb: RingBuffer<i32> = RingBuffer::with_config(RingConfig {
            capacity: 3,
            overwrite: true,
        });
        for i in 0..5 {
            let evicted = rb.push(i).unwrap();
            if i >= 3 {
                assert_eq!(evicted, Some(i - 3));
            } else {
                assert!(evicted.is_none());
            }
        }
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.pop(), Some(2));
        assert_eq!(rb.pop(), Some(3));
        assert_eq!(rb.pop(), Some(4));
    }

    #[test]
    fn overwrite_mode_off_returns_err() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(2);
        rb.push(1).unwrap();
        rb.push(2).unwrap();
        let result = rb.push(3);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, 3);
    }

    #[test]
    fn iterator_correctness_oldest_to_newest() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(5);
        for i in 1..=4 {
            rb.push(i).unwrap();
        }
        let items: Vec<&i32> = rb.iter().collect();
        assert_eq!(items, vec![&1, &2, &3, &4]);
    }

    #[test]
    fn stats_tracking() {
        let mut rb: RingBuffer<i32> = RingBuffer::with_config(RingConfig {
            capacity: 2,
            overwrite: true,
        });
        rb.push(1).unwrap();
        rb.push(2).unwrap();
        rb.push(3).unwrap(); // overwrites 1
        rb.pop(); // reads 2
        let stats = rb.stats();
        assert_eq!(stats.total_written, 3);
        assert_eq!(stats.total_read, 2); // 1 overwrite-read + 1 pop
        assert_eq!(stats.overwrites, 1);
        assert_eq!(stats.current_len, 1);
    }

    #[test]
    fn drain_removes_correct_items() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(5);
        for i in 1..=5 {
            rb.push(i).unwrap();
        }
        let drained = rb.drain(3);
        assert_eq!(drained, vec![1, 2, 3]);
        assert_eq!(rb.len(), 2);
        assert_eq!(rb.peek(), Some(&4));
    }

    #[test]
    fn drain_more_than_available() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(5);
        rb.push(1).unwrap();
        rb.push(2).unwrap();
        let drained = rb.drain(10);
        assert_eq!(drained, vec![1, 2]);
        assert!(rb.is_empty());
    }

    #[test]
    fn latest_and_oldest() {
        let mut rb: RingBuffer<&str> = RingBuffer::new(3);
        assert!(rb.latest().is_none());
        assert!(rb.oldest().is_none());
        rb.push("first").unwrap();
        rb.push("second").unwrap();
        rb.push("third").unwrap();
        assert_eq!(rb.latest(), Some(&"third"));
        assert_eq!(rb.oldest(), Some(&"first"));
    }

    #[test]
    fn capacity_one() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(1);
        assert!(rb.is_empty());
        rb.push(42).unwrap();
        assert!(rb.is_full());
        assert_eq!(rb.peek(), Some(&42));
        assert_eq!(rb.latest(), Some(&42));
        let err = rb.push(99);
        assert!(err.is_err());
    }

    #[test]
    fn capacity_one_overwrite() {
        let mut rb: RingBuffer<i32> = RingBuffer::with_config(RingConfig {
            capacity: 1,
            overwrite: true,
        });
        rb.push(1).unwrap();
        let evicted = rb.push(2).unwrap();
        assert_eq!(evicted, Some(1));
        assert_eq!(rb.pop(), Some(2));
    }

    #[test]
    fn empty_buffer_operations() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(5);
        assert!(rb.is_empty());
        assert_eq!(rb.len(), 0);
        assert!(rb.pop().is_none());
        assert!(rb.peek().is_none());
        assert!(rb.latest().is_none());
        assert!(rb.oldest().is_none());
        assert!(rb.iter().next().is_none());
    }

    #[test]
    fn clear_resets_buffer() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(5);
        rb.push(1).unwrap();
        rb.push(2).unwrap();
        rb.clear();
        assert!(rb.is_empty());
        assert_eq!(rb.len(), 0);
        // stats should reflect reads
        let stats = rb.stats();
        assert_eq!(stats.total_read, 2);
        assert_eq!(stats.total_written, 2);
    }

    #[test]
    fn generic_f64() {
        let mut rb: RingBuffer<f64> = RingBuffer::new(3);
        rb.push(1.5).unwrap();
        rb.push(2.7).unwrap();
        assert_eq!(rb.pop(), Some(1.5));
        assert_eq!(rb.latest(), Some(&2.7));
    }

    #[test]
    fn generic_string() {
        let mut rb: RingBuffer<String> = RingBuffer::new(3);
        rb.push("hello".to_string()).unwrap();
        rb.push("world".to_string()).unwrap();
        assert_eq!(rb.pop(), Some("hello".to_string()));
        assert_eq!(rb.latest(), Some(&"world".to_string()));
    }

    #[test]
    fn generic_custom_struct() {
        #[derive(Debug, PartialEq)]
        struct SensorReading { value: f64, timestamp: u64 }
        let mut rb: RingBuffer<SensorReading> = RingBuffer::new(2);
        rb.push(SensorReading { value: 23.5, timestamp: 1 }).unwrap();
        rb.push(SensorReading { value: 24.0, timestamp: 2 }).unwrap();
        let reading = rb.pop().unwrap();
        assert_eq!(reading.value, 23.5);
    }

    #[test]
    fn serialize_deserialize() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(3);
        rb.push(10).unwrap();
        rb.push(20).unwrap();
        let json = serde_json::to_string(&rb).unwrap();
        let mut rb2: RingBuffer<i32> = serde_json::from_str(&json).unwrap();
        assert_eq!(rb2.pop(), Some(10));
        assert_eq!(rb2.pop(), Some(20));
    }

    #[test]
    fn stats_serialize() {
        let mut rb: RingBuffer<i32> = RingBuffer::new(3);
        rb.push(1).unwrap();
        let stats = rb.stats();
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("total_written"));
    }
}
