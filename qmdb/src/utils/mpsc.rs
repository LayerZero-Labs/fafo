//! Multi-producer, single-consumer channel implementation for 64-bit integers.
//!
//! This module provides a lock-free MPSC channel specifically optimized for
//! transmitting 64-bit integers. It uses atomic operations and a ring buffer
//! to achieve high performance in concurrent scenarios.

use std::iter::repeat_with;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Ordering used for all atomic operations in this module
const SC: Ordering = Ordering::SeqCst;

/// Internal channel structure that manages the shared state.
///
/// This structure contains:
/// - A vector of atomic 64-bit integers for data storage
/// - Atomic indices for tracking send and receive positions
struct I64Channel {
    /// Ring buffer of atomic 64-bit integers
    data: Vec<AtomicU64>,
    /// Current send position in the ring buffer
    send_idx: AtomicUsize,
    /// Current receive position in the ring buffer
    recv_idx: AtomicUsize,
}

/// Sender handle for the MPSC channel.
///
/// Multiple senders can safely send values through the channel
/// concurrently. The sender is cloneable to allow for multiple
/// producer threads.
#[derive(Clone)]
pub struct I64Sender {
    /// Reference to the shared channel state
    chan: Arc<I64Channel>,
}

/// Receiver handle for the MPSC channel.
///
/// Only one receiver can exist for a channel, and it's responsible
/// for consuming values in order. The receiver maintains its own
/// index to track its position in the ring buffer.
pub struct I64Receiver {
    /// Reference to the shared channel state
    chan: Arc<I64Channel>,
    /// Current receive index for this receiver
    idx: usize,
}

/// Creates a new MPSC channel for transmitting 64-bit integers.
///
/// # Arguments
/// * `size` - Size of the ring buffer (maximum number of pending messages)
///
/// # Returns
/// A tuple containing the sender and receiver handles
pub fn i64_sync_channel(size: usize) -> (I64Sender, I64Receiver) {
    let chan = Arc::new(I64Channel {
        data: repeat_with(|| AtomicU64::new(0)).take(size).collect(),
        send_idx: AtomicUsize::new(0),
        recv_idx: AtomicUsize::new(0),
    });
    let receiver = I64Receiver {
        chan: chan.clone(),
        idx: 0,
    };
    let sender = I64Sender { chan };
    (sender, receiver)
}

impl I64Receiver {
    /// Receives a value from the channel.
    ///
    /// This method will block until a value is available.
    ///
    /// # Returns
    /// The received 64-bit integer value
    ///
    /// # Implementation Details
    /// Uses the high bit of each value to track whether it's ready
    /// for consumption, ensuring proper synchronization between
    /// senders and receiver.
    pub fn recv(&mut self) -> i64 {
        let chan = &*self.chan;
        let j = self.idx / chan.data.len();
        let i = self.idx % chan.data.len();
        let mut bits = chan.data[i].load(SC);
        while bits >> 63 == j as u64 % 2 {
            // loop to wait for the new data
            bits = chan.data[i].load(SC);
        }
        self.idx += 1;
        chan.recv_idx.fetch_add(1, SC);
        ((bits << 1) >> 1) as i64 //clear the highest bits
    }
}

impl I64Sender {
    /// Sends a value through the channel.
    ///
    /// This method will block if the channel is full until space
    /// becomes available.
    ///
    /// # Arguments
    /// * `bits` - The 64-bit integer value to send
    ///
    /// # Panics
    /// If the input value is negative
    ///
    /// # Implementation Details
    /// Uses the high bit of each value for synchronization, ensuring
    /// proper ordering and preventing data races between multiple
    /// senders and the receiver.
    pub fn send(&self, bits: i64) {
        assert!(bits >= 0);
        let chan = &*self.chan;
        let idx = chan.send_idx.fetch_add(1, SC);
        let mut last_recv = chan.recv_idx.load(SC);
        while last_recv + chan.data.len() <= idx {
            // we do not want to overwrite non-received data, so we
            // loop to wait for the old data getting received
            last_recv = chan.recv_idx.load(SC);
        }
        let j = idx / chan.data.len();
        let i = idx % chan.data.len();
        let highest_bit = if j % 2 == 0 { 1u64 << 63 } else { 0 };
        let bits = (bits as u64) | highest_bit;
        chan.data[i].store(bits, SC);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests basic functionality of the MPSC channel.
    ///
    /// Verifies that:
    /// - Values can be sent and received
    /// - Values are received in order
    /// - Multiple sends work correctly
    #[test]
    fn test_i64_sync_channel() {
        let (sender, mut receiver) = i64_sync_channel(10);
        sender.send(1);
        sender.send(2);
        sender.send(3);
        assert_eq!(receiver.recv(), 1);
        assert_eq!(receiver.recv(), 2);
        assert_eq!(receiver.recv(), 3);
    }
}
