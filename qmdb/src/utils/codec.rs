//! Encoding and decoding utilities for binary data.
//!
//! This module provides functions for encoding and decoding 64-bit integers
//! in little-endian byte order. These functions are used throughout the database
//! for serializing and deserializing numeric values.

use byteorder::{ByteOrder, LittleEndian};

/// Decodes a signed 64-bit integer from a little-endian byte slice.
///
/// # Arguments
/// * `v` - Byte slice containing at least 8 bytes
///
/// # Returns
/// The decoded i64 value
///
/// # Panics
/// If the input slice is less than 8 bytes long
pub fn decode_le_i64(v: &[u8]) -> i64 {
    LittleEndian::read_i64(&v[0..8])
}

/// Decodes an unsigned 64-bit integer from a little-endian byte slice.
///
/// # Arguments
/// * `v` - Byte slice containing at least 8 bytes
///
/// # Returns
/// The decoded u64 value
///
/// # Panics
/// If the input slice is less than 8 bytes long
pub fn decode_le_u64(v: &[u8]) -> u64 {
    LittleEndian::read_u64(&v[0..8])
}

/// Encodes an unsigned 64-bit integer as a little-endian byte vector.
///
/// # Arguments
/// * `n` - Value to encode
///
/// # Returns
/// A vector containing the 8-byte little-endian representation
pub fn encode_le_u64(n: u64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

/// Encodes a signed 64-bit integer as a little-endian byte vector.
///
/// # Arguments
/// * `n` - Value to encode
///
/// # Returns
/// A vector containing the 8-byte little-endian representation
pub fn encode_le_i64(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}
