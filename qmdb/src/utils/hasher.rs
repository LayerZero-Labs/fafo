//! Hashing utilities for the QMDB database.
//!
//! This module provides SHA-256 based hashing functions for:
//! - Single values
//! - Values with level information
//! - Pairs of values (for Merkle tree nodes)
//! - In-place node hashing

use sha2::{Digest, Sha256};

/// Type alias for a 32-byte hash value.
/// Used throughout the database for cryptographic hashes.
pub type Hash32 = [u8; 32];

/// A constant representing a hash of all zeros.
/// Used as a placeholder or default hash value.
pub const ZERO_HASH32: Hash32 = [0u8; 32];

/// Computes the SHA-256 hash of a single value.
///
/// # Arguments
/// * `a` - Value to hash
///
/// # Returns
/// The 32-byte hash of the input
pub fn hash<T: AsRef<[u8]>>(a: T) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(a);
    hasher.finalize().into()
}

/// Computes the SHA-256 hash of a value with a level prefix.
///
/// This is used in Merkle trees where the level information needs
/// to be incorporated into the hash.
///
/// # Arguments
/// * `level` - Level in the Merkle tree
/// * `a` - Value to hash
///
/// # Returns
/// The 32-byte hash of the level and value
pub fn hash1<T: AsRef<[u8]>>(level: u8, a: T) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update([level]);
    hasher.update(a);
    hasher.finalize().into()
}

/// Computes the SHA-256 hash of two values with a level prefix.
///
/// This is used for Merkle tree nodes where two child hashes are
/// combined to form a parent hash.
///
/// # Arguments
/// * `children_level` - Level of the child nodes
/// * `a` - First value to hash
/// * `b` - Second value to hash
///
/// # Returns
/// The 32-byte hash of the level and both values
pub fn hash2<T: AsRef<[u8]>>(children_level: u8, a: T, b: T) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update([children_level]);
    hasher.update(a);
    hasher.update(b);
    hasher.finalize().into()
}

/// Computes the SHA-256 hash of two values with optional order swapping.
///
/// Similar to hash2, but allows the order of the inputs to be swapped
/// based on the exchange_ab parameter.
///
/// # Arguments
/// * `children_level` - Level of the child nodes
/// * `a` - First value to hash
/// * `b` - Second value to hash
/// * `exchange_ab` - Whether to swap the order of a and b
///
/// # Returns
/// The 32-byte hash of the level and both values
pub fn hash2x<T: AsRef<[u8]>>(children_level: u8, a: T, b: T, exchange_ab: bool) -> Hash32 {
    if exchange_ab {
        hash2(children_level, b, a)
    } else {
        hash2(children_level, a, b)
    }
}

/// Computes the SHA-256 hash of two values and stores it in-place.
///
/// This is an optimization for Merkle tree operations where allocating
/// a new hash array for each node would be inefficient.
///
/// # Arguments
/// * `children_level` - Level of the child nodes
/// * `target` - Buffer to store the resulting hash
/// * `src_a` - First value to hash
/// * `src_b` - Second value to hash
pub fn node_hash_inplace<T: AsRef<[u8]>>(
    children_level: u8,
    target: &mut [u8],
    src_a: T,
    src_b: T,
) {
    let mut hasher = Sha256::new();
    hasher.update([children_level]);
    hasher.update(src_a);
    hasher.update(src_b);
    target.copy_from_slice(&hasher.finalize());
}
