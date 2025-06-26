use std::ops::Range;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SliceError {
    #[error("Index out of bounds: range {range:?} is invalid for slice of length {len}")]
    OutOfBounds { range: Range<usize>, len: usize },
    #[error("Invalid range: start ({0}) is greater than end ({1})")]
    InvalidRange(usize, usize),
}

impl From<SliceError> for String {
    fn from(error: SliceError) -> Self {
        error.to_string()
    }
}

/// Safely extracts a slice from a byte array within the specified range.
///
/// # Arguments
/// * `bz` - The source byte slice
/// * `range` - Range of indices to extract (start is inclusive, end is exclusive)
///
/// # Returns
/// * `Ok(&[u8])` - The extracted slice if the range is valid
/// * `Err(SliceError)` - If the range is invalid or out of bounds
///
/// # Examples
/// ```
/// use qmdb::utils::slice::try_get_slice;
///
/// let data = &[1, 2, 3, 4, 5];
/// assert!(try_get_slice(data, 1..3).is_ok());
/// assert!(try_get_slice(data, 3..1).is_err());
/// assert!(try_get_slice(data, 0..6).is_err());
/// ```
pub fn try_get_slice(bz: &[u8], range: Range<usize>) -> Result<&[u8], SliceError> {
    // Check for invalid range first
    if range.start > range.end {
        return Err(SliceError::InvalidRange(range.start, range.end));
    }

    // Use get for safe slice access
    bz.get(range.clone()).ok_or(SliceError::OutOfBounds {
        range,
        len: bz.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_slice() {
        let data = &[1, 2, 3, 4, 5];
        assert_eq!(try_get_slice(data, 1..3).unwrap(), &[2, 3]);
    }

    #[test]
    fn test_invalid_range() {
        let data = &[1, 2, 3];
        assert!(matches!(
            try_get_slice(data, 2..1),
            Err(SliceError::InvalidRange(2, 1))
        ));
    }

    #[test]
    fn test_out_of_bounds() {
        let data = &[1, 2, 3];
        assert!(matches!(
            try_get_slice(data, 0..4),
            Err(SliceError::OutOfBounds { range, len }) if range == (0..4) && len == 3
        ));
    }

    #[test]
    fn test_empty_slice() {
        let data = &[1, 2, 3];
        assert_eq!(try_get_slice(data, 1..1).unwrap(), &[]);
    }

    #[test]
    fn test_start_equals_end() {
        let data = &[1, 2, 3];
        assert_eq!(try_get_slice(data, 2..2).unwrap(), &[]);
    }

    #[test]
    fn test_full_range() {
        let data = &[1, 2, 3];
        assert_eq!(try_get_slice(data, 0..3).unwrap(), data);
    }

    #[test]
    fn test_error_conversion() {
        let err = try_get_slice(&[1, 2, 3], 5..6).unwrap_err();
        let _: String = err.into(); // Should compile
    }
}
