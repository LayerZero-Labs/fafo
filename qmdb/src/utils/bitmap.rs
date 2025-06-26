use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Bitmap {
    chunks: Vec<AtomicUsize>,
    size: usize,
}

impl Bitmap {
    pub fn new(size: usize) -> Self {
        let bits_per_chunk = usize::BITS as usize;
        let chunk_count = size.div_ceil(bits_per_chunk);
        let mut chunks = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            chunks.push(AtomicUsize::new(0));
        }

        Self { chunks, size }
    }

    pub fn try_acquire(&self) -> Option<usize> {
        let bits_per_chunk = usize::BITS as usize;

        for (chunk_index, chunk) in self.chunks.iter().enumerate() {
            let mut current = chunk.load(Ordering::Relaxed);

            loop {
                if current == usize::MAX {
                    break;
                }

                let zero_bit = (!current).trailing_zeros() as usize;
                let mask = 1usize << zero_bit;
                let new = current | mask;

                match chunk.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => {
                        let global_index = chunk_index * bits_per_chunk + zero_bit;
                        if global_index < self.size {
                            return Some(global_index);
                        } else {
                            return None;
                        }
                    }
                    Err(actual) => {
                        current = actual;
                        std::hint::spin_loop();
                    }
                }
            }
        }

        None
    }

    pub fn release(&self, index: usize) {
        let bits_per_chunk = usize::BITS as usize;
        assert!(index < self.size);

        let chunk_index = index / bits_per_chunk;
        let bit_index = index % bits_per_chunk;
        let mask = !(1usize << bit_index);
        self.chunks[chunk_index].fetch_and(mask, Ordering::SeqCst);
    }

    pub fn is_set(&self, index: usize) -> bool {
        let bits_per_chunk = usize::BITS as usize;
        assert!(index < self.size);

        let chunk_index = index / bits_per_chunk;
        let bit_index = index % bits_per_chunk;
        let mask = 1usize << bit_index;
        self.chunks[chunk_index].load(Ordering::Relaxed) & mask != 0
    }

    pub fn capacity(&self) -> usize {
        self.size
    }
}
