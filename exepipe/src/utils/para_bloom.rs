use byteorder::{BigEndian, ByteOrder};
use exepipe_common::access_set::AccessSet;
use num_traits::Num;

const FUNC_COUNT: usize = 5; //hash function count for bloomfilter
const BLOOM_BITS_SHIFT: u64 = 11; // 11*5 = 55 < 64
pub const BLOOM_BITS: u64 = 1 << BLOOM_BITS_SHIFT; //bit count in one bloomfilter
const BLOOM_BITS_MASK: u64 = BLOOM_BITS - 1;

pub trait PBElement:
    Num
    + std::marker::Copy
    + std::ops::BitAnd<Output = Self>
    + std::ops::BitOr<Output = Self>
    + std::ops::Not<Output = Self>
    + std::ops::Shl<usize, Output = Self>
{
    const BITS: u32;
}

impl PBElement for u8 {
    const BITS: u32 = u8::BITS;
}
impl PBElement for u16 {
    const BITS: u32 = u16::BITS;
}
impl PBElement for u32 {
    const BITS: u32 = u32::BITS;
}
impl PBElement for u64 {
    const BITS: u32 = u64::BITS;
}
impl PBElement for u128 {
    const BITS: u32 = u128::BITS;
}

// N BloomFilters in parallel
pub struct ParaBloom<E: PBElement> {
    rdo_arr: [E; BLOOM_BITS as usize],
    rnw_arr: [E; BLOOM_BITS as usize],
    rdo_set_size: Vec<usize>,
    rnw_set_size: Vec<usize>,
}

impl<E: PBElement> Default for ParaBloom<E> {
    fn default() -> Self {
        ParaBloom::<E>::new()
    }
}

impl<E: PBElement> ParaBloom<E> {
    pub fn new() -> ParaBloom<E> {
        ParaBloom::<E> {
            rdo_arr: [E::zero(); BLOOM_BITS as usize],
            rnw_arr: [E::zero(); BLOOM_BITS as usize],
            rdo_set_size: vec![0; E::BITS as usize],
            rnw_set_size: vec![0; E::BITS as usize],
        }
    }

    fn get_rdo_mask(&self, mut k64: u64) -> E {
        let mut rdo_mask = !E::zero(); // all-ones
        for _ in 0..FUNC_COUNT {
            let idx = (k64 & BLOOM_BITS_MASK) as usize;
            rdo_mask = rdo_mask & self.rdo_arr[idx]; //bitwise-and
            k64 >>= BLOOM_BITS_SHIFT;
        }
        rdo_mask
    }

    fn get_rnw_mask(&self, mut k64: u64) -> E {
        let mut rnw_mask = !E::zero(); // all-ones
        for _ in 0..FUNC_COUNT {
            let idx = (k64 & BLOOM_BITS_MASK) as usize;
            rnw_mask = rnw_mask & self.rnw_arr[idx]; //bitwise-and
            k64 >>= BLOOM_BITS_SHIFT;
        }
        rnw_mask
    }

    fn add_rdo_k64(&mut self, id: usize, mut k64: u64) {
        let target_bit: E = E::one() << id;
        for _ in 0..FUNC_COUNT {
            let idx = (k64 & BLOOM_BITS_MASK) as usize;
            self.rdo_arr[idx] = self.rdo_arr[idx] | target_bit;
            k64 >>= BLOOM_BITS_SHIFT;
        }
        self.rdo_set_size[id] += 1;
    }

    fn add_rnw_k64(&mut self, id: usize, mut k64: u64) {
        let target_bit: E = E::one() << id;
        for _ in 0..FUNC_COUNT {
            let idx = (k64 & BLOOM_BITS_MASK) as usize;
            self.rnw_arr[idx] = self.rnw_arr[idx] | target_bit;
            k64 >>= BLOOM_BITS_SHIFT;
        }
        self.rnw_set_size[id] += 1;
    }

    pub fn get_rdo_set_size(&self, id: usize) -> usize {
        self.rdo_set_size[id]
    }

    pub fn get_rnw_set_size(&self, id: usize) -> usize {
        self.rnw_set_size[id]
    }

    pub fn clear(&mut self, id: usize) {
        let keep_mask = !(E::one() << id); //clear 'id', keep other bits
        for idx in 0..(BLOOM_BITS as usize) {
            self.rdo_arr[idx] = self.rdo_arr[idx] & keep_mask;
            self.rnw_arr[idx] = self.rnw_arr[idx] & keep_mask;
        }
        self.rdo_set_size[id] = 0;
        self.rnw_set_size[id] = 0;
    }

    pub fn clear_all(&mut self) {
        for idx in 0..(BLOOM_BITS as usize) {
            self.rdo_arr[idx] = E::zero();
            self.rnw_arr[idx] = E::zero();
        }
        for id in 0..(E::BITS as usize) {
            self.rdo_set_size[id] = 0;
            self.rnw_set_size[id] = 0;
        }
    }

    pub fn get_dep_mask(&self, access_set: &AccessSet) -> E {
        let mut mask = E::zero();
        // other.rdo vs self.rnw
        for &k80 in access_set.rdo_set.iter() {
            let k64 = BigEndian::read_u64(&k80[..8]);
            mask = mask | self.get_rnw_mask(k64);
        }
        // others.rnw vs self.rdo+self.rnw
        for &k80 in access_set.rnw_set.iter() {
            let k64 = BigEndian::read_u64(&k80[..8]);
            mask = mask | self.get_rdo_mask(k64);
            mask = mask | self.get_rnw_mask(k64);
        }
        mask
    }

    pub fn add(&mut self, id: usize, access_set: &AccessSet) {
        for &k80 in access_set.rdo_set.iter() {
            let k64 = BigEndian::read_u64(&k80[..8]);
            self.add_rdo_k64(id, k64);
        }
        for &k80 in access_set.rnw_set.iter() {
            let k64 = BigEndian::read_u64(&k80[..8]);
            self.add_rnw_k64(id, k64);
        }
    }

    pub fn get_mask_bits(&self) -> usize {
        E::BITS as usize
    }
}
