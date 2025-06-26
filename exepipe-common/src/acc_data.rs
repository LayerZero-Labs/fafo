use byteorder::{BigEndian, ByteOrder};
use revm::{
    bytecode::eip7702::Eip7702Bytecode,
    primitives::{Address, FixedBytes, KECCAK_EMPTY, U256},
    state::{AccountInfo, Bytecode},
};
use smallvec::SmallVec;

// CA:
// max: balance(32 + 1) | nonce(8 + 1) = 42
// most cases: balance(11 + 1) | nonce(3 + 1) = 16
// EOA:
// max: balance(32 + 1) | nonce(8 + 1) | code_hash(32) = 74
// most cases: balance(11 + 1) | nonce(3 + 1) | code_hash(32) = 48
//EOAWithDelegation(EIP7702):
// max: balance(32 + 1) + nonce(8 + 1) + address(20) = 62
// most cases: balance(11 + 1) + nonce(3 + 1) + address(20) = 36
const ACC_DATA_DEFAULT_LEN: usize = 48;

type AccDataBz = SmallVec<[u8; ACC_DATA_DEFAULT_LEN]>;

#[derive(Clone, Default)]
pub struct AccData(AccDataBz);

impl AccData {
    pub fn from(bz: &[u8]) -> Self {
        let mut vec = SmallVec::new();
        vec.extend_from_slice(bz);
        AccData(vec)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn to_account_info(&self) -> AccountInfo {
        decode_account_info_from_bz(self.as_slice())
    }
}

pub fn decode_account_info_from_bz(bz: &[u8]) -> AccountInfo {
    let (balance, len0) = get_var_u256(bz);
    let (nonce, len1) = get_var_u64(&bz[len0..]);
    let reamining_len = bz.len() - len0 - len1;
    let (code_hash, code) = match reamining_len {
        0 => (KECCAK_EMPTY, None),
        20 => {
            let address = Address::from_slice(&bz[len0 + len1..]);
            let bytecode = Bytecode::Eip7702(Eip7702Bytecode::new(address));
            (bytecode.hash_slow(), Some(bytecode))
        }
        32 => (FixedBytes::<32>::from_slice(&bz[len0 + len1..]), None),
        _ => panic!("AccData1: invalid length"),
    };

    AccountInfo {
        balance,
        nonce,
        code_hash,
        code,
    }
}

pub fn encode_account_info(account_info: &AccountInfo) -> AccData {
    let mut acc_data = AccData::default();

    put_var_u256(&mut acc_data.0, &account_info.balance);
    put_var_u64(&mut acc_data.0, account_info.nonce);

    if !account_info.is_empty_code_hash() {
        match account_info.code.as_ref() {
            Some(Bytecode::Eip7702(byte_code)) => {
                // EOAWithDelegation
                acc_data
                    .0
                    .extend_from_slice(&byte_code.delegated_address[..]);
            }
            _ => {
                // EOA
                acc_data
                    .0
                    .extend_from_slice(account_info.code_hash.as_ref());
            }
        }
    }

    acc_data
}

fn put_var_u64(buf: &mut AccDataBz, n: u64) {
    let mut tmp = [0u8; 8];
    BigEndian::write_u64(&mut tmp, n);
    let zeros = count_leading_zeros(&tmp);
    buf.push((8 - zeros) as u8);
    let bz = &tmp[zeros..];
    buf.extend_from_slice(bz);
}

fn get_var_u64(data: &[u8]) -> (u64, usize) {
    let mut buf = [0u8; 8];
    let n = data[0] as usize;
    let leading_zeros = 8 - n;
    buf[leading_zeros..].copy_from_slice(&data[1..n + 1]);
    (BigEndian::read_u64(&buf), n + 1)
}

fn put_var_u256(buf: &mut AccDataBz, n: &U256) {
    let tmp = n.to_be_bytes_vec();
    let zeros = count_leading_zeros(&tmp);
    buf.push((32 - zeros) as u8);
    let bz = &tmp[zeros..];
    buf.extend_from_slice(bz);
}

fn get_var_u256(data: &[u8]) -> (U256, usize) {
    let mut buf = [0u8; 32];
    let n = data[0] as usize;
    let leading_zeros = 32 - n;
    buf[leading_zeros..].copy_from_slice(&data[1..n + 1]);
    (U256::from_be_slice(&buf), n + 1)
}

fn count_leading_zeros(buf: &[u8]) -> usize {
    let mut n = 0;
    for &x in buf {
        if x == 0 {
            n += 1;
        } else {
            break;
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use revm::primitives::{hex, B256, U256};

    use super::*;

    fn encode_var_u64(n: u64) -> Vec<u8> {
        let mut buf = AccData::default();
        put_var_u64(&mut buf.0, n);
        buf.as_slice().to_vec()
    }

    fn decode_var_u64(data: &[u8]) -> u64 {
        let (x, _y) = get_var_u64(data);
        x
    }

    fn encode_var_u256(n: &U256) -> Vec<u8> {
        let mut buf = AccData::default();
        put_var_u256(&mut buf.0, n);
        buf.as_slice().to_vec()
    }

    fn decode_var_u256(data: &[u8]) -> U256 {
        let (x, _y) = get_var_u256(data);
        x
    }

    #[test]
    fn test_count_leading_zeros() {
        assert_eq!(0, count_leading_zeros(&[1u8, 0u8, 2u8, 0u8, 3u8]));
        assert_eq!(1, count_leading_zeros(&[0u8, 1u8, 0u8, 2u8, 3u8]));
        assert_eq!(2, count_leading_zeros(&[0u8, 0u8, 1u8, 2u8, 3u8]));
    }

    #[test]
    fn test_var_u64() {
        assert_eq!("00", hex::encode(encode_var_u64(0x0)));
        assert_eq!("0112", hex::encode(encode_var_u64(0x12)));
        assert_eq!("021234", hex::encode(encode_var_u64(0x1234)));
        assert_eq!("0412005600", hex::encode(encode_var_u64(0x12005600)));
        assert_eq!(
            "081200560078009a00",
            hex::encode(encode_var_u64(0x1200560078009a00))
        );

        assert_eq!(0x0, decode_var_u64(&hex::decode("00").unwrap()));
        assert_eq!(0x12, decode_var_u64(&hex::decode("0112").unwrap()));
        assert_eq!(0x1234, decode_var_u64(&hex::decode("021234").unwrap()));
        assert_eq!(
            0x12005600,
            decode_var_u64(&hex::decode("0412005600").unwrap())
        );
        assert_eq!(
            0x1200560078009a00,
            decode_var_u64(&hex::decode("081200560078009a00").unwrap())
        );
    }

    #[test]
    fn test_var_u256() {
        assert_eq!("00", hex::encode(encode_var_u256(&U256::from(0x0))));
        assert_eq!("0112", hex::encode(encode_var_u256(&U256::from(0x12))));
        assert_eq!("021234", hex::encode(encode_var_u256(&U256::from(0x1234))));
        assert_eq!(
            "0412005600",
            hex::encode(encode_var_u256(&U256::from(0x12005600)))
        );
        assert_eq!(
            "081200560078009a00",
            hex::encode(encode_var_u256(&U256::from(0x1200560078009a00u64)))
        );
        assert_eq!(
            "081200560078009a00",
            hex::encode(encode_var_u256(&U256::from(0x1200560078009a00u64)))
        );
        assert_eq!(
            "1b112200000000000000000000000000000000000000000000000000",
            hex::encode(encode_var_u256(
                &U256::from(0x1122).checked_shl(200).unwrap()
            ))
        );

        assert_eq!(
            U256::from(0x0),
            decode_var_u256(&hex::decode("00").unwrap())
        );
        assert_eq!(
            U256::from(0x12),
            decode_var_u256(&hex::decode("0112").unwrap())
        );
        assert_eq!(
            U256::from(0x1234),
            decode_var_u256(&hex::decode("021234").unwrap())
        );
        assert_eq!(
            U256::from(0x12005600),
            decode_var_u256(&hex::decode("0412005600").unwrap())
        );
        assert_eq!(
            U256::from(0x1122).checked_shl(200).unwrap(),
            decode_var_u256(
                &hex::decode("1b112200000000000000000000000000000000000000000000000000").unwrap()
            )
        );
    }

    #[test]
    fn test_encode_decode_account() {
        let mut acc1 = AccountInfo::default();
        acc1.balance = U256::from(0x1234);
        acc1.nonce = 0x56;
        acc1.code_hash = KECCAK_EMPTY;

        let mut acc2 = AccountInfo::default();
        acc2.balance = U256::from(0x8899).checked_shl(200).unwrap();
        acc2.nonce = 0x11223344;
        acc2.code_hash = B256::from_slice(&[0x52u8; 32]);

        let acc1_hex = "0212340156";
        let acc2_hex = "1b88990000000000000000000000000000000000000000000000000004112233445252525252525252525252525252525252525252525252525252525252525252";

        assert_eq!(acc1_hex, hex::encode(encode_account_info(&acc1).as_slice()));
        assert_eq!(acc2_hex, hex::encode(encode_account_info(&acc2).as_slice()));
        assert_eq!(
            acc1,
            decode_account_info_from_bz(&hex::decode(acc1_hex).unwrap())
        );
        assert_eq!(
            acc2,
            decode_account_info_from_bz(&hex::decode(acc2_hex).unwrap())
        );
    }
}
