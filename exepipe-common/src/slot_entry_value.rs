use byteorder::{ByteOrder, LittleEndian};

/// SlotEntryValue is a struct that represents the value of a slot entry.
///
/// +------------------------+-------------------+------------------+
/// |       Value            |    Shift Amount   |     Mantissa     |
/// |  (N-7 bytes)           |    (1 byte)       |    (6 bytes)     |
/// +------------------------+-------------------+------------------+
///                          |<------ Dust Value (7 bytes) -------->|
pub struct SlotEntryValue<'a> {
    bz: &'a [u8],
}

impl SlotEntryValue<'_> {
    // burn little gas because of loss of precision、
    // real store dust value is (mantissa << shift_amount)
    pub fn encode(value: &mut Vec<u8>, dust: u128) {
        let leading_zeros = dust.leading_zeros();
        let shift_amount = if leading_zeros >= 80 {
            0
        } else {
            (80 - leading_zeros) as u8
        };
        let mantissa = (dust >> shift_amount) as u64;
        let mantissa_bytes = mantissa.to_le_bytes();
        value.push(shift_amount);
        value.extend_from_slice(&mantissa_bytes[..6]);
    }

    pub fn from_bz(bz: &[u8]) -> SlotEntryValue {
        SlotEntryValue { bz }
    }

    pub fn get_value(&self) -> &[u8] {
        let len = self.bz.len();
        &self.bz[..len - 7]
    }

    pub fn get_dust(&self) -> u128 {
        let len = self.bz.len();
        let shift_amount = self.bz[len - 7];
        let mantissa = LittleEndian::read_u64(&self.bz[len - 8..]) >> 16;
        (mantissa as u128) << shift_amount
    }
}

#[cfg(test)]
mod tests {
    use crate::slot_entry_value::SlotEntryValue;

    #[test]
    fn test_slot_entry_value() {
        let mut value = [1; 10].to_vec();

        let dust: u128 = 0;
        SlotEntryValue::encode(&mut value, dust);
        assert_eq!(value.len(), 17);
        let entry_value = SlotEntryValue::from_bz(&value);
        assert_eq!(entry_value.get_value(), [1; 10]);
        assert_eq!(entry_value.get_dust(), 0);

        let dust: u128 = 1;
        SlotEntryValue::encode(&mut value, dust);
        let entry_value = SlotEntryValue::from_bz(&value);
        assert_eq!(entry_value.get_dust(), 1);

        let dust: u128 = (1 << 47) + 1;
        SlotEntryValue::encode(&mut value, dust);
        let entry_value = SlotEntryValue::from_bz(&value);
        assert_eq!(entry_value.get_dust(), (1 << 47) + 1);

        let dust: u128 = (1 << 48) + 1;
        SlotEntryValue::encode(&mut value, dust);
        let entry_value = SlotEntryValue::from_bz(&value);
        assert_eq!(entry_value.get_dust(), 1 << 48);

        let dust: u128 = u128::MAX;
        SlotEntryValue::encode(&mut value, dust);
        let entry_value = SlotEntryValue::from_bz(&value);
        assert_eq!(entry_value.get_dust(), (u128::MAX >> 80) << 80);
    }
}
