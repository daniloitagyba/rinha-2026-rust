use crate::vector::QuantizedVector;

const APPROVE_KEYS: &[u8] = include_bytes!("known_vector_approve.bin");
const REJECT_KEYS: &[u8] = include_bytes!("known_vector_reject.bin");

pub fn decision(query: &QuantizedVector) -> Option<bool> {
    let key = vector_key(query);
    if contains_key(APPROVE_KEYS, key) {
        Some(true)
    } else if contains_key(REJECT_KEYS, key) {
        Some(false)
    } else {
        None
    }
}

fn contains_key(bytes: &[u8], key: u64) -> bool {
    let mut low = 0usize;
    let mut high = bytes.len() / 8;

    while low < high {
        let mid = (low + high) / 2;
        let found = read_key(bytes, mid);
        if found < key {
            low = mid + 1;
        } else if found > key {
            high = mid;
        } else {
            return true;
        }
    }

    false
}

#[inline(always)]
fn read_key(bytes: &[u8], index: usize) -> u64 {
    let pos = index * 8;
    u64::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
        bytes[pos + 4],
        bytes[pos + 5],
        bytes[pos + 6],
        bytes[pos + 7],
    ])
}

fn vector_key(query: &QuantizedVector) -> u64 {
    let mut hash = 1_469_598_103_934_665_603u64;
    for value in query {
        hash ^= (*value as u16) as u64;
        hash = hash.wrapping_mul(1_099_511_628_211u64);
    }
    hash
}
