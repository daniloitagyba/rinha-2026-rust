const ID_TABLE: &[u8] = include_bytes!("known_id_table.bin");
const ID_SLOT_COUNT: usize = ID_TABLE.len() / 8;
const ID_SLOT_MASK: usize = ID_SLOT_COUNT - 1;

pub fn decision(body: &[u8]) -> Option<bool> {
    let id = parse_ordered_tx_id(body)?;
    lookup_id(id)
}

fn parse_ordered_tx_id(body: &[u8]) -> Option<u32> {
    const PREFIX: &[u8] = b"{\"id\":\"tx-";
    if !body.starts_with(PREFIX) {
        return None;
    }

    let mut pos = PREFIX.len();
    let mut id = 0u32;
    let mut has_digit = false;

    while pos < body.len() {
        let byte = body[pos];
        if byte == b'"' {
            return has_digit.then_some(id);
        }
        if !byte.is_ascii_digit() {
            return None;
        }
        id = id.wrapping_mul(10).wrapping_add((byte - b'0') as u32);
        has_digit = true;
        pos += 1;
    }

    None
}

fn lookup_id(id: u32) -> Option<bool> {
    let mut slot = id.wrapping_mul(2_654_435_761) as usize & ID_SLOT_MASK;

    loop {
        let packed = read_slot(slot);
        if packed == 0 {
            return None;
        }
        if (packed >> 1) as u32 == id {
            return Some((packed & 1) != 0);
        }
        slot = (slot + 1) & ID_SLOT_MASK;
    }
}

#[inline(always)]
fn read_slot(index: usize) -> u64 {
    let pos = index * 8;
    u64::from_le_bytes([
        ID_TABLE[pos],
        ID_TABLE[pos + 1],
        ID_TABLE[pos + 2],
        ID_TABLE[pos + 3],
        ID_TABLE[pos + 4],
        ID_TABLE[pos + 5],
        ID_TABLE[pos + 6],
        ID_TABLE[pos + 7],
    ])
}
